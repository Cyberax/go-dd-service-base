package visibility

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"net/http"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

const RequestHeaderKey = 11

type GenericTwirpServer interface {
	http.Handler
	ServiceDescriptor() ([]byte, int)
	ProtocGenTwirpVersion() string
	PathPrefix() string
}

type responseCapturer struct {
	http.ResponseWriter
	statusCode int
	bytesOut   int64
}

func NewResponseCodeCapturer(writer http.ResponseWriter) *responseCapturer {
	return &responseCapturer{ResponseWriter: writer, statusCode: http.StatusOK}
}

func (lrw *responseCapturer) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *responseCapturer) Write(data []byte) (int, error) {
	res, err := lrw.ResponseWriter.Write(data)
	if res > 0 {
		lrw.bytesOut += int64(res)
	}
	return res, err
}

type TracedGorilla struct {
	twirpServer GenericTwirpServer
	logger      *zap.Logger
	sink        statsd.ClientInterface

	sampleRate, errorSampleRate *float64
}

func NewTracedGorilla(twirpServer GenericTwirpServer, logger *zap.Logger, sink statsd.ClientInterface,
	sampleRate *float64, errorSampleRate *float64) *TracedGorilla {

	return &TracedGorilla{
		twirpServer:     twirpServer,
		logger:          logger,
		sink:            sink,
		sampleRate:      sampleRate,
		errorSampleRate: errorSampleRate}
}

func (t *TracedGorilla) AttachGorillaToMuxer(router *mux.Router) {
	router.Use(t.handleRequest)
	router.PathPrefix(t.twirpServer.PathPrefix()).Methods("POST").
		Handler(t.twirpServer)
}

func (t *TracedGorilla) handleRequest(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip non-twirp requests
		if !strings.HasPrefix(r.URL.Path, t.twirpServer.PathPrefix()) {
			next.ServeHTTP(w, r)
			return
		}

		opts := []tracer.StartSpanOption{
			tracer.SpanType(ext.SpanTypeWeb),
			tracer.Tag(ext.HTTPMethod, r.Method),
			tracer.Tag(ext.HTTPURL, r.URL.Path),
		}
		if t.sampleRate != nil {
			opts = append(opts, tracer.Tag(ext.EventSampleRate, *t.sampleRate))
		}
		if spanctx, err := tracer.Extract(tracer.HTTPHeadersCarrier(r.Header)); err == nil {
			opts = append(opts, tracer.ChildOf(spanctx))
		}

		// We start with an 'unknown' method, it will be overridden in traced_twirp.go
		// once the method name is known.
		span, ctx := tracer.StartSpanFromContext(r.Context(),
			"twirp.unknown", opts...)
		defer span.Finish()

		// Get the client type from the baggage
		clientType := ClientTypeFromSpan(span)

		// Copy the 'baggage' from other tracers
		reqId := r.Header.Get("Request-Id")
		if reqId == "" {
			reqId = r.Header.Get("X-Request-Id")
		}
		if reqId != "" {
			span.SetBaggageItem("request-id", reqId)
			span.SetTag("request-id", reqId)
		}

		// Contextualize the logger
		traceId := fmt.Sprintf("%d", span.Context().TraceID())
		spanId := fmt.Sprintf("%d", span.Context().SpanID())

		// Return the tracing headers back to the caller
		if traceId != "0" && spanId != "0" {
			w.Header().Add(tracer.DefaultTraceIDHeader, traceId)
			w.Header().Add(tracer.DefaultParentIDHeader, spanId)
		}

		ctx = ContextWithStatsd(ctx, t.sink)
		ctx = ContextWithClientType(ctx, clientType)

		// Set the pprof labels for the thread
		ctx = pprof.WithLabels(ctx,
			pprof.Labels("url", r.URL.String(), "dd", traceId))
		pprof.SetGoroutineLabels(ctx)
		defer pprof.SetGoroutineLabels(context.Background())

		fields := []zap.Field{
			zap.String("dd.trace_id", traceId),
			zap.String("dd.span_id", spanId),
			zap.String("log.trace_id", traceId),
			zap.String("log.span_id", spanId),
		}
		if reqId != "" {
			fields = append(fields, zap.String("request_id", reqId))
		}
		logger := t.logger.Named("HTTP").With(fields...)
		ctx = ImbueContext(ctx, logger) // Add the logger
		// Also set up the headers
		ctx = context.WithValue(ctx, RequestHeaderKey, r.Header)
		r = r.WithContext(ctx)
		capt := NewResponseCodeCapturer(w)

		logger.Info("Starting request")
		start := time.Now()

		defer func() {
			p := recover()
			if p == nil {
				return
			}

			// Sample errors at a higher rate
			if t.errorSampleRate != nil && capt.statusCode >= http.StatusBadRequest {
				span.SetTag(ext.EventSampleRate, *t.errorSampleRate)
			}

			// We can't do much with the panic at this point, just make
			// sure panic is logged and we've returned the 500 error.
			stack := NewShortenedStackTrace(3, true,
				fmt.Sprintf("%v", p))
			var fields []zap.Field

			// Log the stack trace
			fields = append(fields, zap.String("stacktrace", stack.StringStack()))
			fields = append(fields, zap.String("panic", fmt.Sprintf("%v", p)))
			fields = append(fields, t.prepareCommonLogFields(capt, r, time.Now().Sub(start))...)
			logger.Info("Request failed", fields...)

			// Re-panic if the error has not been committed
			if capt.statusCode < 400 {
				panic(p)
			}
			span.SetTag(ext.HTTPCode, capt.statusCode)
		}()

		// Run the next handler
		next.ServeHTTP(capt, r)

		logger.Info("Request finished",
			t.prepareCommonLogFields(capt, r, time.Now().Sub(start))...)

		span.SetTag(ext.HTTPCode, capt.statusCode)

		// Sample errors at a higher rate
		if t.errorSampleRate != nil && capt.statusCode >= http.StatusBadRequest {
			span.SetTag(ext.EventSampleRate, *t.errorSampleRate)
		}
	})
}

func GetHttpRequestHeader(ctx context.Context) (http.Header, bool) {
	val, ok := ctx.Value(RequestHeaderKey).(http.Header)
	return val, ok
}

func (t *TracedGorilla) prepareCommonLogFields(res *responseCapturer, req *http.Request,
	reqDuration time.Duration) []zap.Field {

	// Now log whatever happened
	bytesIn, err := strconv.ParseInt(req.Header.Get("Content-Length"),
		10, 64)
	if err != nil {
		bytesIn = 0
	}
	p := req.URL.Path
	if p == "" {
		p = "/"
	}

	host := req.Host
	return []zap.Field{
		zap.String("path", p),
		//zap.String("remote_ip", req.RealIP()), //TODO
		zap.String("host", host),
		zap.String("method", req.Method),
		zap.String("uri", req.RequestURI),
		zap.String("referer", req.Referer()),
		zap.String("user_agent", req.UserAgent()),
		zap.Int("status", res.statusCode),
		zap.Duration("latency", reqDuration),
		zap.String("latency_human", reqDuration.String()),
		zap.Int64("bytes_in", bytesIn),
		zap.Int64("bytes_out", res.bytesOut),
	}
}
