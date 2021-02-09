// DataDog and ZapLog integration with the Echo web framework. The middleware in this
// module makes sure that Echo requests and the Golang request context always have
// logging and DataDog tracing set up.
//
// I realize that this module is _severely_ overloaded with functionality, but it's
// a trade-off, as we don't want to have deep caller stacks with loads of middleware.

package oapi

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	. "github.com/Cyberax/go-dd-service-base/utils"
	"github.com/Cyberax/go-dd-service-base/visibility"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"net/http"
	"runtime/pprof"
	"strconv"
	"time"
)

type TracingAndMetricsOptions struct {
	DebugMode  bool
	SampleRate *float64
	Statsd     statsd.ClientInterface

	Logger *zap.Logger
}

func (t *TracingAndMetricsOptions) Validate() {
	PanicIfF(t.Logger == nil, "logger was not set")
}

type traceAndLogMiddleware struct {
	next echo.HandlerFunc
	opts TracingAndMetricsOptions
}

func (z *traceAndLogMiddleware) prepareCommonLogFields(c echo.Context,
	reqDuration time.Duration) []zap.Field {

	req := c.Request()
	res := c.Response()

	// Now log whatever happened
	bytesIn, err := strconv.ParseInt(req.Header.Get(echo.HeaderContentLength),
		10, 64)
	if err != nil {
		bytesIn = 0
	}
	p := req.URL.Path
	if p == "" {
		p = "/"
	}

	return []zap.Field{
		zap.String("path", p),
		zap.String("remote_ip", c.RealIP()),
		zap.String("host", req.Host),
		zap.String("method", req.Method),
		zap.String("uri", req.RequestURI),
		zap.String("referer", req.Referer()),
		zap.String("user_agent", req.UserAgent()),
		zap.Int("status", res.Status),
		zap.Duration("latency", reqDuration),
		zap.String("latency_human", reqDuration.String()),
		zap.Int64("bytes_in", bytesIn),
		zap.Int64("bytes_out", res.Size),
	}
}

func (z *traceAndLogMiddleware) instrumentRequest(c echo.Context) error {
	//// Skip non-API requests
	//if !strings.HasPrefix(c.Path(), z.opts.Prefix) {
	//	return z.next(c)
	//}

	req := c.Request()
	opts := []tracer.StartSpanOption{
		tracer.SpanType(ext.SpanTypeWeb),
		tracer.Tag(ext.HTTPMethod, req.Method),
		tracer.Tag(ext.HTTPURL, c.Path()),
	}
	if z.opts.SampleRate != nil {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, *z.opts.SampleRate))
	}
	if spanctx, err := tracer.Extract(tracer.HTTPHeadersCarrier(req.Header)); err == nil {
		opts = append(opts, tracer.ChildOf(spanctx))
	}

	// We start with an 'unknown' method, it will be overridden in the OAPI handler
	// once the method name is known.
	span, ctx := tracer.StartSpanFromContext(req.Context(),
		"oapi.unknown", opts...)
	defer span.Finish()

	// Copy the 'baggage' from other tracers
	reqId := req.Header.Get("Request-Id")
	if reqId == "" {
		reqId = req.Header.Get("X-Request-Id")
	}
	if reqId != "" {
		span.SetTag("request-id", reqId)
		span.SetBaggageItem("request-id", reqId)
	}

	// Contextualize the logger
	traceId := fmt.Sprintf("%d", span.Context().TraceID())
	spanId := fmt.Sprintf("%d", span.Context().SpanID())

	// Return the tracing headers back to the caller
	if traceId != "0" && spanId != "0" {
		c.Response().Header().Add(tracer.DefaultTraceIDHeader, traceId)
		c.Response().Header().Add(tracer.DefaultParentIDHeader, spanId)
	}

	ctx = visibility.ContextWithStatsd(ctx, z.opts.Statsd)
	clientType := visibility.ClientTypeFromSpan(span)
	ctx = visibility.ContextWithClientType(ctx, clientType)

	// Set the pprof labels for the thread
	ctx = pprof.WithLabels(ctx,
		pprof.Labels("url", req.URL.String(), "dd", traceId))
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

	logger := z.opts.Logger.Named("HTTP").With(fields...)
	ctx = visibility.ImbueContext(ctx, logger) // Add the logger

	// Set up the metrics
	ctx = visibility.MakeMetricContext(ctx, "unknown")
	met := visibility.GetMetricsFromContext(ctx)
	defer met.CopyToStatsd(z.opts.Statsd, clientType)
	defer met.CopyToSpan(span)

	// Remember the context in the Echo request
	req = req.WithContext(ctx)
	c.SetRequest(req)

	logger.Info("Starting request")

	start := time.Now()
	// Protect against panics
	defer func() {
		report := recover()
		if report == nil {
			return
		}

		err := fmt.Errorf("%v", report)
		stack := visibility.NewShortenedStackTrace(0, true, err.Error())
		span.SetTag(ext.ErrorStack, stack.StringStack())
		span.Finish(tracer.WithError(err), tracer.NoDebugStack())

		// Send the 500 error along the way...
		if !c.Response().Committed {
			if z.opts.DebugMode {
				// Send the stack trace along with the error in dev mode
				errMsg := make(map[string]interface{})
				errMsg["reason"] = stack.Error()
				errMsg["stacktrace"] = stack.JSONStack()
				c.Error(echo.NewHTTPError(http.StatusInternalServerError, errMsg))
			} else {
				c.Error(echo.ErrInternalServerError)
			}
		}

		ch := z.prepareCommonLogFields(c, time.Now().Sub(start))
		logger.Info("Request fault", append(ch, zap.Error(stack),
			stack.Field())...)
	}()

	// Actually process the request
	if err := z.next(c); err != nil {
		// We have an error, process it
		c.Error(err)
		ch := z.prepareCommonLogFields(c, time.Now().Sub(start))
		httpErr, ok := err.(*echo.HTTPError)
		if ok {
			// HTTP errors contain a redundant code field
			logger.Info("Request error",
				append(ch, zap.Reflect("error", httpErr.Message))...)
			span.SetTag(ext.Error, err)
		} else {
			logger.Info("Request error", append(ch, zap.Error(err))...)
			span.SetTag(ext.Error, err)
		}
		return nil // Error is not propagated further
	}

	logger.Info("Request finished",
		z.prepareCommonLogFields(c, time.Now().Sub(start))...)

	return nil
}

// Insert middleware responsible for logging, metrics and tracing
func TracingAndLoggingMiddlewareHook(opts TracingAndMetricsOptions) echo.MiddlewareFunc {
	opts.Validate()

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		zlm := &traceAndLogMiddleware{
			opts: opts,
			next: next,
		}
		return zlm.instrumentRequest
	}
}
