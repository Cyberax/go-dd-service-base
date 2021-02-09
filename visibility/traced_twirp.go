package visibility

import (
	"context"
	"fmt"
	"github.com/Cyberax/go-dd-service-base/utils"
	"github.com/twitchtv/twirp"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"runtime/pprof"
)

type contextKey int

const (
	twirpErrorKey    contextKey = 0
	RequestTimingKey contextKey = 1
)

const StackTraceKey = "StackTrace"

type TracedTwirp struct {
	serviceName string
}

func MakeTraceHooks(serviceName string) *twirp.ServerHooks {
	tt := TracedTwirp{
		serviceName: serviceName,
	}

	return &twirp.ServerHooks{
		RequestRouted: tt.requestRoutedHook,
		ResponseSent:  tt.responseSentHook,
		Error:         tt.errorHook,
	}
}

func (t *TracedTwirp) requestRoutedHook(ctx context.Context) (context.Context, error) {
	span, ok := tracer.SpanFromContext(ctx)
	utils.PanicIfF(!ok, "no tracing context")

	pkg, ok := twirp.PackageName(ctx)
	utils.PanicIfF(!ok, "no package in request")
	svc, ok := twirp.ServiceName(ctx)
	utils.PanicIfF(!ok, "no service in request")
	method, ok := twirp.MethodName(ctx)
	utils.PanicIfF(!ok, "no method in request")

	span.SetTag("twirp.package", pkg)
	span.SetTag("twirp.service", svc)
	span.SetTag("twirp.method", method)
	span.SetTag(ext.ResourceName, svc+"."+method)
	span.SetOperationName(svc+"."+method)

	metCtx := MakeMetricContext(ctx, svc+"."+method)
	bench := GetMetricsFromContext(metCtx).Benchmark("Time")
	metCtx = context.WithValue(metCtx, RequestTimingKey, bench)

	// Set the pprof labels for the thread
	traceId := fmt.Sprintf("%d", span.Context().TraceID())
	labelCtx := pprof.WithLabels(context.Background(),
		pprof.Labels("twirp", svc + "." + method, "dd", traceId))
	pprof.SetGoroutineLabels(labelCtx)

	return metCtx, nil
}

func (t *TracedTwirp) responseSentHook(ctx context.Context) {
	span, ok := tracer.SpanFromContext(ctx)
	if !ok {
		return
	}
	if sc, ok := twirp.StatusCode(ctx); ok {
		span.SetTag(ext.HTTPCode, sc)
	}

	err, _ := ctx.Value(twirpErrorKey).(twirp.Error)
	isPanic := err != nil && err.Msg() == "Internal service panic"

	// Collect and send metrics
	met := TryGetMetricsFromContext(ctx)
	clientType := GetClientTypeFromContext(ctx)
	statsd := GetStatsdFromContext(ctx)
	if met != nil {
		if isPanic {
			met.SetCount("Fault", 1)
			met.SetCount("Error", 0)
			met.SetCount("Success", 0)
		} else if err != nil {
			met.SetCount("Fault", 0)
			met.SetCount("Error", 1)
			met.SetCount("Success", 0)
		} else {
			met.SetCount("Fault", 0)
			met.SetCount("Error", 0)
			met.SetCount("Success", 1)
		}
		bench, ok := ctx.Value(RequestTimingKey).(*TimeMeasurement)
		if ok && bench != nil {
			bench.Done()
		}
		met.CopyToSpan(span)
		met.CopyToStatsd(statsd, clientType)
	} else {
		// TODO: check for BadRouteError?
	}

	if err != nil {
		if err.Meta(StackTraceKey) != "" {
			span.SetTag(ext.ErrorStack, err.Meta(StackTraceKey))
			span.Finish(tracer.WithError(err))
		} else if isPanic {
			stack := NewShortenedStackTrace(0, true, err.Msg())
			span.SetTag(ext.ErrorStack, stack.StringStack())
			span.Finish(tracer.WithError(err))
		} else {
			span.Finish(tracer.WithError(err))
		}
	} else {
		span.Finish()
	}
}

func (t *TracedTwirp) errorHook(ctx context.Context, err twirp.Error) context.Context {
	return context.WithValue(ctx, twirpErrorKey, err)
}

func WithStack(err twirp.Error) twirp.Error {
	trace := NewShortenedStackTrace(3, false, "")
	return err.WithMeta(StackTraceKey, trace.StringStack())
}
