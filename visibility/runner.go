package visibility

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const ClientTypeTag = "client-type"
const ClientTypeNormal = "normal"
const ClientTypeCanary = "canary"

func ClientTypeFromSpan(sp tracer.Span) string {
	item := sp.BaggageItem(ClientTypeTag)
	if item == "" {
		return ClientTypeNormal
	}
	return item
}

//RunInstrumented() traces the provided synchronous function by
//beginning and closing a new subsegment around its execution.
//If the parent segment doesn't exist yet then a new top-level segment is created
func RunInstrumented(ctx context.Context, name string, fn func(context.Context) error) error {
	logger := CL(ctx)
	statsd := GetStatsdFromContext(ctx)
	clientType := GetClientTypeFromContext(ctx)

	span, ctx := tracer.StartSpanFromContext(ctx, name,
		tracer.SpanType("background"))
	span.SetTag(ext.ResourceName, name)
	span.SetTag(ClientTypeTag, clientType)
	span.SetOperationName(name)

	var err error

	defer func() {
		if p := recover(); p != nil {
			// Create an error with a nice stack trace
			stack := NewShortenedStackTrace(3, true,
				fmt.Sprintf("%v", p))
			span.SetTag(ext.ErrorStack, stack.StringStack())
			span.SetTag("panic", fmt.Sprintf("%v", p))
			span.Finish(tracer.WithError(fmt.Errorf("gopanic: %v", p)))
			panic(p)
		} else {
			if err != nil {
				span.Finish(tracer.WithError(err))
			} else {
				span.Finish()
			}
		}
	}()

	logger = logger.Named(name).With(
		zap.String("dd.trace_id", fmt.Sprintf("%d", span.Context().TraceID())),
		zap.String("dd.span_id", fmt.Sprintf("%d", span.Context().SpanID())),
	)
	ctx = ImbueContext(ctx, logger)             // Save logger into the context
	ctx = MakeMetricContext(ctx, name)    // Save metrics into the context

	met := GetMetricsFromContext(ctx)
	defer met.CopyToStatsd(statsd, clientType)
	defer met.CopyToSpan(span)

	err = fn(ctx)

	return err
}

func InstrumentWithMetrics(ctx context.Context, fn func(context.Context) error) error {
	met := GetMetricsFromContext(ctx)
	met.AddCount("Success", 0)
	met.AddCount("Error", 0)
	met.AddCount("Fault", 1) // Panic trick (see below)

	bench := met.Benchmark("Time")
	defer bench.Done()

	err := fn(ctx)

	// We have set Fault to 1 initially. If the function panics then we never reach
	// this statement and the value of 1 propagates to the caller. However, if we
	// do reach this, then it means that the fault (panic) hasn't happened and we
	// need to reset it.
	met.AddCount("Fault", -1)

	if err == nil {
		met.AddCount("Success", 1)
	} else {
		met.AddCount("Error", 1)
	}

	return err
}
