package visibility

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"strings"
	"testing"
)

func TestRunInstrumented(t *testing.T) {
	ms := &statsd.NoOpClient{}
	mt := mocktracer.Start()
	defer mt.Stop()

	ctx := ImbueContext(context.Background(), zap.NewNop())
	ctx = ContextWithStatsd(ctx, ms)

	err := RunInstrumented(ctx, "test1",
		func(c context.Context) error {
			_, ok := tracer.SpanFromContext(c)
			if !ok {
				panic("No span!")
			}
			return fmt.Errorf("test err")
		})

	assert.Error(t, err, "test err")

	span0 := mt.FinishedSpans()[0]
	assert.Equal(t, "test1", span0.OperationName())
	assert.Equal(t, fmt.Errorf("test err"), span0.Tag("error"))
}

func TestRunInstrumentedPanic(t *testing.T) {
	ms := &statsd.NoOpClient{}
	mt := mocktracer.Start()
	defer mt.Stop()

	ctx := ImbueContext(context.Background(), zap.NewNop())
	ctx = ContextWithStatsd(ctx, ms)

	assert.Panics(t, func() {
		_ = RunInstrumented(ctx, "test1",
			func(c context.Context) error {
				panic("bad panic")
			})
	}, "bad panic")

	span0 := mt.FinishedSpans()[0]
	assert.Equal(t, "test1", span0.OperationName())
	assert.Equal(t, fmt.Errorf("gopanic: bad panic"), span0.Tag("error"))
	assert.Equal(t, "bad panic", span0.Tag("panic"))
	es := strings.Split(span0.Tag("error.stack").(string), "\n")
	// The line number of the panic line, might change during refactoring
	assert.True(t, strings.HasSuffix(es[0], "runner_test.go:51 TestRunInstrumentedPanic.func1.1"))
}

func TestSegmentWithMetrics(t *testing.T) {
	rs := NewRecordingSink()
	mt := mocktracer.Start()
	defer mt.Stop()

	ctx := ImbueContext(context.Background(), zap.NewNop())
	ctx = ContextWithStatsd(ctx, rs)

	err := RunInstrumented(ctx, "test1",
		func(c context.Context) error {
			met := GetMetricsFromContext(c)
			met.AddCount("hellocount", 1)
			met.AddMetric("gigametric", 12, cloudwatch.StandardUnitGigabits)
			return nil
		})
	assert.NoError(t, err)

	// Metrics must be streamed!
	assert.Equal(t, float64(1), rs.Distributions["test1.hellocount"])
	assert.Equal(t, 12.0*1024*1024*1024, rs.Distributions["test1.gigametric"])

	// Check that the span also has the correct metrics
	span0 := mt.FinishedSpans()[0]
	assert.Equal(t, float64(1), span0.Tag("hellocount"))
	assert.Equal(t, 12.0*1024*1024*1024, span0.Tag("gigametric"))
	assert.Equal(t, "bits", span0.Tag("gigametric_unit"))
}

func TestInstrumentedWithMetrics(t *testing.T) {
	rs := NewRecordingSink()
	mt := mocktracer.Start()
	defer mt.Stop()

	testWithPanic(t, rs)

	assert.Equal(t, float64(1), rs.Distributions["test1.Fault"])
	assert.Equal(t, float64(0), rs.Distributions["test1.Success"])
	assert.Equal(t, float64(0), rs.Distributions["test1.Error"])

	mt.Reset()
	rs.Clear()

	ctx := ImbueContext(context.Background(), zap.NewNop())
	ctx = ContextWithStatsd(ctx, rs)

	err := RunInstrumented(ctx, "test1",
		func(c context.Context) error {
			return InstrumentWithMetrics(c, func(ctx context.Context) error {
				return fmt.Errorf("bad error")
			})
		})
	assert.Error(t, err, "bad error")

	assert.Equal(t, float64(0), rs.Distributions["test1.Fault"])
	assert.Equal(t, float64(0), rs.Distributions["test1.Success"])
	assert.Equal(t, float64(1), rs.Distributions["test1.Error"])

	mt.Reset()
	rs.Clear()

	err = RunInstrumented(ctx, "test1",
		func(c context.Context) error {
			return InstrumentWithMetrics(c, func(ctx context.Context) error {
				return nil
			})
		})
	assert.NoError(t, err)

	assert.Equal(t, float64(0), rs.Distributions["test1.Fault"])
	assert.Equal(t, float64(1), rs.Distributions["test1.Success"])
	assert.Equal(t, float64(0), rs.Distributions["test1.Error"])
}

func testWithPanic(t *testing.T, rs *RecordingSink) {
	defer func() {
		p := recover()
		if p == nil {
			t.Fail()
		}
	}()

	ctx := ImbueContext(context.Background(), zap.NewNop())
	ctx = ContextWithStatsd(ctx, rs)

	_ = RunInstrumented(ctx, "test1",
		func(c context.Context) error {
			_ = InstrumentWithMetrics(c, func(ctx context.Context) error {
				panic("Hello")
			})
			return nil
		})

	assert.Fail(t, "expected panic")
}
