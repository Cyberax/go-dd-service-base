package visibility

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMetricsContext(t *testing.T) {
	ctx := MakeMetricContext(context.Background(), "TestOp")
	mctx := GetMetricsFromContext(ctx)

	// Add metric can be called first, without a SetMetric
	mctx.AddMetric("zonk", 10, cloudwatch.StandardUnitCount)

	mctx.SetCount("count1", 11)
	mctx.SetCount("count1", 12) // Will override
	mctx.AddCount("count1", 2)

	mctx.SetMetric("speed", 123, cloudwatch.StandardUnitGigabitsSecond)
	mctx.AddMetric("speed", 2, cloudwatch.StandardUnitGigabitsSecond)

	mctx.SetDuration("duration", time.Millisecond*500)
	mctx.AddDuration("duration", time.Second*2)

	bench := mctx.Benchmark("delay")
	time.Sleep(500 * time.Millisecond)
	bench.Done()

	fakeSink := NewRecordingSink()
	mctx.CopyToStatsd(fakeSink, "ThisClientType")

	assert.Equal(t, "client-type:ThisClientType", fakeSink.Tags["TestOp.duration"][1])
	assert.Equal(t, "TestOp", mctx.OpName)

	assert.Equal(t, float64(14), fakeSink.Distributions["TestOp.count1"])

	assert.True(t, fakeSink.Distributions["TestOp.delay"] > 0.5*1000000)
	assert.Equal(t, "unit:microseconds", fakeSink.Tags["TestOp.delay"][0])

	assert.Equal(t, 2.5*1e6, fakeSink.Distributions["TestOp.duration"])
	assert.Equal(t, "unit:microseconds", fakeSink.Tags["TestOp.duration"][0])

	assert.Equal(t, 125.0*1024*1024*1024, fakeSink.Distributions["TestOp.speed"])
	assert.Equal(t, "unit:bits_per_second", fakeSink.Tags["TestOp.speed"][0])

	assert.Equal(t, float64(10), fakeSink.Distributions["TestOp.zonk"])

	z1, zu := mctx.GetMetric("zonk")
	assert.Equal(t, 10.0, z1)
	assert.Equal(t, cloudwatch.StandardUnitCount, zu)
	assert.Equal(t, 10.0, mctx.GetMetricVal("zonk"))

	// Non-existing metric
	assert.Equal(t, 0.0, mctx.GetMetricVal("badbad"))

	mctx.Reset()
	mctx.Reset() // Idempotent

	z1, zu = mctx.GetMetric("zonk")
	assert.Equal(t, 0.0, z1)
	assert.Equal(t, cloudwatch.StandardUnitNone, zu)
	assert.Equal(t, 0.0, mctx.GetMetricVal("zonk"))
}

func TestMetricsSubmission(t *testing.T) {
	ctx := context.Background()
	ctx = MakeMetricContext(ctx, "TestCtxOriginal") // An original context
	ctx = MakeMetricContext(ctx, "TestCtx") // Save metrics into the context

	for i := 0; i < 17; i++ {
		mctx := GetMetricsFromContext(ctx)
		assert.Equal(t, "TestCtx", mctx.OpName)
		mctx.AddCount(fmt.Sprintf("count%d", i), 2)
		mctx.AddMetric(fmt.Sprintf("met%d", i), float64(i), cloudwatch.StandardUnitBytes)
	}

	fc := &FakeSpan{tags: map[string]interface{}{}}
	GetMetricsFromContext(ctx).CopyToSpan(fc)

	for i := 0; i < 17; i++ {
		assert.Equal(t, float64(2), fc.tags[fmt.Sprintf("count%d", i)])
		assert.Nil(t, fc.tags[fmt.Sprintf("count%d_unit", i)])

		assert.Equal(t, float64(i), fc.tags[fmt.Sprintf("met%d", i)])
		assert.Equal(t, "bytes", fc.tags[fmt.Sprintf("met%d_unit", i)])
	}
}
