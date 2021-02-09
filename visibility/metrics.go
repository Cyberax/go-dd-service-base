package visibility

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	. "github.com/Cyberax/go-dd-service-base/utils"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"strings"
	"sync"
	"time"
)

const MetricsContextKey = "MetricContext"

type MetricsContext struct {
	Lock    sync.Mutex
	OpName  string
	Metrics map[string]*MetricEntry

	sink statsd.ClientInterface
	span tracer.Span
}

type MetricEntry struct {
	Val       float64
	Unit      cloudwatch.StandardUnit
	Timestamp time.Time
}

// Normalize unit to use the smallest possible unit: microsecond, bit, byte
func (e MetricEntry) Normalize() (float64, cloudwatch.StandardUnit) {
	switch e.Unit {
	case cloudwatch.StandardUnitSeconds:
		return e.Val * 1e6, cloudwatch.StandardUnitMicroseconds
	case cloudwatch.StandardUnitMicroseconds:
		return e.Val, cloudwatch.StandardUnitMicroseconds
	case cloudwatch.StandardUnitMilliseconds:
		return e.Val * 1e3, cloudwatch.StandardUnitMicroseconds
	case cloudwatch.StandardUnitBytes:
		return e.Val, cloudwatch.StandardUnitBytes
	case cloudwatch.StandardUnitKilobytes:
		return e.Val * 1024, cloudwatch.StandardUnitBytes
	case cloudwatch.StandardUnitMegabytes:
		return e.Val * 1024 * 1024, cloudwatch.StandardUnitBytes
	case cloudwatch.StandardUnitGigabytes:
		return e.Val * 1024 * 1024 * 1024, cloudwatch.StandardUnitBytes
	case cloudwatch.StandardUnitTerabytes:
		return e.Val * 1024 * 1024 * 1024 * 1024, cloudwatch.StandardUnitBytes
	case cloudwatch.StandardUnitBits:
		return e.Val, cloudwatch.StandardUnitBits
	case cloudwatch.StandardUnitKilobits:
		return e.Val * 1024, cloudwatch.StandardUnitBits
	case cloudwatch.StandardUnitMegabits:
		return e.Val * 1024 * 1024, cloudwatch.StandardUnitBits
	case cloudwatch.StandardUnitGigabits:
		return e.Val * 1024 * 1024 * 1024, cloudwatch.StandardUnitBits
	case cloudwatch.StandardUnitTerabits:
		return e.Val * 1024 * 1024 * 1024 * 1024, cloudwatch.StandardUnitBits
	case cloudwatch.StandardUnitPercent:
		return e.Val, cloudwatch.StandardUnitPercent
	case cloudwatch.StandardUnitCount:
		return e.Val, cloudwatch.StandardUnitCount
	case cloudwatch.StandardUnitBytesSecond:
		return e.Val, cloudwatch.StandardUnitBytesSecond
	case cloudwatch.StandardUnitKilobytesSecond:
		return e.Val * 1024, cloudwatch.StandardUnitBytesSecond
	case cloudwatch.StandardUnitMegabytesSecond:
		return e.Val * 1024 * 1024, cloudwatch.StandardUnitBytesSecond
	case cloudwatch.StandardUnitGigabytesSecond:
		return e.Val * 1024 * 1024 * 1024, cloudwatch.StandardUnitBytesSecond
	case cloudwatch.StandardUnitTerabytesSecond:
		return e.Val * 1024 * 1024 * 1024 * 1024, cloudwatch.StandardUnitBytesSecond
	case cloudwatch.StandardUnitBitsSecond:
		return e.Val, cloudwatch.StandardUnitBitsSecond
	case cloudwatch.StandardUnitKilobitsSecond:
		return e.Val * 1024, cloudwatch.StandardUnitBitsSecond
	case cloudwatch.StandardUnitMegabitsSecond:
		return e.Val * 1024 * 1024, cloudwatch.StandardUnitBitsSecond
	case cloudwatch.StandardUnitGigabitsSecond:
		return e.Val * 1024 * 1024 * 1024, cloudwatch.StandardUnitBitsSecond
	case cloudwatch.StandardUnitTerabitsSecond:
		return e.Val * 1024 * 1024 * 1024 * 1024, cloudwatch.StandardUnitBitsSecond
	case cloudwatch.StandardUnitCountSecond:
		return e.Val, cloudwatch.StandardUnitCountSecond
	case cloudwatch.StandardUnitNone:
		return e.Val, cloudwatch.StandardUnitNone
	}
	return e.Val, cloudwatch.StandardUnitNone
}

func MakeMetricContext(ctx context.Context, opName string) context.Context {
	return context.WithValue(ctx, MetricsContextKey,
		&MetricsContext{
			OpName:  opName,
			Metrics: map[string]*MetricEntry{},
		})
}

func GetMetricsFromContext(ctx context.Context) *MetricsContext {
	res, ok := ctx.Value(MetricsContextKey).(*MetricsContext)
	PanicIfF(!ok, "No metrics context attached")

	return res
}

func TryGetMetricsFromContext(ctx context.Context) *MetricsContext {
	res, ok := ctx.Value(MetricsContextKey).(*MetricsContext)
	if !ok {
		return nil
	}
	return res
}

// Remove all metrics for the context, useful for tests
func (m *MetricsContext) Reset() {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	m.Metrics = make(map[string]*MetricEntry)
}

func (m *MetricsContext) GetMetric(name string) (val float64, unit cloudwatch.StandardUnit) {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	curVal := m.Metrics[name]
	if curVal == nil {
		return 0, cloudwatch.StandardUnitNone
	}

	return curVal.Val, curVal.Unit
}

func (m *MetricsContext) GetMetricVal(name string) float64 {
	v, _ := m.GetMetric(name)
	return v
}

func (m *MetricsContext) AddMetric(name string, val float64, unit cloudwatch.StandardUnit) {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	curVal := m.Metrics[name]
	if curVal == nil {
		m.Metrics[name] = &MetricEntry{
			Val:       val,
			Unit:      unit,
			Timestamp: time.Now(),
		}
		return
	}

	PanicIfF(curVal.Unit != unit, "inconsistent unit assignment, was %s want %s",
		curVal.Unit, unit)
	curVal.Val += val
}

func (m *MetricsContext) SetMetric(name string, val float64, unit cloudwatch.StandardUnit) {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	ent := &MetricEntry{Val: val, Unit: unit, Timestamp: time.Now()}
	m.Metrics[name] = ent
}

func (m *MetricsContext) AddCount(name string, val float64) {
	m.AddMetric(name, val, cloudwatch.StandardUnitCount)
}

func (m *MetricsContext) SetCount(name string, val float64) {
	m.SetMetric(name, val, cloudwatch.StandardUnitCount)
}

func (m *MetricsContext) AddDuration(name string, duration time.Duration) {
	m.AddMetric(name, duration.Seconds(), cloudwatch.StandardUnitSeconds)
}

func (m *MetricsContext) SetDuration(name string, duration time.Duration) {
	m.SetMetric(name, duration.Seconds(), cloudwatch.StandardUnitSeconds)
}

type TimeMeasurement struct {
	parent *MetricsContext
	name   string
	start  time.Time
}

func (m *MetricsContext) Benchmark(name string) *TimeMeasurement {
	return &TimeMeasurement{
		parent: m,
		name:   name,
		start:  time.Now(),
	}
}

func (t *TimeMeasurement) Done() {
	t.parent.AddDuration(t.name, time.Now().Sub(t.start))
}

func (m *MetricsContext) CopyToSpan(span tracer.Span) {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	for name, val := range m.Metrics {
		normVal, normUnit := val.Normalize()
		span.SetTag(name, normVal)
		if normUnit != cloudwatch.StandardUnitCount {
			span.SetTag(name+"_unit", m.normalizeUnitName(normUnit))
		}
	}
}

func (m *MetricsContext) CopyToStatsd(client statsd.ClientInterface, clientType string) {
	m.Lock.Lock()
	defer m.Lock.Unlock()

	for name, val := range m.Metrics {
		normVal, normUnit := val.Normalize()
		normUnitName := m.normalizeUnitName(normUnit)

		_ = client.Distribution(m.OpName+"."+name, normVal,
			[]string{"unit:" + normUnitName, "client-type:" + clientType}, 1)
	}
}

func (m *MetricsContext) normalizeUnitName(unit cloudwatch.StandardUnit) string {
	normUnitName := strings.Title(string(unit))
	normUnitName = strings.ReplaceAll(normUnitName, "/", "Per")
	return ToSnakeCase(normUnitName, '_')
}

type statsdKey struct{}

var statsdKeyValue = &statsdKey{}

func ContextWithStatsd(ctx context.Context, sink statsd.ClientInterface) context.Context {
	return context.WithValue(ctx, statsdKeyValue, sink)
}

func GetStatsdFromContext(ctx context.Context) statsd.ClientInterface {
	val := ctx.Value(statsdKeyValue)
	if val == nil {
		return &statsd.NoOpClient{}
	}
	return val.(statsd.ClientInterface)
}

type clientTypeKey struct{}

var clientTypeKeyValue = &clientTypeKey{}

func ContextWithClientType(ctx context.Context, ct string) context.Context {
	return context.WithValue(ctx, clientTypeKeyValue, ct)
}

func GetClientTypeFromContext(ctx context.Context) string {
	val := ctx.Value(clientTypeKeyValue)
	if val == nil {
		return ClientTypeNormal
	}
	return val.(string)
}
