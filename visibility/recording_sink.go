package visibility

import (
	"github.com/DataDog/datadog-go/statsd"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"time"
)

type RecordingSink struct {
	Distributions map[string]float64
	Counts        map[string]int64
	Tags          map[string][]string
}

func NewRecordingSink() *RecordingSink {
	return &RecordingSink{
		Distributions: make(map[string]float64),
		Counts:        make(map[string]int64),
		Tags:          make(map[string][]string),
	}
}

func(r *RecordingSink) Clear() {
	r.Distributions = make(map[string]float64)
	r.Counts = make(map[string]int64)
	r.Tags = make(map[string][]string)
}

func (r *RecordingSink) Gauge(_ string, _ float64, _ []string, _ float64) error {
	return nil
}

func (r *RecordingSink) Count(name string, value int64, tags []string, _ float64) error {
	r.Counts[name] = value
	r.Tags[name] = tags
	return nil
}

func (r *RecordingSink) Histogram(_ string, _ float64, _ []string, _ float64) error {
	return nil
}

func (r *RecordingSink) Distribution(name string, value float64, tags []string, _ float64) error {
	r.Distributions[name] = value
	r.Tags[name] = tags
	return nil
}

func (r *RecordingSink) Decr(_ string, _ []string, _ float64) error {
	return nil
}

func (r *RecordingSink) Incr(_ string, _ []string, _ float64) error {
	return nil
}

func (r *RecordingSink) Set(_ string, _ string, _ []string, _ float64) error {
	return nil
}

func (r *RecordingSink) Timing(_ string, _ time.Duration, _ []string, _ float64) error {
	return nil
}

func (r *RecordingSink) TimeInMilliseconds(_ string, _ float64, _ []string, _ float64) error {
	return nil
}

func (r *RecordingSink) Event(_ *statsd.Event) error {
	return nil
}

func (r *RecordingSink) SimpleEvent(_, _ string) error {
	return nil
}

func (r *RecordingSink) ServiceCheck(_ *statsd.ServiceCheck) error {
	return nil
}

func (r *RecordingSink) SimpleServiceCheck(_ string, _ statsd.ServiceCheckStatus) error {
	return nil
}

func (r *RecordingSink) Close() error {
	return nil
}

func (r *RecordingSink) Flush() error {
	return nil
}

func (r *RecordingSink) SetWriteTimeout(_ time.Duration) error {
	return nil
}

type FakeSpan struct {
	tags map[string]interface{}
}

func (f *FakeSpan) SetTag(key string, value interface{}) {
	f.tags[key] = value
}

func (f *FakeSpan) SetOperationName(operationName string) {
}

func (f *FakeSpan) BaggageItem(key string) string {
	return ""
}

func (f *FakeSpan) SetBaggageItem(key, val string) {
}

func (f *FakeSpan) Finish(opts ...ddtrace.FinishOption) {
}

func (f *FakeSpan) Context() ddtrace.SpanContext {
	return nil
}
