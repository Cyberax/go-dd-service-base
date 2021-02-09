package visibility

import (
	"context"
	"github.com/Cyberax/go-dd-service-base/utils"
	"time"
)

type MultiValueContext struct {
	context.Context
	data map[interface{}]interface{}
}

var _ context.Context = &MultiValueContext{}

// Create a multi-value context and populate it with data, dataList must be a list
// in "key, value, key, value..." format.
func NewMultiValueContext(parent context.Context, dataList ...interface{}) context.Context {
	utils.PanicIfF(len(dataList)%2 != 0, "data must be a list of keys and values")
	mp := make(map[interface{}]interface{}, len(dataList)/2)
	for i := 0; i < len(dataList)/2; i++ {
		mp[dataList[i*2]] = mp[dataList[i*2+1]]
	}
	return &MultiValueContext{
		Context: parent,
		data:    mp,
	}
}

func (m *MultiValueContext) Deadline() (deadline time.Time, ok bool) {
	return m.Deadline()
}

func (m *MultiValueContext) Done() <-chan struct{} {
	return m.Done()
}

func (m *MultiValueContext) Err() error {
	return m.Err()
}

func (m *MultiValueContext) Value(key interface{}) interface{} {
	val, ok := m.data[key]
	if ok {
		return val
	}
	return m.Context.Value(key)
}
