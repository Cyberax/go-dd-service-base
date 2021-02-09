package oapi

import (
	"github.com/Cyberax/go-dd-service-base/utils"
	"github.com/labstack/gommon/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

const expectation = `{"level":"info","msg":"Print1print2"}
{"level":"info","msg":"Formatted 123"}
{"level":"info","msg":"Message","data":{"Hello":"world","once":1}}
{"level":"debug","msg":"Debug1debug2"}
{"level":"debug","msg":"Formatted debug 123"}
{"level":"debug","msg":"Message","data":{"Hello":"debug","once":1}}
{"level":"info","msg":"Info1info2"}
{"level":"info","msg":"Formatted info 123"}
{"level":"info","msg":"Message","data":{"Hello":"info","once":1}}
{"level":"warn","msg":"Warn1warn2"}
{"level":"warn","msg":"Formatted warn 123"}
{"level":"warn","msg":"Message","data":{"Hello":"warn","once":1}}
{"level":"error","msg":"Error1error2"}
{"level":"error","msg":"Formatted error 123"}
{"level":"error","msg":"Message","data":{"Hello":"error","once":1}}
{"level":"panic","msg":"Panic1panic2"}
{"level":"panic","msg":"Formatted panic 123"}
{"level":"panic","msg":"Message","data":{"Hello":"panic"}}
{"level":"info","msg":"This is a writer message"}
`

func TestEchoWrapper(t *testing.T) {
	sink, logger := utils.NewMemorySinkLogger()

	wrapper := NewLoggerWrapper(logger)
	assert.Equal(t, logger, GetZapLoggerFromEchoLogger(wrapper))

	wrapper.Print("Print1", "print2")
	wrapper.Printf("Formatted %d", 123)
	wrapper.Printj(map[string]interface{}{"Hello": "world", "once": 1})

	wrapper.Debug("Debug1", "debug2")
	wrapper.Debugf("Formatted debug %d", 123)
	wrapper.Debugj(map[string]interface{}{"Hello": "debug", "once": 1})

	wrapper.Info("Info1", "info2")
	wrapper.Infof("Formatted info %d", 123)
	wrapper.Infoj(map[string]interface{}{"Hello": "info", "once": 1})

	wrapper.Warn("Warn1", "warn2")
	wrapper.Warnf("Formatted warn %d", 123)
	wrapper.Warnj(map[string]interface{}{"Hello": "warn", "once": 1})

	wrapper.Error("Error1", "error2")
	wrapper.Errorf("Formatted error %d", 123)
	wrapper.Errorj(map[string]interface{}{"Hello": "error", "once": 1})

	// Fatal messages terminate the process - so no testing here
	//wrapper.Fatal("Fatal1", "fatal2")
	//wrapper.Fatalf("Formatted fatal %d", 123)
	//wrapper.Fatalj(map[string]interface{}{"Hello": "fatal", "once": 1})

	assert.Panics(t, func() { wrapper.Panic("Panic1", "panic2") }, )
	assert.Panics(t, func() { wrapper.Panicf("Formatted panic %d", 123) })
	assert.Panics(t, func() { wrapper.Panicj(map[string]interface{}{"Hello": "panic"}) })

	assert.Panics(t, func() { wrapper.SetHeader("Hello") })
	assert.Panics(t, func() { wrapper.Prefix() })
	assert.Panics(t, func() { wrapper.SetPrefix("world") })
	assert.Panics(t, func() { wrapper.Level() })
	wrapper.SetLevel(log.ERROR) // No-op
	assert.Panics(t, func() { wrapper.SetOutput(nil) })

	_, _ = wrapper.Output().Write([]byte("This is a writer message"))

	result := sink.String()
	assert.Equal(t, expectation, result)
}

