package oapi

import (
	"context"
	"fmt"
	"github.com/cyberax/go-dd-service-base/utils"
	. "github.com/cyberax/go-dd-service-base/visibility"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

const schema = `
{
  "openapi": "3.0.0",
  "info": {
    "version": "1.0.0",
    "title": "Test API"
  },
  "paths": {
    "/api/run/{res}": {
      "get": {
        "summary": "Run something",
        "operationId": "runSomething",
        "parameters": [
          {
            "name": "res",
            "in": "path",
            "required": true,
            "description": "Action type",
            "schema": {
              "type": "string"
            }
          }
        ],
        "responses": {
          "200": {
            "description": "OK"
          },
          "default": {
            "description": "unexpected error"
          }
        }
      }
    }
  }
}
`

func setupServer(t *testing.T, logger *zap.Logger,
	metrics *RecordingSink, listener net.Listener) *echo.Echo {
	// First, set up a minimal Echo server
	e := echo.New()
	e.HideBanner = true

	// Insert the logging/tracing middleware
	tmo := TracingAndMetricsOptions{
		DebugMode:  true,
		SampleRate: aws.Float64(1.0),
		Statsd:     metrics,
		Logger:     logger,
	}
	e.Use(TracingAndLoggingMiddlewareHook(tmo))

	swagger, err := openapi3.NewSwaggerLoader().LoadSwaggerFromData([]byte(schema))
	assert.NoError(t, err)

	e.Use(OapiRequestValidatorWithMetrics(swagger, "/api", nil))

	e.GET("/api/run/*", func(ctx echo.Context) error {
		c := ctx.Request().Context()
		path := ctx.Request().URL.Path
		CLS(c).Infof("From inside handler %s", path)
		ct := GetClientTypeFromContext(c)

		if strings.HasSuffix(path, "ok") {
			GetMetricsFromContext(c).AddCount("Frob", 1)
			if ct != "Vasja" {
				panic("Bad Client Type")
			}
			return ctx.JSONBlob(http.StatusOK, []byte(`{"hello": "world"}`))
		}
		if strings.HasSuffix(path, "error") {
			if ct != ClientTypeCanary {
				panic("Bad Client Type")
			}
			return echo.NewHTTPError(http.StatusConflict, "An error")
		}
		if strings.HasSuffix(path, "bad") {
			if ct != ClientTypeNormal {
				panic("Bad Client Type")
			}
			return fmt.Errorf("logic error")
		}

		time.Sleep(200 * time.Millisecond)
		panic("unknown parameter")
	})

	go func() {
		_ = e.Server.Serve(listener)
	}()

	return e
}

func TestEchoTracing(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	sink, logger := utils.NewMemorySinkLogger()
	metricsSink := NewRecordingSink()

	lstn, err := net.Listen("tcp", "[::]:9123")
	assert.NoError(t, err)
	//noinspection GoUnhandledErrorResult
	defer lstn.Close()

	e := setupServer(t, logger, metricsSink, lstn)
	//noinspection GoUnhandledErrorResult
	defer e.Shutdown(context.Background())

	testOkCall(t, sink, mt, metricsSink)
	testRegularError(t, sink, mt, metricsSink)
	testLogicError(t, sink, mt, metricsSink)
	testPanic(t, sink, mt, metricsSink)

	resp, err := http.Get("http://[::]:9123/api/unknown")
	assert.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)

	resp, err = http.Get("http://[::]:9123/api/run?param=123")
	assert.NoError(t, err)
	assert.Equal(t, 400, resp.StatusCode)
}

func testOkCall(t *testing.T, logSink *utils.MemorySink, segSink mocktracer.Tracer,
	metSink *RecordingSink) {
	defer segSink.Reset()
	defer metSink.Clear()
	defer logSink.Reset()

	req, err := http.NewRequest("GET", "http://[::]:9123/api/run/ok", nil)
	assert.NoError(t, err)

	// Add client type header
	span, ctx := tracer.StartSpanFromContext(context.Background(), "clientSide")
	span.SetBaggageItem(ClientTypeTag, "Vasja")
	defer span.Finish()
	req = req.WithContext(ctx)
	err = tracer.Inject(span.Context(), tracer.HTTPHeadersCarrier(req.Header))
	assert.NoError(t, err)

	// Add tracers
	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)

	assert.Equal(t, 1, len(segSink.FinishedSpans()))
	seg := segSink.FinishedSpans()[0]
	assert.Equal(t, "RunSomething", seg.OperationName())
	assert.Equal(t, "oapi.RunSomething", seg.Tag("resource.name"))
	assert.Equal(t, span.Context().TraceID(), seg.TraceID())

	// Check metrics
	assert.Equal(t, 5, len(metSink.Distributions))

	assert.Equal(t, float64(0), metSink.Distributions["RunSomething.Fault"])
	assert.Equal(t, float64(1), metSink.Distributions["RunSomething.Success"])
	assert.Equal(t, float64(0), metSink.Distributions["RunSomething.Error"])
	assert.True(t, metSink.Distributions["RunSomething.Time"] >= 0)

	assert.Equal(t, float64(1), metSink.Distributions["RunSomething.Frob"])

	assert.True(t, strings.Contains(logSink.String(), `"msg":"Request finished"`))
}

func testRegularError(t *testing.T, logSink *utils.MemorySink,
	segSink mocktracer.Tracer, metSink *RecordingSink) {
	defer segSink.Reset()
	defer metSink.Clear()
	defer logSink.Reset()

	req, err := http.NewRequest("GET", "http://[::]:9123/api/run/error", nil)
	assert.NoError(t, err)

	// Add client type header
	span, ctx := tracer.StartSpanFromContext(context.Background(), "clientSide")
	span.SetBaggageItem(ClientTypeTag, ClientTypeCanary)
	defer span.Finish()
	req = req.WithContext(ctx)
	err = tracer.Inject(span.Context(), tracer.HTTPHeadersCarrier(req.Header))
	assert.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, 409, resp.StatusCode)

	assert.Equal(t, 1, len(segSink.FinishedSpans()))
	seg := segSink.FinishedSpans()[0]
	assert.Equal(t, span.Context().TraceID(), seg.TraceID())

	// Check metrics
	assert.Equal(t, 4, len(metSink.Distributions))
	assert.Equal(t, float64(0), metSink.Distributions["RunSomething.Fault"])
	assert.Equal(t, float64(0), metSink.Distributions["RunSomething.Success"])
	assert.Equal(t, float64(1), metSink.Distributions["RunSomething.Error"])

	assert.True(t, strings.Contains(logSink.String(), `"msg":"Request error"`))
}

func testPanic(t *testing.T, logSink *utils.MemorySink,
	segSink mocktracer.Tracer, sink *RecordingSink) {
	defer segSink.Reset()
	defer sink.Clear()
	defer logSink.Reset()

	resp, err := http.Get("http://[::]:9123/api/run/panic")
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode)

	assert.Equal(t, 4, len(sink.Distributions))
	assert.Equal(t, float64(1), sink.Distributions["RunSomething.Fault"])
	assert.Equal(t, float64(0), sink.Distributions["RunSomething.Success"])
	assert.Equal(t, float64(1), sink.Distributions["RunSomething.Error"])
	assert.True(t, sink.Distributions["RunSomething.Time"] >= 0.2)

	assert.True(t, strings.Contains(logSink.String(), `"unknown parameter"`))
	assert.True(t, strings.Contains(logSink.String(), "stacktrace"))
}

func testLogicError(t *testing.T, logSink *utils.MemorySink,
	segSink mocktracer.Tracer, sink *RecordingSink) {
	defer segSink.Reset()
	defer sink.Clear()
	defer logSink.Reset()

	resp, err := http.Get("http://[::]:9123/api/run/bad")
	assert.NoError(t, err)
	assert.Equal(t, 500, resp.StatusCode)

	assert.Equal(t, 4, len(sink.Distributions))
	assert.Equal(t, float64(0), sink.Distributions["RunSomething.Fault"])
	assert.Equal(t, float64(0), sink.Distributions["RunSomething.Success"])
	assert.Equal(t, float64(1), sink.Distributions["RunSomething.Error"])
	assert.True(t, sink.Distributions["RunSomething.Time"] >= 0)

	assert.True(t, strings.Contains(logSink.String(), `"error":"logic error"`))
}
