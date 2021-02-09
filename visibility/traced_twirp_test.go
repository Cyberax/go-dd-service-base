// This file is licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.
package visibility

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"net"
	"net/http"
	"strings"
	"testing"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"

	"github.com/stretchr/testify/assert"
	"github.com/twitchtv/twirp"
	"github.com/twitchtv/twirp/ctxsetters"
	"github.com/twitchtv/twirp/example"
)

func mockServer(hooks *twirp.ServerHooks, assert *assert.Assertions, twerr twirp.Error) {
	_, ctx := tracer.StartSpanFromContext(context.Background(), "Op1")
	ctx = ctxsetters.WithPackageName(ctx, "twirp.test")
	ctx = ctxsetters.WithServiceName(ctx, "Example")
	var err error
	if hooks.RequestReceived != nil {
		ctx, err = hooks.RequestReceived(ctx)
		assert.NoError(err)
	}

	ctx = ctxsetters.WithMethodName(ctx, "Method")
	if hooks.RequestRouted != nil {
		ctx, err = hooks.RequestRouted(ctx)
		assert.NoError(err)
	}

	if twerr != nil {
		ctx = ctxsetters.WithStatusCode(ctx, twirp.ServerHTTPStatusFromErrorCode(twerr.Code()))
		if hooks.Error != nil {
			ctx = hooks.Error(ctx, twerr)
		}
	} else {
		if hooks.ResponsePrepared != nil {
			ctx = hooks.ResponsePrepared(ctx)
		}
		ctx = ctxsetters.WithStatusCode(ctx, http.StatusOK)
	}

	if hooks.ResponseSent != nil {
		hooks.ResponseSent(ctx)
	}
}

func TestServerHooks(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()
	hooks := MakeTraceHooks("twirp-test")

	t.Run("success", func(t *testing.T) {
		defer mt.Reset()
		ass := assert.New(t)

		mockServer(hooks, ass, nil)

		spans := mt.FinishedSpans()
		ass.Len(spans, 1)
		span := spans[0]
		ass.Equal("Example.Method", span.OperationName())
		ass.Equal("twirp.test", span.Tag("twirp.package"))
		ass.Equal("Example", span.Tag("twirp.service"))
		ass.Equal("Method", span.Tag("twirp.method"))
		ass.Equal("200", span.Tag(ext.HTTPCode))
	})

	t.Run("error", func(t *testing.T) {
		defer mt.Reset()
		ass := assert.New(t)

		mockServer(hooks, ass, twirp.InternalError("something bad or unexpected happened"))

		spans := mt.FinishedSpans()
		ass.Len(spans, 1)
		span := spans[0]
		ass.Equal("Example.Method", span.OperationName())
		ass.Equal("twirp.test", span.Tag("twirp.package"))
		ass.Equal("Example", span.Tag("twirp.service"))
		ass.Equal("Method", span.Tag("twirp.method"))
		ass.Equal("500", span.Tag(ext.HTTPCode))
		ass.Equal("twirp error internal: something bad or unexpected happened",
			span.Tag(ext.Error).(error).Error())
	})
}

type notifyListener struct {
	net.Listener
	ch chan<- struct{}
}

func (n *notifyListener) Accept() (c net.Conn, err error) {
	if n.ch != nil {
		close(n.ch)
		n.ch = nil
	}
	return n.Listener.Accept()
}

type haberdasher int32

func (h haberdasher) MakeHat(ctx context.Context, size *example.Size) (*example.Hat, error) {
	header, ok := GetHttpRequestHeader(ctx)
	if !ok || header.Get("Accept") == "" {
		panic("no header")
	}
	if GetClientTypeFromContext(ctx) != "myClient" {
		panic("bad client type")
	}

	if size.Inches == 42 {
		panic("A very bad idea")
	}
	if size.Inches != int32(h) {
		return nil, WithStack(twirp.InvalidArgumentError("Inches",
			"Only size of %d is allowed"))
	}
	hat := &example.Hat{
		Size:  size.Inches,
		Color: "purple",
		Name:  "doggie beanie",
	}
	return hat, nil
}

func TestHaberdash(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()
	ass := assert.New(t)

	l, err := net.Listen("tcp4", "127.0.0.1:0")
	ass.NoError(err)
	//noinspection GoUnhandledErrorResult
	defer l.Close()

	readyCh := make(chan struct{})
	nl := &notifyListener{Listener: l, ch: readyCh}

	rs := NewRecordingSink()
	hooks := MakeTraceHooks("twirp-test")

	server := example.NewHaberdasherServer(haberdasher(6), hooks)
	gorilla := NewTracedGorilla(server, zap.NewNop(), rs, aws.Float64(1), aws.Float64(1))

	muxer := mux.NewRouter()
	gorilla.AttachGorillaToMuxer(muxer)

	errCh := make(chan error)
	go func() {
		err := http.Serve(nl, muxer)
		if err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-readyCh:
		break
	case err := <-errCh:
		ass.FailNow("server not started", err)
	}

	client := example.NewHaberdasherJSONClient("http://"+nl.Addr().String(),
		WrapTwirpClient(&http.Client{}, "tester", DefAnalyticsRate,
		"myClient"))

	hat, err := client.MakeHat(context.Background(), &example.Size{Inches: 6})
	ass.NoError(err)

	// Success
	ass.Equal("purple", hat.Color)
	spans := mt.FinishedSpans()
	ass.Len(spans, 2)
	ass.Equal(ext.SpanTypeWeb, spans[0].Tag(ext.SpanType))
	ass.Equal(ext.SpanTypeHTTP, spans[1].Tag(ext.SpanType))

	ass.Equal(float64(1), rs.Distributions["Haberdasher.MakeHat.Success"])
	ass.Equal(float64(0), rs.Distributions["Haberdasher.MakeHat.Fault"])
	ass.Equal(float64(0), rs.Distributions["Haberdasher.MakeHat.Error"])

	// Regular error
	mt.Reset()
	rs.Clear()
	hat2, err := client.MakeHat(context.Background(), &example.Size{Inches: 12})
	ass.Equal(twirp.InvalidArgument, err.(twirp.Error).Code())
	ass.Nil(hat2)

	spans = mt.FinishedSpans()
	stack := strings.Split(spans[0].Tag(ext.ErrorStack).(string), "\n")
	// Line number might break after refactoring. It's the line with the WithStack() statement
	ass.True(strings.Contains(stack[0], "traced_twirp_test.go:127 haberdasher.MakeHat"))
	ass.Equal(float64(0), rs.Distributions["Haberdasher.MakeHat.Success"])
	ass.Equal(float64(0), rs.Distributions["Haberdasher.MakeHat.Fault"])
	ass.Equal(float64(1), rs.Distributions["Haberdasher.MakeHat.Error"])

	// Panic
	mt.Reset()
	rs.Clear()
	hat3, err := client.MakeHat(context.Background(), &example.Size{Inches: 42})
	ass.Equal(twirp.Internal, err.(twirp.Error).Code())
	ass.Nil(hat3)

	spans = mt.FinishedSpans()
	stack = strings.Split(spans[0].Tag(ext.ErrorStack).(string), "\n")
	// Line number might break after refactoring. It's the line with the panic() statement
	ass.True(strings.Contains(stack[0], "traced_twirp_test.go:124 haberdasher.MakeHat"))
	ass.Equal(float64(0), rs.Distributions["Haberdasher.MakeHat.Success"])
	ass.Equal(float64(1), rs.Distributions["Haberdasher.MakeHat.Fault"])
	ass.Equal(float64(0), rs.Distributions["Haberdasher.MakeHat.Error"])
}
