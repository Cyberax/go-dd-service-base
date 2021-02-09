// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2019 Datadog, Inc.
package visibility

import (
	"fmt"
	"math"
	"net/http"
	"strconv"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/twitchtv/twirp"
)

// TwirpHttpClient is duplicated from twirp's generated service code.
// It is declared in this package so that the client can be wrapped
// to initiate traces.
type TwirpHttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type wrappedClient struct {
	c                 TwirpHttpClient
	analyticsRate     float64
	clientServiceName string
	clientType        string
}

var DefAnalyticsRate = math.NaN()

// WrapTwirpClient wraps an TwirpHttpClient to add distributed tracing to its requests.
func WrapTwirpClient(c TwirpHttpClient, clientServiceName string,
	analyticsRate float64, clientType string) TwirpHttpClient {
	return &wrappedClient{c: c, clientServiceName: clientServiceName,
		analyticsRate: analyticsRate, clientType: clientType}
}

func WrapTwirpClientDef(c TwirpHttpClient, clientServiceName string) TwirpHttpClient {
	return WrapTwirpClient(c, clientServiceName, DefAnalyticsRate, ClientTypeNormal)
}

func (wc *wrappedClient) Do(req *http.Request) (*http.Response, error) {
	opts := []tracer.StartSpanOption{
		tracer.SpanType(ext.SpanTypeHTTP),
		tracer.ServiceName(wc.clientServiceName),
		tracer.Tag(ext.HTTPMethod, req.Method),
		tracer.Tag(ext.HTTPURL, req.URL.Path),
	}
	ctx := req.Context()
	if pkg, ok := twirp.PackageName(ctx); ok {
		opts = append(opts, tracer.Tag("twirp.package", pkg))
	}

	svc, ok := twirp.ServiceName(ctx)
	if !ok {
		svc = "twirp"
	}
	opts = append(opts, tracer.Tag("twirp.service", svc))

	method, ok := twirp.MethodName(ctx)
	if !ok {
		method = "request"
	}
	opts = append(opts, tracer.Tag("twirp.method", method))

	if !math.IsNaN(wc.analyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, wc.analyticsRate))
	}
	if spanctx, err := tracer.Extract(tracer.HTTPHeadersCarrier(req.Header)); err == nil {
		opts = append(opts, tracer.ChildOf(spanctx))
	}

	span, ctx := tracer.StartSpanFromContext(req.Context(),
		svc+"."+method, opts...)
	defer span.Finish()
	if span.BaggageItem(ClientTypeTag) == "" {
		span.SetBaggageItem(ClientTypeTag, wc.clientType)
	}

	err := tracer.Inject(span.Context(), tracer.HTTPHeadersCarrier(req.Header))
	if err != nil {
		panic(fmt.Sprintf("twirp: failed to inject http headers: %v\n", err))
	}

	req = req.WithContext(ctx)
	res, err := wc.c.Do(req)
	if err != nil {
		span.SetTag(ext.Error, err)
	} else {
		span.SetTag(ext.HTTPCode, strconv.Itoa(res.StatusCode))
		// treat 4XX and 5XX as errors for a client
		if res.StatusCode >= 400 {
			span.SetTag(ext.Error, true)
			span.SetTag(ext.ErrorMsg, fmt.Sprintf("%d: %s", res.StatusCode, http.StatusText(res.StatusCode)))
		}
	}
	return res, err
}
