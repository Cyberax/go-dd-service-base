// Licensed under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package tracedaws

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"math"
	"strconv"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

const (
	tagAWSAgent     = "aws.agent"
	tagAWSOperation = "aws.operation"
	tagAWSRegion    = "aws.region"
)

type instrumenter struct {
	cfg *config
}

func InstrumentHandlers(handlers *aws.Handlers, opts ...Option) {
	cfg := new(config)
	defaults(cfg)
	for _, opt := range opts {
		opt(cfg)
	}
	h := &instrumenter{cfg: cfg}
	handlers.Send.PushFrontNamed(aws.NamedHandler{
		Name: "visibility/aws/handlers.Send",
		Fn:   h.Send,
	})
	handlers.Complete.PushFrontNamed(aws.NamedHandler{
		Name: "visibility/aws/handlers.Complete",
		Fn:   h.Complete,
	})
}

func (h *instrumenter) Send(req *aws.Request) {
	opts := []ddtrace.StartSpanOption{
		tracer.SpanType(ext.SpanTypeHTTP),
		tracer.ServiceName(h.serviceName(req)),
		tracer.ResourceName(h.resourceName(req)),
		tracer.Tag(tagAWSAgent, h.awsAgent(req)),
		tracer.Tag(tagAWSOperation, h.awsOperation(req)),
		tracer.Tag(tagAWSRegion, h.awsRegion(req)),
		tracer.Tag(ext.HTTPMethod, req.Operation.HTTPMethod),
		tracer.Tag(ext.HTTPURL, req.HTTPRequest.URL.String()),
	}
	if !math.IsNaN(h.cfg.analyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, h.cfg.analyticsRate))
	}
	_, ctx := tracer.StartSpanFromContext(req.Context(), h.operationName(req), opts...)
	req.SetContext(ctx)
}

func (h *instrumenter) Complete(req *aws.Request) {
	span, ok := tracer.SpanFromContext(req.Context())
	if !ok {
		return
	}
	if req.HTTPResponse != nil {
		span.SetTag(ext.HTTPCode, strconv.Itoa(req.HTTPResponse.StatusCode))
	}
	span.Finish(tracer.WithError(req.Error))
}

func (h *instrumenter) operationName(req *aws.Request) string {
	return h.awsService(req) + ".command"
}

func (h *instrumenter) resourceName(req *aws.Request) string {
	return h.awsService(req) + "." + req.Operation.Name
}

func (h *instrumenter) serviceName(req *aws.Request) string {
	if h.cfg.serviceName != "" {
		return h.cfg.serviceName
	}
	return "aws." + h.awsService(req)
}

func (h *instrumenter) awsAgent(req *aws.Request) string {
	if agent := req.HTTPRequest.Header.Get("User-Agent"); agent != "" {
		return agent
	}
	return "aws-sdk-go"
}

func (h *instrumenter) awsOperation(req *aws.Request) string {
	return req.Operation.Name
}

func (h *instrumenter) awsRegion(req *aws.Request) string {
	return req.Metadata.SigningRegion
}

func (h *instrumenter) awsService(req *aws.Request) string {
	return req.Metadata.SigningName
}
