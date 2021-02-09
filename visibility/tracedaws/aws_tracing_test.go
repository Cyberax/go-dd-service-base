// Unless explicitly stated otherwise all files in this directory are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.

package tracedaws

import (
	"context"
	"github.com/cyberax/go-dd-service-base/utils"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

func TestAWS(t *testing.T) {
	am := utils.NewAwsMockHandler()
	am.AddHandler(func(ctx context.Context, arg *ec2.TerminateInstancesInput) (
		*ec2.TerminateInstancesOutput, error) {
		return &ec2.TerminateInstancesOutput{}, nil
	})

	var ec *ec2.Client

	tester := func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		root, ctx := tracer.StartSpanFromContext(context.Background(), "test")

		_, _ = ec.TerminateInstancesRequest(&ec2.TerminateInstancesInput{
			InstanceIds: []string{"i-123"},
		}).Send(ctx)

		root.Finish()

		spans := mt.FinishedSpans()
		assert.Len(t, spans, 2)
		assert.Equal(t, spans[1].TraceID(), spans[0].TraceID())

		s := spans[0]
		assert.Equal(t, "ec2.command", s.OperationName())
		assert.Contains(t, s.Tag(tagAWSAgent), "aws-sdk-go")
		assert.Equal(t, "TerminateInstances", s.Tag(tagAWSOperation))
		assert.Equal(t, "us-mars-1", s.Tag(tagAWSRegion))
		assert.Equal(t, "ec2.TerminateInstances", s.Tag(ext.ResourceName))
		assert.Equal(t, "aws.ec2", s.Tag(ext.ServiceName))
		assert.Equal(t, "POST", s.Tag(ext.HTTPMethod))
		assert.Equal(t, "https://ec2.us-mars-1.amazonaws.com/", s.Tag(ext.HTTPURL))
	}

	// Test instrumentation with the session-local instrumentation
	awsConfig := am.AwsConfig()
	ec = ec2.New(awsConfig)
	InstrumentHandlers(&ec.Handlers)
	t.Run("ec2", tester)

	// Now try config-wide instrumentation
	awsConfig = am.AwsConfig()
	InstrumentHandlers(&awsConfig.Handlers)
	ec = ec2.New(awsConfig)
	t.Run("ec2-global", tester)
}

func TestAnalyticsSettings(t *testing.T) {
	am := utils.NewAwsMockHandler()
	am.AddHandler(func(ctx context.Context, arg *ec2.TerminateInstancesInput) (
		*ec2.TerminateInstancesOutput, error) {
		return &ec2.TerminateInstancesOutput{}, nil
	})

	awsConfig := am.AwsConfig()

	assertRate := func(t *testing.T, mt mocktracer.Tracer, rate interface{}, opts ...Option) {
		ec := ec2.New(awsConfig)
		InstrumentHandlers(&ec.Handlers, opts...)

		_, _ = ec.TerminateInstancesRequest(&ec2.TerminateInstancesInput{
			InstanceIds: []string{"i-123"},
		}).Send(context.Background())

		spans := mt.FinishedSpans()
		assert.Len(t, spans, 1)
		s := spans[0]
		assert.Equal(t, rate, s.Tag(ext.EventSampleRate))
	}

	t.Run("defaults", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		assertRate(t, mt, nil)
	})

	t.Run("enabled", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		assertRate(t, mt, 1.0, WithAnalytics(true))
	})

	t.Run("disabled", func(t *testing.T) {
		mt := mocktracer.Start()
		defer mt.Stop()

		assertRate(t, mt, nil, WithAnalytics(false))
	})

	t.Run("override", func(t *testing.T) {
		tracer.Start(tracer.WithAnalyticsRate(0.4))
		tracer.Stop()

		mt := mocktracer.Start()
		defer mt.Stop()

		assertRate(t, mt, 0.23, WithAnalyticsRate(0.23))
	})
}

