package utils

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/assert"
	"testing"
)

type tester struct {
}

//noinspection GoUnusedParameter
func (t *tester) TerminateInstances(ctx context.Context,
	input *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error) {
	return nil, aws.NewErrParamRequired("something")
}

//noinspection GoUnusedParameter
func (t *tester) AlmostRunDescribe1(ctx context.Context,
	input *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, string) {
	return nil, ""
}

//noinspection GoUnusedParameter
func (t *tester) AlmostRunDescribe2(input *ec2.DescribeInstancesInput, _ string) (
	*ec2.DescribeInstancesOutput, error) {
	return nil, nil
}

//noinspection GoUnusedParameter
func (t *tester) AlmostRunDescribe3(ctx context.Context,
	input *ec2.DescribeAccountAttributesInput) (*ec2.DescribeInstancesOutput, error) {
	return nil, nil
}

//noinspection GoUnusedParameter
func (t *tester) AlmostRunDescribe4(input *ec2.DescribeInstancesInput) (
	*ec2.DescribeInstancesOutput, error) {
	return nil, nil
}

//noinspection GoUnusedParameter
func (t *tester) AlmostRunDescribe5(ctx context.Context,
	input *ec2.DescribeInstancesInput) error {
	return nil
}

func TestMockNotFound(t *testing.T) {
	am := AwsMockHandler{}
	am.AddHandler(&tester{})

	assert.Panics(t, func() {
		_, _ = am.invokeMethod(context.Background(), &ec2.DescribeInstancesInput{
			MaxResults: aws.Int64(11)})
	}, "could not find a handler")
}

func TestAwsMock(t *testing.T) {
	am := NewAwsMockHandler()
	am.AddHandler(&tester{})
	am.AddHandler(func(ctx context.Context, arg *ec2.DescribeInstancesInput) (
		*ec2.DescribeInstancesOutput, error) {
		return &ec2.DescribeInstancesOutput{NextToken: arg.NextToken}, nil
	})
	am.AddHandler(func(ctx context.Context, arg *ec2.TerminateInstancesInput) (
		*ec2.DescribeInstancesOutput, error) {
		return nil, nil
	})

	ec := ec2.New(am.AwsConfig())

	response, e := ec.DescribeInstancesRequest(&ec2.DescribeInstancesInput{
		NextToken:  aws.String("hello, token"),
	}).Send(context.Background())
	assert.NoError(t, e)
	assert.Equal(t, "hello, token", *response.NextToken)

	// Check the tester methods
	_, err := ec.TerminateInstancesRequest(&ec2.TerminateInstancesInput{}).Send(
		context.Background())
	assert.Error(t, err, "something")
}

func ExampleNewAwsMockHandler() {
	am := NewAwsMockHandler()
	am.AddHandler(func(ctx context.Context, arg *ec2.TerminateInstancesInput) (
		*ec2.TerminateInstancesOutput, error) {

		if arg.InstanceIds[0] != "i-123" {
			panic("BadInstanceId")
		}
		return &ec2.TerminateInstancesOutput{}, nil
	})

	ec := ec2.New(am.AwsConfig())

	_, _ = ec.TerminateInstancesRequest(&ec2.TerminateInstancesInput{
		InstanceIds: []string{"i-123"},
	}).Send(context.Background())
}
