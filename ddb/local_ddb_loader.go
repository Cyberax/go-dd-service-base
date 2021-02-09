package ddb

import (
	"bufio"
	"github.com/cyberax/go-dd-service-base/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/defaults"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"os"
	"os/exec"
	"strconv"
	"testing"
)

type TestContext struct {
	Conn   *dynamodb.Client
	Config aws.Config
	Ddb    *exec.Cmd
	Port   uint16
}

//noinspection GoUnhandledErrorResult
func (ctx *TestContext) Close() {
	ctx.Ddb.Process.Kill()
	ctx.Ddb.Wait()
}

func NewDdbTestContext(t *testing.T, ddbDir string, failOnErr bool) *TestContext {
	// Get a free port
	port, e := utils.GetFreeTcpPort()
	if e != nil {
		t.FailNow()
	}

	// Try to launch the Local DDB
	cmd := exec.Command("java", "-Xmx256m",
		"-jar", "DynamoDBLocal.jar", "-inMemory", "-port", strconv.Itoa(port))
	out, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	cmd.Dir = ddbDir
	cmd.Stdin = os.Stdin

	e = cmd.Start()

	failer := t.SkipNow
	if failOnErr {
		failer = t.FailNow
	}

	if e != nil {
		t.Log("Can't launch DDB local")
		failer()
	}

	scanner := bufio.NewScanner(out)
	scanner.Split(bufio.ScanWords)
	var found = false
	for {
		scanner.Scan()
		if scanner.Err() != nil {
			t.Log("Can't launch DDB local")
			failer()
		}
		if scanner.Text() == "CorsParams:" {
			found = true
			break
		}
	}

	if !found {
		t.Log("Failed to initialize the DDB")
		failer()
	}

	config := defaults.Config()
	config.Region = "mock-region"
	config.EndpointResolver = aws.ResolveWithEndpointURL(
		"http://localhost:" + strconv.Itoa(port))
	config.Credentials = aws.StaticCredentialsProvider{
		Value: aws.Credentials{
			AccessKeyID: "AKID", SecretAccessKey: "SECRET", SessionToken: "SESSION",
			Source: "unit test credentials",
		},
	}

	return &TestContext{
		Conn:   dynamodb.New(config),
		Config: config,
		Ddb:    cmd,
		Port:   uint16(port),
	}
}
