package utils

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestTime(t *testing.T) {
	clock := StaticClock(10000)
	assert.Equal(t, int64(10000), clock().Unix())
	assert.Equal(t, 0, clock().Nanosecond())

	at := FromTimeSec(clock())
	assert.Equal(t, int64(10000), at.ToUnix())
	assert.Equal(t, int64(10000), at.ToTime().Unix())
}

func TestPanicIfF(t *testing.T) {
	PanicIfF(false, "hello")

	assert.PanicsWithValue(t, "bad panic error", func() {
		PanicIfF(true, "bad panic %s", fmt.Errorf("error"))
	})

	assert.PanicsWithValue(t, "error", func() {
		PanicIfErr(fmt.Errorf("error"))
	})
}

func TestMakeRandomStr(t *testing.T) {
	assert.Equal(t, 20, len(MakeRandomStr(10)))
}

func TestMemorySinkLogger(t *testing.T) {
	sink, logger := NewMemorySinkLogger()
	logger.Info("hello, world")
	assert.Equal(t, "{\"level\":\"info\",\"msg\":\"hello, world\"}\n", sink.String())
	_ = sink.Close()
}

func TestGetFreeTcpPort(t *testing.T) {
	port, err := GetFreeTcpPort()
	assert.NoError(t, err)
	l1, err := net.Listen("tcp", fmt.Sprintf("[::0]:%d", port))
	assert.NoError(t, err)
	//noinspection GoUnhandledErrorResult
	defer l1.Close()

	port2, err := GetFreeTcpPort()
	assert.NoError(t, err)
	l2, err := net.Listen("tcp", fmt.Sprintf("[::0]:%d", port2))
	assert.NoError(t, err)
	//noinspection GoUnhandledErrorResult
	defer l2.Close()

	assert.NotEqual(t, port, port2)
}

func TestMustJson(t *testing.T) {
	args := map[string]interface{} {
		"Hello": "world",
	}
	json := MustJson(args)
	assert.Equal(t, `{"Hello":"world"}`, json)

	json = MustJsonIndent(args, " ")
	assert.Equal(t, "{\n \"Hello\": \"world\"\n}", json)

	// Test panics
	argsBad := map[interface{}]interface{} {
		"Hello": "world",
	}

	assert.Panics(t, func() {MustJson(argsBad)})
	assert.Panics(t, func() {MustJsonIndent(argsBad, " ")})
}

func TestNullables(t *testing.T) {
	assert.Equal(t, "", NullableStr(nil))
	assert.Equal(t, "hello", NullableStr("hello"))

	assert.True(t, ToNullStr("aa").Valid)
	assert.True(t, ToNullStr("aa").String == "aa")

	assert.True(t, ToSqlI64(aws.Int64(123)).Valid)
	assert.True(t, ToSqlI64(aws.Int64(123)).Int64 == 123)
}

func TestToSnakeCase(t *testing.T) {
	assert.Equal(t, "a_snake_case", ToSnakeCase("ASnakeCase", '_'))
	assert.Equal(t, "cia_snake_1", ToSnakeCase("CIA_snake1", '_'))

	assert.Equal(t, "a-snake-case", ToSnakeCase("ASnakeCase", '-'))
	assert.Equal(t, "cia-snake-1", ToSnakeCase("CIA_snake1", '-'))

	assert.Equal(t, "terra-canary", ToSnakeCase("terra.canary", '-'))
	assert.Equal(t, "terra-canary", ToSnakeCase("Terra.Canary", '-'))
}
