package zaputils

import (
	"bufio"
	"github.com/cyberax/go-dd-service-base/visibility"
	"github.com/kami-zh/go-capturer"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"net"
	"os"
	"strings"
	"testing"
)

func TestTcpSink(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	assert.NoError(t, err)
	//noinspection GoUnhandledErrorResult
	defer listener.Close()

	witness := make(chan string, 2000)
	go func() {
		for ; ; {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(cn net.Conn) {
				reader := bufio.NewReader(cn)
				// Read lines before bailing out
				for i := 0; i < 2; i++ {
					lnBytes, _, _ := reader.ReadLine()
					witness <- string(lnBytes)
				}
				_ = cn.Close()
			}(conn)
		}
	}()
	_ = os.Setenv("DD_TCP_SINK", listener.Addr().String())
	//noinspection GoUnhandledErrorResult
	defer os.Setenv("DD_TCP_SINK", "")
	prod := ConfigureProdLogger()

	stack := visibility.NewShortenedStackTrace(1, false, "")
	prod.Error("this is bad", stack.Field())
	prod.Error("this is bad also", stack.Field())
	_ = prod.Sync()
	s1 := <-witness
	s2 := <-witness
	// Check for stack traces (line number of NewShortenedStackTrace constructor, might change)
	assert.True(t, strings.Contains(s1, "zaputils/pretty_zap_test.go:44"))
	assert.True(t, strings.Contains(s2, "zaputils/pretty_zap_test.go:44"))

	for i := 0; i < 1000; i++ {
		prod.Warn("this is not bad")
		_ = prod.Sync()
	}

	count := 0
outer:
	for ; ; {
		select {
		case ln := <-witness:
			assert.True(t, strings.Contains(ln, "this is not bad"))
			count++
		default:
			break outer
		}
	}

	// We've survived at least one reconnect
	assert.True(t, count > 1)
}

func TestPrettyStacks(t *testing.T) {
	out := capturer.CaptureStderr(func() {
		devLogger := ConfigureDevLogger()
		stack := visibility.NewShortenedStackTrace(2, false, "")
		devLogger.Error("this is bad", stack.Field())
	})

	// Check that we got the stack back, the line number is the line of
	// NewShortenedStack, might change during refactoring
	assert.True(t, strings.Contains(out, "zaputils/pretty_zap_test.go:78"))
}

func TestPrettyStacksStr(t *testing.T) {
	out := capturer.CaptureStderr(func() {
		devLogger := ConfigureDevLogger()
		stack := visibility.NewShortenedStackTrace(2, false, "")
		devLogger.Error("this is bad",
			zap.String("stacktrace", stack.StringStack()), zap.Int64("haha", 123))
	})

	// Check that we got the stack back, the line number is the line of
	// NewShortenedStack, might change during refactoring
	assert.True(t, strings.Contains(out, "pretty_zap_test.go:90 TestPrettyStacksStr"))
}

func TestFieldOverride(t *testing.T) {
	out := capturer.CaptureStderr(func() {
		devLogger := ConfigureDevLogger()
		devLogger = devLogger.With(zap.String("field1", "hello"),
			zap.String("field2", "world"))
		devLogger = devLogger.With(zap.String("field1", "goodbye"))
		devLogger.Info("Everything is OK")
	})

	// The field1 initial value was overridden
	assert.True(t, strings.Contains(out,
		"Everything is OK\t{\"field2\":\"world\",\"field1\":\"goodbye\"}"))
}
