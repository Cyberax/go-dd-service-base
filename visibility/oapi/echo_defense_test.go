package oapi

import (
	"context"
	"fmt"
	"github.com/Cyberax/go-dd-service-base/utils"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEchoReqTooLarge(t *testing.T) {
	e := echo.New()
	AttachDefenseAgainstDarkArts(e, 1000, 100 * time.Millisecond)
	//noinspection GoUnhandledErrorResult
	e.POST("/", func(ctx echo.Context) error {
		_, err := ioutil.ReadAll(ctx.Request().Body)
		defer ctx.Request().Body.Close()
		if err != nil {
			return err
		}
		return ctx.String(200, "Hi!")
	})

	aLongLine := utils.MakeRandomStr(10000)

	// Try a too large request with content-length set
	req, err := http.NewRequest(http.MethodPost, "/", strings.NewReader(aLongLine))
	assert.NoError(t, err)
	rec := httptest.NewRecorder()
	e.Server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)

	// Now try it without the content length
	req, err = http.NewRequest(http.MethodPost, "/", strings.NewReader(aLongLine))
	assert.NoError(t, err)
	req.ContentLength = 0
	rec = httptest.NewRecorder()
	e.Server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)

	// Test direct-to-echo HTTP
	req, err = http.NewRequest(http.MethodPost, "/", strings.NewReader(aLongLine))
	assert.NoError(t, err)
	client := NewEchoTargetedHttpClient(e)
	response, err := client.Do(req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusRequestEntityTooLarge, response.StatusCode)
}

const testRequest = `GET / HTTP/1.1
User-Agent: Mozilla/4.0 (compatible; MSIE5.01; Windows NT)
Host: localhost
Accept-Language: en-us
Accept-Encoding: gzip, deflate
Connection: close
X-Strange-Filler: a_a_a_a_a_a_a_a_a_a_a_a_a_a_a

`

func TestEchoSlowLoris(t *testing.T) {
	// Test slow-loris attacks
	e := echo.New()
	//noinspection GoUnhandledErrorResult
	defer e.Shutdown(context.Background())

	AttachDefenseAgainstDarkArts(e, 100000, 100 * time.Millisecond)
	e.GET("/", func(ctx echo.Context) error {
		return ctx.String(200, "Hi!")
	})

	port, err := utils.GetFreeTcpPort()
	assert.NoError(t, err)
	addr := fmt.Sprintf("[::0]:%d", port)

	// Start the server
	go func() {
		_ = e.Start(addr)
	}()

	// Wait for the connection to become online
	for ;; {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// A regular-speed request works fine
	err = testReq(addr, t, 0)
	assert.NoError(t, err)

	// But a slow-loris exits with EPIPE
	err = testReq(addr, t, 10)
	assert.Error(t, err)
	assert.True(t, strings.HasSuffix(err.Error(), "broken pipe"))
}

func testReq(addr string, t *testing.T, delayMillis int64) error {
	reqText := []byte(strings.ReplaceAll(testRequest, "\n", "\r\n"))
	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)

	//noinspection GoUnhandledErrorResult
	defer conn.Close()

	written := 0
	for ; written < len(reqText); {
		remains := len(reqText) - written
		if remains > 5 {
			remains = 5
		}

		_, err = conn.Write(reqText[ written : written+remains ])
		written += remains
		if err != nil {
			return err
		}

		if delayMillis != 0 {
			time.Sleep(time.Duration(delayMillis) * time.Millisecond)
		}
	}

	bytes, err := ioutil.ReadAll(conn)
	if err != nil {
		return err
	}

	if !strings.HasPrefix(string(bytes), "HTTP/1.1 200 OK") {
		return fmt.Errorf("bad exit code")
	}

	return nil
}
