package dada

import (
	"context"
	"fmt"
	"github.com/cyberax/go-dd-service-base/utils"
	"github.com/gorilla/mux"
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
	router := mux.NewRouter()
	router.Path("/").HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, err := ioutil.ReadAll(request.Body)
		if err == ReqTooLargeError {
			writer.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}

		//noinspection GoUnhandledErrorResult
		defer request.Body.Close()
		if err != nil {
			writer.WriteHeader(400)
		} else {
			writer.WriteHeader(200)
			//noinspection GoUnhandledErrorResult
			writer.Write([]byte("Hi!"))
		}
	})

	server := ServerWithDefenseAgainstDarkArts(1000, 100*time.Millisecond, router)
	aLongLine := utils.MakeRandomStr(10000)

	// Try a too large request with content-length set
	req, err := http.NewRequest(http.MethodPost, "/", strings.NewReader(aLongLine))
	assert.NoError(t, err)
	rec := httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)

	// Now try it without the content length
	req, err = http.NewRequest(http.MethodPost, "/", strings.NewReader(aLongLine))
	assert.NoError(t, err)
	req.ContentLength = 0
	rec = httptest.NewRecorder()
	server.Handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

const testRequest = `GET / HTTP/1.1
User-Agent: Mozilla/4.0 (compatible; MSIE5.01; Windows NT)
Host: localhost
Accept-Language: en-us
Accept-Encoding: gzip, deflate
Connection: close
X-Strange-Filler: a_a_a_a_a_a_a_a_a_a_a_a_a_a_a

`

func TestSlowLoris(t *testing.T) {
	// Test slow-loris attacks
	router := mux.NewRouter()
	router.Path("/").HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(200)
		_, _ = writer.Write([]byte("Hi!"))
	})

	server := ServerWithDefenseAgainstDarkArts(100000, 100*time.Millisecond, router)
	//noinspection GoUnhandledErrorResult
	defer server.Shutdown(context.Background())

	port, err := utils.GetFreeTcpPort()
	assert.NoError(t, err)
	addr := fmt.Sprintf("[::0]:%d", port)

	// Start the server
	go func() {
		server.Addr = addr
		_ = server.ListenAndServe()
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
