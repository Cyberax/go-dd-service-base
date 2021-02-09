package dada

import (
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net/http"
	"time"
)

var ReqTooLargeError = fmt.Errorf("request is too large")

// Attach middleware to Echo to prevent slow-loris attacks and DDoS-es by extremely large
// requests.
func ServerWithDefenseAgainstDarkArts(maxRequestSize int, timeout time.Duration,
	muxer *mux.Router) *http.Server {

	server := &http.Server{}
	server.MaxHeaderBytes = maxRequestSize

	// Limit the total request time
	server.ReadHeaderTimeout = timeout
	server.ReadTimeout = timeout
	server.WriteTimeout = timeout
	server.IdleTimeout = timeout

	// Limit the total body size
	server.Handler = &sizeLimiter{
		muxer:          muxer,
		maxRequestSize: int64(maxRequestSize),
	}

	return server
}

type sizeLimiter struct {
	muxer          *mux.Router
	maxRequestSize int64
}

func (t sizeLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// If there's content length set, try the check before
	// doing the read.
	if r.ContentLength > t.maxRequestSize {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_, _ = w.Write([]byte("request is too large"))
		return
	}
	r.Body = LimitReaderWithErr(r.Body, t.maxRequestSize, ReqTooLargeError)
	t.muxer.ServeHTTP(w, r)
}

// LimitReader returns a Reader that reads from r
// but stops with an error after n bytes.
// The underlying implementation is a *LimitedReaderWithErr.
func LimitReaderWithErr(r io.ReadCloser, n int64, err error) io.ReadCloser {
	return &LimitedReaderWithErr{r, n, err}
}

// A LimitedReaderWithErr reads from Reader but limits the amount of
// data returned to just BytesLeft bytes. Each call to Read
// updates BytesLeft to reflect the new amount remaining.
// Read returns error when BytesLeft <= 0 or when the underlying Reader returns EOF.
type LimitedReaderWithErr struct {
	Reader    io.ReadCloser // underlying reader
	BytesLeft int64         // max bytes remaining
	Error     error         // the error to return in case of too much data
}

func (l *LimitedReaderWithErr) Close() error {
	return l.Reader.Close()
}

func (l *LimitedReaderWithErr) Read(p []byte) (n int, err error) {
	if l.BytesLeft <= 0 {
		return 0, l.Error
	}
	if int64(len(p)) > l.BytesLeft {
		p = p[0:l.BytesLeft]
	}
	n, err = l.Reader.Read(p)
	l.BytesLeft -= int64(n)
	return
}
