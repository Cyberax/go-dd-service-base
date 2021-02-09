package oapi

import (
"github.com/labstack/echo/v4"
	"io"
	"net/http"
"time"
)

var ReqTooLargeError = echo.NewHTTPError(
	http.StatusRequestEntityTooLarge, "request is too large")

// Attach middleware to Echo to prevent slow-loris attacks and DDoS-es by extremely large
// requests.
func AttachDefenseAgainstDarkArts(e *echo.Echo, maxRequestSize int, timeout time.Duration) {
	e.Server.MaxHeaderBytes = maxRequestSize

	// Limit the total request time
	e.Server.ReadHeaderTimeout = timeout
	e.Server.ReadTimeout = timeout
	e.Server.WriteTimeout = timeout
	e.Server.IdleTimeout = timeout

	// Limit the total body size
	e.Pre(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(i echo.Context) error {
			// If there's content length set, try the check before
			// doing the read.
			if i.Request().ContentLength > int64(maxRequestSize) {
				return ReqTooLargeError
			}
			i.Request().Body = LimitReaderWithErr(i.Request().Body,
				int64(maxRequestSize), ReqTooLargeError)
			return next(i)
		}
	})
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
