package oapi

import (
	"github.com/labstack/echo/v4"
	"net/http"
	"net/http/httptest"
)

type DirectEchoTransport struct {
	Echo *echo.Echo
}

func (f *DirectEchoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	f.Echo.Server.Handler.ServeHTTP(rec, req)
	return rec.Result(), nil
}

func NewEchoTargetedHttpClient(ec *echo.Echo) http.Client {
	return http.Client{Transport: &DirectEchoTransport{Echo: ec}}
}
