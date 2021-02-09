package oapi
// This module is responsible for uploading the metrics collected on OAPI requests
// to DD and the initial request validation.

import (
	"context"
	"fmt"
	. "github.com/cyberax/go-dd-service-base/utils"
	"github.com/cyberax/go-dd-service-base/visibility"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"net/http"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/labstack/echo/v4"
)

type AuthValidatorFunc func(e echo.Context, input *openapi3filter.AuthenticationInput) error

type requestValidationAndMetrics struct {
	router  *openapi3filter.Router
	apiPath string
	next    echo.HandlerFunc
	auth    AuthValidatorFunc
}

// Create middleware to validate requests against OAPI3 specification. Additionally
// this middleware initialized the metric context for the request with appropriate
// metrics and submits this segment at the end of the request.
//
// Each request gets annotated with the following metrics:
// Success: 0 or 1 (count). 0 if the request errors out or panics.
// Fault: 0 or 1 (count). 1 if the request panics.
// Time: request duration (time)
func OapiRequestValidatorWithMetrics(swagger *openapi3.Swagger, apiPath string,
	validator AuthValidatorFunc) echo.MiddlewareFunc {
	PanicIfF(apiPath == "", "API methods must have a common prefix")
	router := openapi3filter.NewRouter().WithSwagger(swagger)
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		val := requestValidationAndMetrics{
			router: router,
			next: next,
			apiPath: apiPath,
			auth: validator,
		}
		return val.validateAndRunWithMetrics
	}
}

func (r *requestValidationAndMetrics) validateAndRunWithMetrics(ctx echo.Context) error {
	req := ctx.Request()
	// This is not an API call, just let it go through
	if !strings.HasPrefix(req.URL.Path, r.apiPath) {
		return r.next(ctx)
	}
	route, pathParams, err := r.router.FindRoute(req.Method, req.URL)

	// We failed to find a matching route for the request.
	if err != nil {
		switch e := err.(type) {
		case *openapi3filter.RouteError:
			// We've got a bad request, the path requested doesn't match
			// either server, or path, or something.
			return echo.NewHTTPError(http.StatusBadRequest, e.Reason)
		default:
			// This should never happen today, but if our upstream code changes,
			// we don't want to crash the server, so handle the unexpected error.
			return echo.NewHTTPError(http.StatusInternalServerError,
				fmt.Sprintf("error validating route: %s", err.Error()))
		}
	}

	validationInput := &openapi3filter.RequestValidationInput{
		Request:    req,
		PathParams: pathParams,
		Route:      route,
		Options:    &openapi3filter.Options{
			AuthenticationFunc: func(_ context.Context,
				authInput *openapi3filter.AuthenticationInput) error {
				return r.auth(ctx, authInput)
			},
		},
	}

	err = openapi3filter.ValidateRequest(req.Context(), validationInput)
	if err != nil {
		switch e := err.(type) {
		case *openapi3filter.RequestError:
			// We've got a bad request
			// Split up the verbose error by lines and return the first one
			// openapi errors seem to be multi-line with a decent message on the first
			errorLines := strings.Split(e.Error(), "\n")
			return echo.NewHTTPError(http.StatusBadRequest, errorLines[0])
		case *openapi3filter.SecurityRequirementsError:
			return echo.NewHTTPError(http.StatusForbidden, e.Error())
		default:
			// This should never happen today, but if our upstream code changes,
			// we don't want to crash the server, so handle the unexpected error.
			return echo.NewHTTPError(http.StatusInternalServerError,
				fmt.Sprintf("error validating request: %s", err))
		}
	}

	opId := route.Operation.OperationID
	if opId == "" {
		return echo.NewHTTPError(http.StatusInternalServerError,
			"no operation ID set")
	}
	// CapitalizeTheOperationName
	opId = strings.ToUpper(opId[0:1]) + opId[1:]

	span, ok := tracer.SpanFromContext(req.Context())
	if ok {
		span.SetOperationName(opId)
		span.SetTag(ext.ResourceName, "oapi."+opId)
	}

	met := visibility.GetMetricsFromContext(req.Context())
	met.OpName = opId

	// We set the service fault counter immediately to 1
	// so if the next() function panics, we still record the fault.
	met.SetCount("Fault", 1)
	met.SetCount("Error", 1)
	met.SetCount("Success", 0)
	bench := met.Benchmark("Time")
	defer bench.Done()

	// Run the next handler in the chain
	err = r.next(ctx)

	met.SetCount("Fault", 0) // Defuse the fault count
	if err == nil {
		met.SetCount("Success", 1)
		met.SetCount("Error", 0) // Defuse the error count
	}

	return err
}
