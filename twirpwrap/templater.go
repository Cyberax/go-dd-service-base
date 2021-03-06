package main

import (
	pgs "github.com/lyft/protoc-gen-star"
	pgsgo "github.com/lyft/protoc-gen-star/lang/go"
	"text/template"
)

func FilePathFor(f pgs.File, ctx pgsgo.Context, tpl *template.Template) *pgs.FilePath {
	out := ctx.OutputPath(f)
	out = out.SetExt(".lv." + tpl.Name())
	return &out
}

const fileTpl = `// Code generated by protoc-gen-twirpwrap. DO NOT EDIT.
// source: {{ .InputPath }}
// Functionality: logging and validation wrapper for Twirp messages
package {{ pkg . }}
import (
	"context"
	"github.com/cyberax/go-dd-service-base/visibility"
	"github.com/golang/protobuf/proto"
	"github.com/twitchtv/twirp"
	"go.uber.org/zap"
)

type validationError interface {
    error
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
}

{{ range $service := .Services }}

{{$lvName := printf "%sLogValidate" $service.Name}}
type {{$lvName}} struct {
    Delegate {{$service.Name}}
    MaxLoggableMessage int
}

// Ensure that LogValidator implements the API
var _ {{$service.Name}} = &{{$lvName}}{}

func New{{$lvName}}(delegate {{$service.Name}}) *{{$lvName}} {
    return &{{$lvName}}{
        Delegate: delegate,
        MaxLoggableMessage: 8129,
    }
}

func (l *{{$lvName}}) handleInput(ctx context.Context, in proto.Message,
	method string) {

	inSize := proto.Size(in)
	if inSize <= l.MaxLoggableMessage {
		visibility.CL(ctx).Info("Twirp request",
			zap.String("service", "{{$service.Name}}"), zap.String("method", method),
			zap.Int("input_size", inSize), zap.Reflect("input", in))
	} else {
		visibility.CL(ctx).Info("Twirp request (too big to log)",
			zap.String("service", "{{$service.Name}}"), zap.String("method", method),
			zap.Int("input_size", inSize))
	}
}

func (l *{{$lvName}}) handleOutput(ctx context.Context,
	msg proto.Message, err error, method string) {

	if err != nil {
		fields := []zap.Field{
			zap.String("service", "{{$service.Name}}"),
			zap.String("method", method),
			zap.Error(err),
		}
		if twErr, ok := err.(twirp.Error); ok {
			stack := twErr.Meta(visibility.StackTraceKey)
			if stack != "" {
				fields = append(fields, zap.String("stacktrace", stack))
			}
		}
		visibility.CL(ctx).Info("Twirp failure", fields...)
		return
	}

	outSize := proto.Size(msg)
	if outSize <= l.MaxLoggableMessage {
		visibility.CL(ctx).Info("Twirp response",
			zap.String("service", "{{$service.Name}}"), zap.String("method", method),
			zap.Int("output_size", outSize), zap.Reflect("output", msg))
	} else {
		visibility.CL(ctx).Info("Twirp response (too big to log)",
			zap.String("service", "{{$service.Name}}"), zap.String("method", method),
			zap.Int("output_size", outSize))
	}
}

{{ range $method := $service.Methods }}
func (l *{{$lvName}}) {{$method.Name}}(ctx context.Context, in *{{$method.Input.Name}}) (
 	*{{$method.Output.Name}}, error){

	l.handleInput(ctx, in, "{{$method.Name}}")

    err := in.Validate()
	if vErr, ok := err.(validationError); ok {
		twErr := twirp.NewError(twirp.InvalidArgument, vErr.Error())
		twErr = twErr.WithMeta("argument", vErr.Field())
		l.handleOutput(ctx, nil, twErr, "{{$method.Name}}")
		return nil, twErr
	} else if err != nil {
		return nil, err
	}

    res, err := l.Delegate.{{$method.Name}}(ctx, in)
	if err == nil {
		err = res.Validate()
	}
	l.handleOutput(ctx, res, err, "{{$method.Name}}")

    return res, err
}

{{ end }}

{{ end }}
`
