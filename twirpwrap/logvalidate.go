package main

import (
	pgs "github.com/lyft/protoc-gen-star"
	pgsgo "github.com/lyft/protoc-gen-star/lang/go"
	"text/template"
)

const (
	logvalidateName = "logvalidate"
)

type Module struct {
	*pgs.ModuleBase
	ctx pgsgo.Context
}

var _ pgs.Module = (*Module)(nil)

func Validator() pgs.Module { return &Module{ModuleBase: &pgs.ModuleBase{}} }

func (m *Module) InitContext(ctx pgs.BuildContext) {
	m.ModuleBase.InitContext(ctx)
	m.ctx = pgsgo.InitContext(ctx.Parameters())
}

func (m *Module) Name() string { return logvalidateName }

func (m *Module) Execute(targets map[string]pgs.File, _ map[string]pgs.Package) []pgs.Artifact {

	tpl := template.New("go")

	fns := pgsgo.InitContext(m.Parameters())
	tpl.Funcs(map[string]interface{}{
		"cmt":           pgs.C80,
		"name":          fns.Name,
		"pkg":           fns.PackageName,
		"typ":           fns.Type,
	})

	template.Must(tpl.Parse(fileTpl))

	for _, f := range targets {
		m.Push(f.Name().String())

		out := FilePathFor(f, m.ctx, tpl)
		if out != nil {
			m.AddGeneratorTemplateFile(out.String(), tpl, f)
		}
		m.Pop()
	}

	return m.Artifacts()
}
