package main

import (
	"github.com/lyft/protoc-gen-star"
	pgsgo "github.com/lyft/protoc-gen-star/lang/go"
)

func main() {
	pgs.Init(pgs.DebugEnv("DEBUG_PGV")).
		RegisterModule(Validator()).
		RegisterPostProcessor(pgsgo.GoFmt()).
		Render()
}
