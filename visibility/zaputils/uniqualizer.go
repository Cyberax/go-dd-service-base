package zaputils

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// This is a wrapper core that makes sure that pre-specified fields are unique
type uniqueFieldsCore struct{
	root    zapcore.Core
	current zapcore.Core
	fields  []zapcore.Field
}

func MakeFieldsUnique() zap.Option {
	return zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return &uniqueFieldsCore{
			root: core,
			current: core,
		}
	})
}

func (u uniqueFieldsCore) Enabled(level zapcore.Level) bool {
	return u.current.Enabled(level)
}

func (u uniqueFieldsCore) With(newFields []zapcore.Field) zapcore.Core {
	// Copy fields
	newFieldList := make([]zapcore.Field, 0, len(u.fields)+len(newFields))

	outer: for _, f := range u.fields {
		// Skip all the existing fields with the names that match one
		// of the new fields.
		for _, k := range newFields {
			if f.Key == k.Key {
				continue outer
			}
		}
		newFieldList = append(newFieldList, f)
	}
	newFieldList = append(newFieldList, newFields...)

	return &uniqueFieldsCore{
		root: u.root,
		current: u.root.With(newFieldList),
		fields: newFieldList,
	}
}

func (u uniqueFieldsCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return u.current.Check(entry, checked)
}

func (u uniqueFieldsCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return u.current.Write(entry, fields)
}

func (u uniqueFieldsCore) Sync() error {
	return u.current.Sync()
}
