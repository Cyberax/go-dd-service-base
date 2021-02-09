package zaputils

import (
	"encoding/json"
	"fmt"
	"github.com/Cyberax/go-dd-service-base/utils"
	"github.com/Cyberax/go-dd-service-base/visibility"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"strings"
	"time"
)

type prettyConsoleEncoder struct {
	zapcore.Encoder
	cfg zapcore.EncoderConfig
}

var pool = buffer.NewPool()

// NewConsoleEncoder creates an encoder whose output is designed for human -
// rather than machine - consumption. It serializes the core log entry data
// (message, level, timestamp, etc.) in a plain-text format and leaves the
// structured context as pretty-printed multiline JSON.
//
// Additional functionality includes easily-readable stack traces.
//
// Note that while pretty-printing is useful in development, it's bad for production
func NewPrettyConsoleEncoder(cfg zapcore.EncoderConfig) zapcore.Encoder {
	// Use empty config because we don't care about encoding informational
	// fields, we only want to use it to encode extra fields.
	encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
		EncodeLevel:    cfg.EncodeLevel,
		EncodeTime:     cfg.EncodeTime,
		EncodeDuration: cfg.EncodeDuration,
		EncodeCaller:   cfg.EncodeCaller,
		EncodeName:     cfg.EncodeName,
	})
	return &prettyConsoleEncoder{cfg: cfg, Encoder: encoder}
}

func (c *prettyConsoleEncoder) Clone() zapcore.Encoder {
	return &prettyConsoleEncoder{cfg: c.cfg, Encoder: c.Encoder.Clone()}
}

func (c *prettyConsoleEncoder) EncodeEntry(ent zapcore.Entry,
	fields []zapcore.Field) (*buffer.Buffer, error) {
	line := pool.Get()

	// We don't want the entry's metadata to be quoted and escaped (if it's
	// encoded as strings), which means that we can't use the JSON encoder. The
	// simplest option is to use the memory encoder and fmt.Fprint.
	//
	// If this ever becomes a performance bottleneck, we can implement
	// ArrayEncoder for our plain-text format.
	arr := &sliceArrayEncoder{}
	if c.cfg.TimeKey != "" && c.cfg.EncodeTime != nil {
		c.cfg.EncodeTime(ent.Time, arr)
	}
	if c.cfg.LevelKey != "" && c.cfg.EncodeLevel != nil {
		c.cfg.EncodeLevel(ent.Level, arr)
	}
	if ent.LoggerName != "" && c.cfg.NameKey != "" {
		nameEncoder := c.cfg.EncodeName

		if nameEncoder == nil {
			// Fall back to FullNameEncoder for backward compatibility.
			nameEncoder = zapcore.FullNameEncoder
		}
		nameEncoder(ent.LoggerName, arr)
	}
	if ent.Caller.Defined && c.cfg.CallerKey != "" && c.cfg.EncodeCaller != nil {
		c.cfg.EncodeCaller(ent.Caller, arr)
	}
	for i := range arr.elems {
		if i > 0 {
			line.AppendByte('\t')
		}
		_, _ = fmt.Fprint(line, arr.elems[i])
	}

	// Add the message itself.
	if c.cfg.MessageKey != "" {
		c.addTabIfNecessary(line)
		line.AppendString(ent.Message)
	}

	// Add any structured context.
	c.writeContext(line, fields)

	// If there's no stacktrace key, honor that; this allows users to force
	// single-line output.
	if ent.Stack != "" && c.cfg.StacktraceKey != "" {
		line.AppendByte('\n')
		line.AppendString(ent.Stack)
	}

	if c.cfg.LineEnding != "" {
		line.AppendString(c.cfg.LineEnding)
	} else {
		line.AppendString(zapcore.DefaultLineEnding)
	}
	return line, nil
}

func (c *prettyConsoleEncoder) writeContext(line *buffer.Buffer, extra []zapcore.Field) {
	context := c.Encoder.Clone()
	ent := zapcore.Entry{
		Level:      0,
		Time:       time.Time{},
		LoggerName: "",
		Message:    "",
		Caller:     zapcore.EntryCaller{},
		Stack:      "",
	}
	buf, _ := context.EncodeEntry(ent, extra)
	if buf == nil {
		return
	}
	defer buf.Free()

	c.addTabIfNecessary(line)

	// Make sure we always have something to write
	defer line.TrimNewline()
	fieldsToPrint := []byte(strings.TrimRight(buf.String(), "\r\n"))
	cleanuper := utils.NewCleanup(func() {
		_, _ = line.Write(fieldsToPrint)
	})
	defer cleanuper.Cleanup()

	var fieldsData map[string]interface{}
	err := json.Unmarshal(fieldsToPrint, &fieldsData)
	if err != nil {
		return
	}

	stack, hasStack := c.tryGetStack(fieldsData)
	if !hasStack {
		return
	}

	// Remove the stack trace data
	delete(fieldsData, "stacktrace")
	// Format the rest of the fields
	withoutStack, err := json.Marshal(fieldsData)
	if err != nil {
		return
	}

	cleanuper.Disarm()
	_, _ = line.Write(withoutStack)
	if hasStack {
		_, _ = line.Write([]byte(stack))
	}
}

func (c *prettyConsoleEncoder) tryGetStack(fieldsData map[string]interface{}) (string, bool) {
	panicText, hasPanic := fieldsData["panic"]

	// Print the stack trace in a nice and separated way
	stack, ok := fieldsData["stacktrace"]
	if !ok {
		return "", false
	}

	if stackStr, ok := stack.(string); ok {
		// Add a "\t" prefix to each line
		stackStr = strings.TrimSpace(stackStr)
		stackStr = strings.Join(strings.Split(stackStr, "\n"), "\n\t")
		if hasPanic {
			return fmt.Sprintf("\nPanic: %v\n\t%s", panicText, stackStr), true
		}
		return "\n\t" + stackStr, true
	}

	// Try to transmute the stack back into StackElements
	data, err := json.Marshal(stack)
	if err != nil {
		return "", false
	}

	var transmutedStack []visibility.StackElement
	err = json.Unmarshal(data, &transmutedStack)
	if err != nil {
		return "", false
	}

	stackStr := "\n"
	if hasPanic {
		stackStr = fmt.Sprintf("\nPanic: %v\n", panicText)
	}

	for _, s := range transmutedStack {
		stackStr += fmt.Sprintf("\t%s %s\n", s.Fl, s.Fn)
	}

	return stackStr, true
}

func (c *prettyConsoleEncoder) addTabIfNecessary(line *buffer.Buffer) {
	if line.Len() > 0 {
		line.AppendByte('\t')
	}
}

// sliceArrayEncoder is an ArrayEncoder backed by a simple []interface{}. Like
// the MapObjectEncoder, it's not designed for production use.
type sliceArrayEncoder struct {
	elems []interface{}
}

func (s *sliceArrayEncoder) AppendArray(v zapcore.ArrayMarshaler) error {
	enc := &sliceArrayEncoder{}
	err := v.MarshalLogArray(enc)
	s.elems = append(s.elems, enc.elems)
	return err
}

func (s *sliceArrayEncoder) AppendObject(v zapcore.ObjectMarshaler) error {
	m := zapcore.NewMapObjectEncoder()
	err := v.MarshalLogObject(m)
	s.elems = append(s.elems, m.Fields)
	return err
}

func (s *sliceArrayEncoder) AppendReflected(v interface{}) error {
	s.elems = append(s.elems, v)
	return nil
}

func (s *sliceArrayEncoder) AppendBool(v bool)              { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendByteString(v []byte)      { s.elems = append(s.elems, string(v)) }
func (s *sliceArrayEncoder) AppendComplex128(v complex128)  { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendComplex64(v complex64)    { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendDuration(v time.Duration) { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendFloat64(v float64)        { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendFloat32(v float32)        { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendInt(v int)                { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendInt64(v int64)            { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendInt32(v int32)            { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendInt16(v int16)            { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendInt8(v int8)              { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendString(v string)          { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendTime(v time.Time)         { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendUint(v uint)              { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendUint64(v uint64)          { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendUint32(v uint32)          { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendUint16(v uint16)          { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendUint8(v uint8)            { s.elems = append(s.elems, v) }
func (s *sliceArrayEncoder) AppendUintptr(v uintptr)        { s.elems = append(s.elems, v) }
