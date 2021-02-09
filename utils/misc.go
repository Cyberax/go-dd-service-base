package utils

import (
	"bytes"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net"
	"strings"
	"time"
	"unicode"
)

// Time in milliseconds since the Unix epoch
type AbsoluteTimeSec int64

func (at AbsoluteTimeSec) ToTime() time.Time {
	return time.Unix(int64(at), 0)
}

func (at AbsoluteTimeSec) ToUnix() int64 {
	return int64(at)
}

func FromTimeSec(tm time.Time) AbsoluteTimeSec {
	return AbsoluteTimeSec(tm.Unix())
}

func StaticClock(sec int64) func() time.Time {
	return func() time.Time {
		return time.Unix(sec, 0)
	}
}

func Use(_ interface{}) {}

func MakeRandomStr(numBytes int) string {
	bytesSlice := make([]byte, numBytes)
	_, err := rand.Read(bytesSlice)
	PanicIfF(err != nil, "failed to read random numbers: %s", err)
	return hex.EncodeToString(bytesSlice)
}

func PanicIfF(cond bool, msg string, args ...interface{}) {
	if cond {
		panic(fmt.Sprintf(msg, args...))
	}
}

func PanicIfErr(err error) {
	if err != nil {
		panic(fmt.Sprintf("%v", err))
	}
}

// MemorySink implements zap.Sink by writing all messages to a buffer.
type MemorySink struct {
	bytes.Buffer
}
func (s *MemorySink) Close() error { return nil }
func (s *MemorySink) Sync() error  { return nil }

func NewMemorySinkLogger() (*MemorySink, *zap.Logger) {
	sink := &MemorySink{}
	config := zap.NewProductionEncoderConfig()
	config.TimeKey = ""
	core := zapcore.NewCore(zapcore.NewJSONEncoder(config), sink, zap.DebugLevel)
	logger := zap.New(core)
	return sink, logger
}


func GetFreeTcpPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func MustJsonIndent(obj interface{}, indent string) string {
	data, err := json.MarshalIndent(obj, "", indent)
	if err != nil {
		panic(err.Error())
	}
	return string(data)
}

func MustJson(obj interface{}) string {
	data, err := json.Marshal(obj)
	if err != nil {
		panic(err.Error())
	}
	return string(data)
}

func NullableStr(val interface{}) string {
	if val == nil {
		return ""
	}
	return val.(string)
}

func ToNullStr(val string) sql.NullString {
	return sql.NullString{String: val, Valid: val != ""}
}

func ToSqlI64(val *int64) sql.NullInt64 {
	if val == nil {
		return sql.NullInt64{Valid: false}
	}
	return sql.NullInt64{Int64: *val, Valid: true}
}

// ToSnakeCase convert the given string to snake case following the Golang format:
// acronyms are converted to lower-case and preceded by an underscore.
func ToSnakeCase(in string, delim rune) string {
	runes := []rune(in)

	letterAndLower := func(i int) bool {return unicode.IsLetter(runes[i]) && unicode.IsLower(runes[i])}
	letterAndUpper := func(i int) bool {return unicode.IsLetter(runes[i]) && unicode.IsUpper(runes[i])}
	var out []rune
	for i := 0; i < len(runes); i++ {
		if i > 0 && (letterAndUpper(i) || unicode.IsNumber(runes[i])) &&
			((i+1 < len(runes) && letterAndLower(i+1)) || letterAndLower(i-1)) &&
			(unicode.IsLetter(runes[i-1]) || unicode.IsNumber(runes[i-1])){
			out = append(out, delim)
		}
		if runes[i] == '_' || runes[i] == '-' {
			out = append(out, delim)
		} else {
			out = append(out, unicode.ToLower(runes[i]))
		}
	}

	res := string(out)
	res = strings.ReplaceAll(res, ".", string(delim))
	return res
}
