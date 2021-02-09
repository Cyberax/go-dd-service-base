package oapi

import (
	"github.com/labstack/echo/v4"
	"github.com/labstack/gommon/log"
	"go.uber.org/zap"
	"io"
)

type ZapLoggerWrapper struct {
	zLogger *zap.SugaredLogger
	output  io.Writer
}

// Create a new wrapper for an Echo logger, imbued with a contextualized
// Zap logger
func NewLoggerWrapper(zLogger *zap.Logger) *ZapLoggerWrapper {
	// The output is used only for colorer
	return &ZapLoggerWrapper{
		zLogger: zLogger.Sugar(),
		output:  zap.NewStdLog(zLogger).Writer(),
	}
}

func GetZapLoggerFromEchoLogger(logger echo.Logger) *zap.Logger {
	zlw := logger.(*ZapLoggerWrapper)
	return zlw.zLogger.Desugar()
}

func (l *ZapLoggerWrapper) Print(i ...interface{}) {
	l.zLogger.Info(i...)
}

func (l *ZapLoggerWrapper) Printf(format string, args ...interface{}) {
	l.zLogger.Infof(format, args...)
}

func (l *ZapLoggerWrapper) Printj(j log.JSON) {
	l.zLogger.Desugar().Info("Message", zap.Reflect("data", j))
}

func (l *ZapLoggerWrapper) Debug(i ...interface{}) {
	l.zLogger.Debug(i...)
}

func (l *ZapLoggerWrapper) Debugf(format string, args ...interface{}) {
	l.zLogger.Debugf(format, args...)
}

func (l *ZapLoggerWrapper) Debugj(j log.JSON) {
	l.zLogger.Desugar().Debug("Message", zap.Reflect("data", j))
}

func (l *ZapLoggerWrapper) Info(i ...interface{}) {
	l.zLogger.Info(i...)
}

func (l *ZapLoggerWrapper) Infof(format string, args ...interface{}) {
	l.zLogger.Infof(format, args...)
}

func (l *ZapLoggerWrapper) Infoj(j log.JSON) {
	l.zLogger.Desugar().Info("Message", zap.Reflect("data", j))
}

func (l *ZapLoggerWrapper) Warn(i ...interface{}) {
	l.zLogger.Warn(i...)
}

func (l *ZapLoggerWrapper) Warnf(format string, args ...interface{}) {
	l.zLogger.Warnf(format, args...)
}

func (l *ZapLoggerWrapper) Warnj(j log.JSON) {
	l.zLogger.Desugar().Warn("Message", zap.Reflect("data", j))
}

func (l *ZapLoggerWrapper) Error(i ...interface{}) {
	l.zLogger.Error(i...)
}

func (l *ZapLoggerWrapper) Errorf(format string, args ...interface{}) {
	l.zLogger.Errorf(format, args...)
}

func (l *ZapLoggerWrapper) Errorj(j log.JSON) {
	l.zLogger.Desugar().Error("Message", zap.Reflect("data", j))
}

func (l *ZapLoggerWrapper) Fatal(i ...interface{}) {
	l.zLogger.Fatal(i...)
}

func (l *ZapLoggerWrapper) Fatalj(j log.JSON) {
	l.zLogger.Desugar().Fatal("Message", zap.Reflect("data", j))
}

func (l *ZapLoggerWrapper) Fatalf(format string, args ...interface{}) {
	l.zLogger.Fatalf(format, args...)
}

func (l *ZapLoggerWrapper) Panic(i ...interface{}) {
	l.zLogger.Panic(i...)
}

func (l *ZapLoggerWrapper) Panicj(j log.JSON) {
	l.zLogger.Desugar().Panic("Message", zap.Reflect("data", j))
}

func (l *ZapLoggerWrapper) Panicf(format string, args ...interface{}) {
	l.zLogger.Panicf(format, args...)
}

func (l *ZapLoggerWrapper) SetHeader(h string) {
	panic("Method is not available")
}

func (l *ZapLoggerWrapper) Prefix() string {
	panic("Method is not available")
}

func (l *ZapLoggerWrapper) SetPrefix(s string) {
	panic("Method is not available")
}

func (l *ZapLoggerWrapper) Level() log.Lvl {
	panic("Method is not available")
}

func (l *ZapLoggerWrapper) SetLevel(lvl log.Lvl) {
	// No-op - called only to set up the debug level
}

func (l *ZapLoggerWrapper) Output() io.Writer {
	// The output is used only for colorer that prints the banner
	return l.output
}

func (l ZapLoggerWrapper) SetOutput(w io.Writer) {
	panic("Method is not available")
}
