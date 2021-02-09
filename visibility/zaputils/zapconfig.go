package zaputils

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"net"
	"net/url"
	"os"
	"sync"
	"time"
)

const TcpSinkCheckSec = 5
const TcpSinkConnTimeout = 100*time.Millisecond
var initMutex sync.Mutex
var initialized = false

type zapTcpSink struct {
	mtx sync.Mutex

	addr            string
	conn            net.Conn
	lastTimeChecked time.Time
	discard         []byte
}

func (t *zapTcpSink) Write(p []byte) (int, error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.conn == nil {
		t.connect()
	}
	if t.conn == nil {
		return len(p), nil
	}

	n, err := t.conn.Write(p)
	if err == nil {
		return n, nil
	}

	// Reset the connection and try one reconnect cycle
	t.conn = nil
	t.connect()
	if t.conn != nil {
		n, err = t.conn.Write(p)
		if err != nil {
			t.conn = nil
		}
	}

	// We always return success even if we discard the bytes
	// received while there's no connection.
	return len(p), nil
}

func (t *zapTcpSink) connect() {
	if time.Now().Sub(t.lastTimeChecked).Seconds() < TcpSinkCheckSec {
		return
	}

	conn, err := net.DialTimeout("tcp", t.addr, TcpSinkConnTimeout)
	if err == nil {
		t.lastTimeChecked = time.Time{}
		t.conn = conn
		return
	} else {
		t.lastTimeChecked = time.Now()
	}
}

func (t *zapTcpSink) Sync() error {
	return nil
}

func (t *zapTcpSink) Close() error {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if t.conn == nil {
		return nil
	}
	err := t.conn.Close()
	t.conn = nil
	return err
}

func ConfigureZapGlobals() {
	initMutex.Lock()
	defer initMutex.Unlock()
	if initialized {
		return
	}

	err := zap.RegisterSink("tcp", func(url *url.URL) (zap.Sink, error) {
		conn, err := net.Dial("tcp", url.Host)
		return &zapTcpSink{addr: url.Host, conn: conn,
			discard: make([]byte, 1024)}, err
	})
	if err != nil {
		panic(err.Error())
	}

	err = zap.RegisterEncoder("prettyconsole",
		func(config zapcore.EncoderConfig) (zapcore.Encoder, error) {
			ce := NewPrettyConsoleEncoder(config)
			return ce, nil
		})

	if err != nil {
		panic(err.Error())
	}

	initialized = true
}


func ConfigureDevLogger() *zap.Logger {
	ConfigureZapGlobals()

	config := zap.NewDevelopmentConfig()
	config.Encoding = "prettyconsole"
	config.DisableStacktrace = true
	checkTcpSink(&config)
	logger, err := config.Build(MakeFieldsUnique())
	if err != nil {
		panic(err.Error())
	}
	return logger
}

func ConfigureProdLogger() *zap.Logger {
	ConfigureZapGlobals()

	config := zap.NewProductionConfig()
	checkTcpSink(&config)
	logger, err := config.Build(MakeFieldsUnique())
	if err != nil {
		panic(err.Error())
	}
	return logger
}

func checkTcpSink(config *zap.Config) {
	tcpSink := os.Getenv("DD_TCP_SINK")
	if tcpSink != "" {
		config.OutputPaths = []string{"tcp://" + tcpSink, "stderr"}
		config.ErrorOutputPaths = []string{"tcp://" + tcpSink, "stderr"}
	}
}
