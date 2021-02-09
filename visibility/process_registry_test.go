package visibility

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"sync"
	"testing"
	"time"
)

func TestProcessRegistry(t *testing.T) {
	ctx := context.Background()
	ctx = ImbueContext(ctx, zap.NewNop())
	reg := NewProcessRegistry(ctx)

	// Non-existing finishes are fine
	<-reg.GetWaitChannel("procName")

	wg := sync.WaitGroup{}
	wg.Add(1)
	pc := reg.CreateProcessContext("proc1")
	pc.Run(func(ctx context.Context) error {
		<- ctx.Done()
		wg.Done()
		return nil
	})
	assert.True(t, reg.HasProcess("proc1"))

	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	p2Done := make(chan bool)
	p2c := reg.CreateProcessContext("proc2")
	p2c.Run(func(ctx context.Context) error {
		<- p2Done
		wg2.Done()
		return nil
	})

	select {
	case <-reg.GetWaitChannel("proc2"):
		assert.Fail(t, "Process is unexpectedly dead")
	default:
	}

	assert.Equal(t, "proc1, proc2", reg.LogRunning())
	close(p2Done)
	wg2.Wait()
	// The process is done, the finish channel is closed
	<-reg.GetWaitChannel("proc2")

	for ;; {
		if reg.LogRunning() == "proc1" {
			break
		}
		time.Sleep(100*time.Millisecond)
	}

	reg.Close()
	wg.Wait()
	assert.Equal(t, "", reg.LogRunning())
}

func TestNoDups(t *testing.T) {
	ctx := context.Background()
	ctx = ImbueContext(ctx, zap.NewNop())
	reg := NewProcessRegistry(ctx)

	p := reg.CreateProcessContext("proc1")
	p.Run(func(ctx context.Context) error {return nil})
	assert.Panics(t, func() {
		p.Run(func(ctx context.Context) error {return nil})
	})
}

func TestPeriodic(t *testing.T) {
	ctx := context.Background()
	ctx = ImbueContext(ctx, zap.NewNop())
	reg := NewProcessRegistry(ctx)

	progressChan := make(chan bool)

	pc := reg.CreateProcessContext("proc1")
	pc.RunPeriodicProcess(10*time.Millisecond, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
		case progressChan <- true:
		}
		return nil
	})

	<-progressChan
	<-progressChan

	reg.Close()
	pc.Wait()
}

func TestProcessRegistryInstrumentation(t *testing.T) {
	ctx := context.Background()
	ctx = ImbueContext(ctx, zap.NewNop())
	reg := NewProcessRegistry(ctx)

	p := reg.CreateProcessContext("Proc1")
	good := false
	p.Run(func(ctx context.Context) error {
		// Check that the logger context is there
		CL(ctx)
		CLS(ctx)

		// Check for the segment
		_, ok := tracer.SpanFromContext(ctx)
		if !ok {
			return nil
		}

		good = true
		return nil
	})

	reg.Close()
	assert.True(t, good)
}
