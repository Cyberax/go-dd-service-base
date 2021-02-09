package visibility

import (
	"context"
	"go.uber.org/zap"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ProcessRegistry struct {
	mtx        sync.Mutex
	numRunning uint64

	rootCtx context.Context
	cancel  context.CancelFunc

	processes     map[string]*ProcessContext
	runningGroups sync.WaitGroup
}

type ProcessContext struct {
	Parent *ProcessRegistry
	Name   string
	Done   chan struct{}
}

func NewProcessRegistry(parentCtx context.Context) *ProcessRegistry {
	ctx, cancel := context.WithCancel(parentCtx)
	p := &ProcessRegistry{
		rootCtx:   ctx,
		cancel:    cancel,
		processes: make(map[string]*ProcessContext),
	}
	return p
}

func (p *ProcessRegistry) Close() {
	CL(p.rootCtx).Sugar().Infof(
		"Closing the process registry with %d processes running: %s",
		atomic.LoadUint64(&p.numRunning), p.LogRunning())
	p.cancel()
	p.runningGroups.Wait()
	CL(p.rootCtx).Info("Finished waiting for processes to finish")
}

func (p *ProcessRegistry) LogRunning() string {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	var elems []string
	for k := range p.processes {
		elems = append(elems, k)
	}
	sort.Strings(elems)

	return strings.Join(elems, ", ")
}

func (p *ProcessRegistry) HasProcess(name string) bool {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	_, has := p.processes[name]
	return has
}

func (p *ProcessRegistry) CreateProcessContext(name string) ProcessContext {
	return ProcessContext{
		Parent: p,
		Name:   name,
		Done:   make(chan struct{}),
	}
}

func (pc *ProcessContext) prepareRun() bool {
	p := pc.Parent
	p.mtx.Lock()
	defer p.mtx.Unlock()

	_, has := p.processes[pc.Name]
	if has {
		return false
	}

	p.processes[pc.Name] = pc
	atomic.AddUint64(&p.numRunning, 1)
	p.runningGroups.Add(1)

	return true
}

func (pc *ProcessContext) Run(proc func(ctx context.Context) error) {
	res := pc.TryRun(proc)
	if !res {
		panic("There's already a process named: " + pc.Name)
	}
}

func (pc *ProcessContext) TryRun(proc func(ctx context.Context) error) bool {
	res := pc.prepareRun()
	if !res {
		return false
	}

	go func() {
		defer close(pc.Done)
		defer pc.Parent.markDone(pc.Name)

		// Run the process with XRay instrumentation
		_ = RunInstrumented(pc.Parent.rootCtx, pc.Name, func(xc context.Context) error {
				err := proc(xc)
				if err != nil {
					CL(xc).Error("Async process returned an error", zap.Error(err))
				}
				return err
			})
	}()

	return true
}

func (p *ProcessRegistry) markDone(s string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	delete(p.processes, s)
	atomic.AddUint64(&p.numRunning, ^uint64(0))
	p.runningGroups.Done()
}

func (pc *ProcessContext) RunPeriodicProcess(period time.Duration,
	proc func(ctx context.Context) error) {

	pc.prepareRun()

	go func() {
		defer close(pc.Done)
		defer pc.Parent.markDone(pc.Name)

		ticker := time.NewTicker(period)
		defer ticker.Stop()

	loop:
		for {
			// Run the process with tracing instrumentation
			_ = RunInstrumented(pc.Parent.rootCtx, pc.Name, func(xc context.Context) error {
					err := proc(xc)
					if err != nil {
						CL(xc).Error("Async process returned an error", zap.Error(err))
					}
					return err
				})

			select {
			case <-ticker.C:
			case <-pc.Parent.rootCtx.Done():
				break loop
			}
		}
	}()
}

func (pc *ProcessContext) Wait() {
	<-pc.Done
}

func (p *ProcessRegistry) GetWaitChannel(processName string) <-chan struct{} {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	proc := p.processes[processName]
	if proc == nil {
		ch := make(chan struct{})
		close(ch)
		return ch
	}

	return proc.Done
}
