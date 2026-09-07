package hostagent_test

import (
	"context"
	"sync"

	"github.com/liquidmetal-dev/battery/internal/vsockconnect"
)

// fakeRunner is an in-memory vsockconnect.Runner double for fast, subprocess-free server tests.
type fakeRunner struct {
	mu sync.Mutex

	// pingErrs is consumed in order across successive Ping calls; the last entry repeats once
	// exhausted. A nil entry means success.
	pingErrs  []error
	pingCalls []pingCall

	execFn func(ctx context.Context, vsockPath string, port int, cmd []string) (vsockconnect.Execution, error)
}

type pingCall struct {
	vsockPath string
	port      int
}

func (f *fakeRunner) Ping(_ context.Context, vsockPath string, port int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pingCalls = append(f.pingCalls, pingCall{vsockPath: vsockPath, port: port})

	if len(f.pingErrs) == 0 {
		return nil
	}

	idx := len(f.pingCalls) - 1
	if idx >= len(f.pingErrs) {
		idx = len(f.pingErrs) - 1
	}
	return f.pingErrs[idx]
}

func (f *fakeRunner) Exec(ctx context.Context, vsockPath string, port int, cmd []string) (vsockconnect.Execution, error) {
	return f.execFn(ctx, vsockPath, port, cmd)
}

func (f *fakeRunner) pingCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pingCalls)
}
