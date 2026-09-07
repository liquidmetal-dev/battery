package hostagent_test

import (
	"errors"
	"io"
	"sync/atomic"
)

// fakeExecution is an in-memory vsockconnect.Execution double for server tests.
type fakeExecution struct {
	stdout   io.Reader
	stderr   io.Reader
	exitCode int
	waitErr  error
	// waitFn, when set, overrides exitCode/waitErr and lets a test block Wait().
	waitFn func() (int, error)

	waitCalls atomic.Int32
}

func (f *fakeExecution) Stdout() io.Reader { return f.stdout }
func (f *fakeExecution) Stderr() io.Reader { return f.stderr }

func (f *fakeExecution) Wait() (int, error) {
	f.waitCalls.Add(1)
	if f.waitFn != nil {
		return f.waitFn()
	}
	return f.exitCode, f.waitErr
}

func (f *fakeExecution) waitCallCount() int32 { return f.waitCalls.Load() }

// errReader is an io.Reader that always fails, used to simulate a broken stdout/stderr pipe.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }
