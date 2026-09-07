package hostagent_test

import "io"

// fakeExecution is an in-memory vsockconnect.Execution double for server tests.
type fakeExecution struct {
	stdout   io.Reader
	stderr   io.Reader
	exitCode int
	waitErr  error
	// waitFn, when set, overrides exitCode/waitErr and lets a test block Wait().
	waitFn func() (int, error)
}

func (f *fakeExecution) Stdout() io.Reader { return f.stdout }
func (f *fakeExecution) Stderr() io.Reader { return f.stderr }

func (f *fakeExecution) Wait() (int, error) {
	if f.waitFn != nil {
		return f.waitFn()
	}
	return f.exitCode, f.waitErr
}
