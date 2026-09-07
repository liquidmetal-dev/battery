package flintlockclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
)

// ErrExecFailed is returned by Exec when the server sends an Error message
// on the exec stream (e.g. the guest-agent is unreachable, or the VM isn't
// in a state that allows exec), as opposed to a non-zero exit code, which
// is a normal command result and not a Go error.
var ErrExecFailed = errors.New("flintlockclient: exec failed")

// maxProbeTimeout bounds how long a single WaitReady probe attempt is
// allowed to run, so one hung attempt can't consume the entire caller
// deadline without any retries happening.
const maxProbeTimeout = 5 * time.Second

// ExecResult is the outcome of a single command run via
// MicroVMExec.ExecCommand.
type ExecResult struct {
	Stdout   []byte
	Stderr   []byte
	ExitCode int32
}

// ExecOptions configures a single Exec invocation.
type ExecOptions struct {
	Cwd string
	Env map[string]string
	// User, if set, runs the command as that guest system user.
	User string
	// TimeoutSeconds bounds the command's run time on the server side; 0
	// means no server-side timeout.
	TimeoutSeconds int32
}

// Exec runs cmd as a shell command (via ExecStart.Shell) on the microvm
// identified by uid, using client, and blocks until the command completes
// or ctx is done. It has no stdin: hook commands are fire-and-forget shell
// strings, not interactive sessions.
//
// A non-zero exit code is not a Go error: it's returned via
// ExecResult.ExitCode, matching normal shell semantics. Exec returns an
// error only for a transport/RPC failure, or for a server-sent Error
// message (wrapped as ErrExecFailed).
func Exec(ctx context.Context, client microvmexecv1alpha1.MicroVMExecClient, uid, cmd string, opts ExecOptions) (*ExecResult, error) {
	stream, err := client.ExecCommand(ctx)
	if err != nil {
		return nil, fmt.Errorf("flintlockclient: exec %s: open stream: %w", uid, err)
	}

	start := &microvmexecv1alpha1.ExecCommandRequest{
		Payload: &microvmexecv1alpha1.ExecCommandRequest_Start{
			Start: &microvmexecv1alpha1.ExecStart{
				Uid:            uid,
				Cmd:            cmd,
				Shell:          true,
				Cwd:            opts.Cwd,
				Env:            opts.Env,
				User:           opts.User,
				TimeoutSeconds: opts.TimeoutSeconds,
				HasStdin:       false,
			},
		},
	}

	if err := stream.Send(start); err != nil {
		return nil, fmt.Errorf("flintlockclient: exec %s: send start: %w", uid, err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("flintlockclient: exec %s: close send: %w", uid, err)
	}

	result := &ExecResult{}
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("flintlockclient: exec %s: stream closed before exit code", uid)
		}
		if err != nil {
			return nil, fmt.Errorf("flintlockclient: exec %s: %w", uid, err)
		}

		switch payload := resp.GetPayload().(type) {
		case *microvmexecv1alpha1.ExecCommandResponse_Stdout:
			result.Stdout = append(result.Stdout, payload.Stdout...)
		case *microvmexecv1alpha1.ExecCommandResponse_Stderr:
			result.Stderr = append(result.Stderr, payload.Stderr...)
		case *microvmexecv1alpha1.ExecCommandResponse_Error:
			return nil, fmt.Errorf("%w: %s", ErrExecFailed, payload.Error)
		case *microvmexecv1alpha1.ExecCommandResponse_ExitCode:
			result.ExitCode = payload.ExitCode
			return result, nil
		}
	}
}

// WaitReady retries a trivial no-op exec against the microvm identified by
// uid, using client, until one succeeds or ctx is done. It returns nil as
// soon as any exec reaches a terminal response (regardless of the no-op
// command's own exit code — reaching a terminal response at all means the
// guest-agent path is up), or a wrapped error once ctx expires.
//
// There is no dedicated readiness/ping RPC, and flintlock's own exec-server
// behavior gives no reliable way to distinguish "guest-agent not up yet"
// from a permanent failure (e.g. the VM was never allowed guest-agent
// access) by error type alone. WaitReady therefore retries on every error,
// including ErrExecFailed: a permanently-broken VM will spin until ctx's
// deadline rather than fail fast. Callers must bound ctx sensibly.
//
// Each probe attempt gets its own sub-timeout (capped at 5s) derived from
// ctx, so a single hung attempt can't consume the whole deadline without
// any retries happening.
func WaitReady(ctx context.Context, client microvmexecv1alpha1.MicroVMExecClient, uid string, interval time.Duration) error {
	probeTimeout := interval
	if probeTimeout > maxProbeTimeout {
		probeTimeout = maxProbeTimeout
	}

	var lastErr error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		_, err := Exec(attemptCtx, client, uid, "true", ExecOptions{})
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return fmt.Errorf("flintlockclient: WaitReady %s: timed out, last error: %w", uid, lastErr)
		case <-time.After(interval):
		}
	}
}
