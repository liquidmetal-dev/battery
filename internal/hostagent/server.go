// Package hostagent implements the poolmgr-hostagent gRPC server: a per-flintlock-host sidecar
// that proxies guest-agent vsock calls (via the vsock-connect CLI) on behalf of the pool manager.
package hostagent

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/vsockconnect"
)

// defaultPingInterval is how often WaitReady retries a failed ping while waiting for the
// guest-agent to come up.
const defaultPingInterval = 200 * time.Millisecond

// Server implements poolmgrv1alpha1.HostagentServer.
type Server struct {
	poolmgrv1alpha1.UnimplementedHostagentServer

	runner       vsockconnect.Runner
	port         int
	pingInterval time.Duration
}

// NewServer returns a Server that reaches guest-agents via runner on the given vsock port,
// retrying WaitReady pings at pingInterval.
func NewServer(runner vsockconnect.Runner, port int, pingInterval time.Duration) *Server {
	if pingInterval <= 0 {
		pingInterval = defaultPingInterval
	}
	return &Server{runner: runner, port: port, pingInterval: pingInterval}
}

// WaitReady blocks until the guest-agent at req.VsockPath responds to a ping, or returns
// codes.DeadlineExceeded once req.Timeout elapses.
func (s *Server) WaitReady(ctx context.Context, req *poolmgrv1alpha1.WaitReadyRequest) (*poolmgrv1alpha1.WaitReadyResponse, error) {
	if req.GetVsockPath() == "" {
		return nil, status.Error(codes.InvalidArgument, "vsock_path is required")
	}

	if timeout := req.GetTimeout().AsDuration(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()

	for {
		if err := s.runner.Ping(ctx, req.GetVsockPath(), s.port); err == nil {
			return &poolmgrv1alpha1.WaitReadyResponse{}, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctxDoneError(ctx, "timed out waiting for guest-agent to become ready")
		case <-ticker.C:
		}
	}
}

// Run executes req.Cmd via the guest-agent's exec, streaming stdout/stderr chunks back as
// they're produced and finishing with an exit-code message once the command completes, or
// returning codes.DeadlineExceeded once req.Timeout elapses.
func (s *Server) Run(req *poolmgrv1alpha1.RunRequest, stream poolmgrv1alpha1.Hostagent_RunServer) error {
	if req.GetVsockPath() == "" {
		return status.Error(codes.InvalidArgument, "vsock_path is required")
	}
	if len(req.GetCmd()) == 0 {
		return status.Error(codes.InvalidArgument, "cmd is required")
	}

	ctx := stream.Context()
	if timeout := req.GetTimeout().AsDuration(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	execution, err := s.runner.Exec(ctx, req.GetVsockPath(), s.port, req.GetCmd())
	if err != nil {
		return status.Errorf(codes.Unavailable, "starting command: %v", err)
	}

	// chunks is drained by a single goroutine below, so stream.Send is only ever called from one
	// place at a time: gRPC server streams aren't safe for concurrent Send calls.
	chunks := make(chan *poolmgrv1alpha1.RunResponse)
	streamErr := make(chan error, 1)
	go func() {
		for chunk := range chunks {
			if err := stream.Send(chunk); err != nil {
				select {
				case streamErr <- err:
				default:
				}
			}
		}
		close(streamErr)
	}()

	stdoutErr := make(chan error, 1)
	go func() { stdoutErr <- readChunks(chunks, execution.Stdout(), wrapStdout) }()
	stderrErr := make(chan error, 1)
	go func() { stderrErr <- readChunks(chunks, execution.Stderr(), wrapStderr) }()

	readErr := <-stdoutErr
	if err := <-stderrErr; readErr == nil {
		readErr = err
	}
	close(chunks)
	sendErr := <-streamErr

	if readErr != nil {
		_, _ = execution.Wait() // best-effort: reap the process even though we're erroring out
		return status.Errorf(codes.Unavailable, "reading command output: %v", readErr)
	}
	if sendErr != nil {
		_, _ = execution.Wait() // best-effort: reap the process even though we're erroring out
		return status.Errorf(codes.Unavailable, "streaming command output: %v", sendErr)
	}

	waitDone := make(chan struct{})
	var exitCode int
	var waitErr error
	go func() {
		exitCode, waitErr = execution.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-ctx.Done():
	}

	if ctx.Err() != nil {
		return ctxDoneError(ctx, "timed out waiting for command to complete")
	}

	if waitErr != nil {
		return status.Errorf(codes.Internal, "command failed: %v", waitErr)
	}

	return stream.Send(&poolmgrv1alpha1.RunResponse{
		Output: &poolmgrv1alpha1.RunResponse_ExitCode{ExitCode: int32(exitCode)},
	})
}

// ctxDoneError maps a context that has already fired to the corresponding gRPC status: a client
// cancellation becomes codes.Canceled, everything else (i.e. a timeout) becomes
// codes.DeadlineExceeded.
func ctxDoneError(ctx context.Context, deadlineMsg string) error {
	if errors.Is(ctx.Err(), context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	return status.Error(codes.DeadlineExceeded, deadlineMsg)
}

func wrapStdout(chunk []byte) *poolmgrv1alpha1.RunResponse {
	return &poolmgrv1alpha1.RunResponse{Output: &poolmgrv1alpha1.RunResponse_StdoutChunk{StdoutChunk: chunk}}
}

func wrapStderr(chunk []byte) *poolmgrv1alpha1.RunResponse {
	return &poolmgrv1alpha1.RunResponse{Output: &poolmgrv1alpha1.RunResponse_StderrChunk{StderrChunk: chunk}}
}

// readChunks reads from r until EOF, sending each non-empty read to chunks wrapped by wrap.
func readChunks(chunks chan<- *poolmgrv1alpha1.RunResponse, r io.Reader, wrap func([]byte) *poolmgrv1alpha1.RunResponse) error {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			chunks <- wrap(chunk)
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
