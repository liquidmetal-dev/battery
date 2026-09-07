// Package hostagent implements the poolmgr-hostagent gRPC server: a per-flintlock-host sidecar
// that proxies guest-agent vsock calls (via the vsock-connect CLI) on behalf of the pool manager.
package hostagent

import (
	"context"
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
			return nil, status.Error(codes.DeadlineExceeded, "timed out waiting for guest-agent to become ready")
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

	stdoutErr := make(chan error, 1)
	go func() { stdoutErr <- streamChunks(stream, execution.Stdout(), wrapStdout) }()
	stderrErr := make(chan error, 1)
	go func() { stderrErr <- streamChunks(stream, execution.Stderr(), wrapStderr) }()

	if err := <-stdoutErr; err != nil {
		return status.Errorf(codes.Unavailable, "streaming stdout: %v", err)
	}
	if err := <-stderrErr; err != nil {
		return status.Errorf(codes.Unavailable, "streaming stderr: %v", err)
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
		return status.Error(codes.DeadlineExceeded, "timed out waiting for command to complete")
	}

	if waitErr != nil {
		return status.Errorf(codes.Internal, "command failed: %v", waitErr)
	}

	return stream.Send(&poolmgrv1alpha1.RunResponse{
		Output: &poolmgrv1alpha1.RunResponse_ExitCode{ExitCode: int32(exitCode)},
	})
}

func wrapStdout(chunk []byte) *poolmgrv1alpha1.RunResponse {
	return &poolmgrv1alpha1.RunResponse{Output: &poolmgrv1alpha1.RunResponse_StdoutChunk{StdoutChunk: chunk}}
}

func wrapStderr(chunk []byte) *poolmgrv1alpha1.RunResponse {
	return &poolmgrv1alpha1.RunResponse{Output: &poolmgrv1alpha1.RunResponse_StderrChunk{StderrChunk: chunk}}
}

// streamChunks reads from r until EOF, sending each non-empty read to stream wrapped by wrap.
func streamChunks(stream poolmgrv1alpha1.Hostagent_RunServer, r io.Reader, wrap func([]byte) *poolmgrv1alpha1.RunResponse) error {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			if sendErr := stream.Send(wrap(chunk)); sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}
