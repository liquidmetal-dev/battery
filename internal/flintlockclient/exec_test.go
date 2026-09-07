package flintlockclient_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
)

// fakeMicroVMExecServer is a minimal flintlock MicroVMExec service used to
// exercise Exec/WaitReady without a real flintlock/guest-agent.
//
// respond is called once per ExecCommand stream after the start message is
// received; it drives what the fake sends back (or the error it returns).
type fakeMicroVMExecServer struct {
	microvmexecv1alpha1.UnimplementedMicroVMExecServer
	respond func(start *microvmexecv1alpha1.ExecStart) (stdout, stderr []byte, exitCode int32, execErr string, streamErr error)
}

func (f *fakeMicroVMExecServer) ExecCommand(stream microvmexecv1alpha1.MicroVMExec_ExecCommandServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	start := req.GetStart()

	stdout, stderr, exitCode, execErr, streamErr := f.respond(start)
	if streamErr != nil {
		return streamErr
	}

	if len(stdout) > 0 {
		if err := stream.Send(&microvmexecv1alpha1.ExecCommandResponse{
			Payload: &microvmexecv1alpha1.ExecCommandResponse_Stdout{Stdout: stdout},
		}); err != nil {
			return err
		}
	}
	if len(stderr) > 0 {
		if err := stream.Send(&microvmexecv1alpha1.ExecCommandResponse{
			Payload: &microvmexecv1alpha1.ExecCommandResponse_Stderr{Stderr: stderr},
		}); err != nil {
			return err
		}
	}
	if execErr != "" {
		return stream.Send(&microvmexecv1alpha1.ExecCommandResponse{
			Payload: &microvmexecv1alpha1.ExecCommandResponse_Error{Error: execErr},
		})
	}

	return stream.Send(&microvmexecv1alpha1.ExecCommandResponse{
		Payload: &microvmexecv1alpha1.ExecCommandResponse_ExitCode{ExitCode: exitCode},
	})
}

// startFakeExecServer starts a fake flintlock MicroVMExec gRPC server on a
// real TCP loopback listener and returns its address. The server is
// stopped via t.Cleanup.
func startFakeExecServer(t *testing.T, fake *fakeMicroVMExecServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	microvmexecv1alpha1.RegisterMicroVMExecServer(srv, fake)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

func dialExecClient(t *testing.T, addr string) microvmexecv1alpha1.MicroVMExecClient {
	t.Helper()

	pool, err := flintlockclient.New(&config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: addr, TLS: config.TLSConfig{Insecure: true}},
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	client, err := pool.ExecClient("host-a")
	if err != nil {
		t.Fatalf("ExecClient: %v", err)
	}
	return client
}

func TestExec_Success(t *testing.T) {
	fake := &fakeMicroVMExecServer{
		respond: func(start *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			if start.GetUid() != "vm-1" || start.GetCmd() != "echo hi" || !start.GetShell() {
				t.Errorf("unexpected start message: %+v", start)
			}
			return []byte("hi\n"), nil, 0, "", nil
		},
	}
	addr := startFakeExecServer(t, fake)
	client := dialExecClient(t, addr)

	result, err := flintlockclient.Exec(context.Background(), client, "vm-1", "echo hi", flintlockclient.ExecOptions{})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if string(result.Stdout) != "hi\n" {
		t.Fatalf("expected stdout %q, got %q", "hi\n", result.Stdout)
	}
	if result.ExitCode != 0 {
		t.Fatalf("expected exit code 0, got %d", result.ExitCode)
	}
}

func TestExec_NonZeroExit(t *testing.T) {
	fake := &fakeMicroVMExecServer{
		respond: func(_ *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			return nil, []byte("boom\n"), 1, "", nil
		},
	}
	addr := startFakeExecServer(t, fake)
	client := dialExecClient(t, addr)

	result, err := flintlockclient.Exec(context.Background(), client, "vm-1", "false", flintlockclient.ExecOptions{})
	if err != nil {
		t.Fatalf("expected no error for non-zero exit code, got %v", err)
	}
	if result.ExitCode != 1 {
		t.Fatalf("expected exit code 1, got %d", result.ExitCode)
	}
	if string(result.Stderr) != "boom\n" {
		t.Fatalf("expected stderr %q, got %q", "boom\n", result.Stderr)
	}
}

func TestExec_ServerError(t *testing.T) {
	fake := &fakeMicroVMExecServer{
		respond: func(_ *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			return nil, nil, 0, "guest agent not ready", nil
		},
	}
	addr := startFakeExecServer(t, fake)
	client := dialExecClient(t, addr)

	_, err := flintlockclient.Exec(context.Background(), client, "vm-1", "echo hi", flintlockclient.ExecOptions{})
	if !errors.Is(err, flintlockclient.ErrExecFailed) {
		t.Fatalf("expected ErrExecFailed, got %v", err)
	}
}

func TestExec_RPCError(t *testing.T) {
	fake := &fakeMicroVMExecServer{
		respond: func(_ *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			return nil, nil, 0, "", status.Error(codes.Unavailable, "guest agent unreachable")
		},
	}
	addr := startFakeExecServer(t, fake)
	client := dialExecClient(t, addr)

	_, err := flintlockclient.Exec(context.Background(), client, "vm-1", "echo hi", flintlockclient.ExecOptions{})
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if errors.Is(err, flintlockclient.ErrExecFailed) {
		t.Fatalf("expected a plain RPC error, not ErrExecFailed: %v", err)
	}
}

func TestWaitReady_EventualSuccess(t *testing.T) {
	var calls atomic.Int32
	fake := &fakeMicroVMExecServer{
		respond: func(_ *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			n := calls.Add(1)
			if n < 3 {
				return nil, nil, 0, "", status.Error(codes.Unavailable, "guest agent not up yet")
			}
			return nil, nil, 0, "", nil
		},
	}
	addr := startFakeExecServer(t, fake)
	client := dialExecClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := flintlockclient.WaitReady(ctx, client, "vm-1", 20*time.Millisecond); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if calls.Load() < 3 {
		t.Fatalf("expected at least 3 attempts, got %d", calls.Load())
	}
}

func TestWaitReady_DeadlineExceeded(t *testing.T) {
	fake := &fakeMicroVMExecServer{
		respond: func(_ *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			return nil, nil, 0, "", status.Error(codes.Unavailable, "guest agent never ready")
		},
	}
	addr := startFakeExecServer(t, fake)
	client := dialExecClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := flintlockclient.WaitReady(ctx, client, "vm-1", 10*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("WaitReady took too long to give up: %v", elapsed)
	}
}

func TestWaitReady_InvalidInterval(t *testing.T) {
	fake := &fakeMicroVMExecServer{
		respond: func(_ *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			t.Fatalf("exec should not be attempted with a non-positive interval")
			return nil, nil, 0, "", nil
		},
	}
	addr := startFakeExecServer(t, fake)
	client := dialExecClient(t, addr)

	for _, interval := range []time.Duration{0, -1 * time.Second} {
		if err := flintlockclient.WaitReady(context.Background(), client, "vm-1", interval); err == nil {
			t.Fatalf("expected error for interval %v, got nil", interval)
		}
	}
}

func TestPool_ExecClientUnknownHost(t *testing.T) {
	addr := startFakeExecServer(t, &fakeMicroVMExecServer{
		respond: func(_ *microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			return nil, nil, 0, "", nil
		},
	})

	pool, err := flintlockclient.New(&config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: addr, TLS: config.TLSConfig{Insecure: true}},
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	if _, err := pool.ExecClient("does-not-exist"); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("expected ErrUnknownHost, got %v", err)
	}
	if _, err := pool.SSHProxyClient("does-not-exist"); !errors.Is(err, flintlockclient.ErrUnknownHost) {
		t.Fatalf("expected ErrUnknownHost, got %v", err)
	}
}
