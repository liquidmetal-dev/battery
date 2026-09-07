package reconciler_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
)

// fakeMicroVM is a minimal flintlock MicroVM service used to exercise the
// provisioning pipeline without a real flintlock host.
//
// pollsUntilCreated is how many GetMicroVM calls (per uid) return PENDING
// before the state changes; failAfterPolls decides whether it then becomes
// FAILED (instead of CREATED).
type fakeMicroVM struct {
	microvmv1alpha1.UnimplementedMicroVMServer

	pollsUntilCreated int
	failAfterPolls    bool

	mu       sync.Mutex
	getCalls map[string]int
	deleted  []string
	created  []*flintlocktypes.MicroVMSpec
}

var fakeUIDCounter atomic.Int64

func (f *fakeMicroVM) CreateMicroVM(_ context.Context, req *microvmv1alpha1.CreateMicroVMRequest) (*microvmv1alpha1.CreateMicroVMResponse, error) {
	spec, _ := proto.Clone(req.GetMicrovm()).(*flintlocktypes.MicroVMSpec)
	if spec == nil {
		spec = &flintlocktypes.MicroVMSpec{}
	}
	uid := fmt.Sprintf("vm-%d", fakeUIDCounter.Add(1))
	spec.Uid = &uid

	f.mu.Lock()
	f.created = append(f.created, spec)
	f.mu.Unlock()

	return &microvmv1alpha1.CreateMicroVMResponse{
		Microvm: &flintlocktypes.MicroVM{
			Spec:   spec,
			Status: &flintlocktypes.MicroVMStatus{State: flintlocktypes.MicroVMStatus_PENDING},
		},
	}, nil
}

func (f *fakeMicroVM) GetMicroVM(_ context.Context, req *microvmv1alpha1.GetMicroVMRequest) (*microvmv1alpha1.GetMicroVMResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.getCalls == nil {
		f.getCalls = make(map[string]int)
	}
	f.getCalls[req.GetUid()]++
	n := f.getCalls[req.GetUid()]

	state := flintlocktypes.MicroVMStatus_CREATED
	switch {
	case n <= f.pollsUntilCreated:
		state = flintlocktypes.MicroVMStatus_PENDING
	case f.failAfterPolls:
		state = flintlocktypes.MicroVMStatus_FAILED
	}

	return &microvmv1alpha1.GetMicroVMResponse{
		Microvm: &flintlocktypes.MicroVM{
			Status: &flintlocktypes.MicroVMStatus{State: state},
		},
	}, nil
}

func (f *fakeMicroVM) DeleteMicroVM(_ context.Context, req *microvmv1alpha1.DeleteMicroVMRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	f.deleted = append(f.deleted, req.GetUid())
	f.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func (f *fakeMicroVM) deletedUIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deleted))
	copy(out, f.deleted)
	return out
}

// fakeMicroVMExec is a minimal flintlock MicroVMExec service. respond is
// called once per ExecCommand stream after the start message is received;
// it drives what the fake sends back (or the error it returns). A nil
// respond always succeeds with exit code 0 (used to satisfy
// flintlockclient.WaitReady's readiness probe).
type fakeMicroVMExec struct {
	microvmexecv1alpha1.UnimplementedMicroVMExecServer

	respond func(start *microvmexecv1alpha1.ExecStart) (stdout, stderr []byte, exitCode int32, execErr string, streamErr error)
}

func (f *fakeMicroVMExec) ExecCommand(stream microvmexecv1alpha1.MicroVMExec_ExecCommandServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	start := req.GetStart()

	respond := f.respond
	if respond == nil {
		respond = func(*microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			return nil, nil, 0, "", nil
		}
	}

	stdout, stderr, exitCode, execErr, streamErr := respond(start)
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

// startFakeFlintlock starts a fake flintlock gRPC server (MicroVM +
// MicroVMExec, both backed by fake) on a real loopback listener, dials it
// as a single-host flintlockclient.Pool named "host-a", and returns the
// pool. Server and pool are cleaned up via t.Cleanup.
func startFakeFlintlock(t *testing.T, vm *fakeMicroVM, exec *fakeMicroVMExec) *flintlockclient.Pool {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	microvmv1alpha1.RegisterMicroVMServer(srv, vm)
	microvmexecv1alpha1.RegisterMicroVMExecServer(srv, exec)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	pool, err := flintlockclient.New(&config.Config{Hosts: []config.HostConfig{
		{Name: "host-a", Address: lis.Addr().String(), TLS: config.TLSConfig{Insecure: true}},
	}})
	if err != nil {
		t.Fatalf("flintlockclient.New: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })

	return pool
}
