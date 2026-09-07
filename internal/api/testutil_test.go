package api_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// scrapeMetrics renders reg's metrics through its HTTP handler and returns
// the exposition-format body.
func scrapeMetrics(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, req)
	body, err := io.ReadAll(w.Result().Body)
	if err != nil {
		t.Fatalf("read scrape body: %v", err)
	}
	return string(body)
}

// openTestStore returns a fresh SQLite-backed Store in a temp dir, closed
// automatically at the end of the test.
func openTestStore(t *testing.T) store.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "poolmgr.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return s
}

// samplePool returns a minimal, valid PoolSpec on host-a with the given
// hook-failure policy and pre_lease_commands.
func samplePool(name string, hookFailurePolicy poolmgrv1alpha1.HookFailurePolicy, preLeaseCommands []string) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:            name,
		Namespace:       "default",
		Size:            1,
		FlintlockHosts:  []string{"host-a"},
		MicrovmTemplate: &flintlocktypes.MicroVMSpec{Vcpu: 1},
		ReplenishmentStrategy: &poolmgrv1alpha1.ReplenishmentStrategy{
			Type:    poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
			MinSize: proto.Int32(1),
		},
		PreLeaseCommands:         preLeaseCommands,
		HookFailurePolicy:        hookFailurePolicy,
		HeartbeatInterval:        durationpb.New(30 * time.Second),
		HeartbeatExpiryThreshold: durationpb.New(90 * time.Second),
	}
}

// sampleEvent returns a minimal outbox Event for poolName/poolNamespace/vmUID.
func sampleEvent(poolName, poolNamespace, vmUID string, eventType poolmgrv1alpha1.EventType) *poolmgrv1alpha1.Event {
	return &poolmgrv1alpha1.Event{
		PoolName:      poolName,
		PoolNamespace: poolNamespace,
		VmUid:         vmUID,
		Type:          eventType,
		CreatedAt:     timestamppb.Now(),
	}
}

// sampleAvailableVM returns a minimal AVAILABLE VMRecord for poolName on
// host-a.
func sampleAvailableVM(uid, poolName string) *poolmgrv1alpha1.VMRecord {
	now := timestamppb.Now()
	return &poolmgrv1alpha1.VMRecord{
		Uid:           uid,
		PoolName:      poolName,
		PoolNamespace: "default",
		FlintlockHost: "host-a",
		Phase:         poolmgrv1alpha1.VMPhase_AVAILABLE,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

// fakeMicroVM is a minimal flintlock MicroVM service: GetMicroVM returns a
// fixed set of network interfaces, DeleteMicroVM records the uids it was
// called with.
type fakeMicroVM struct {
	microvmv1alpha1.UnimplementedMicroVMServer

	mu        sync.Mutex
	deleted   []string
	deleteErr error // if set, DeleteMicroVM returns this instead of succeeding
}

func (f *fakeMicroVM) GetMicroVM(_ context.Context, _ *microvmv1alpha1.GetMicroVMRequest) (*microvmv1alpha1.GetMicroVMResponse, error) {
	return &microvmv1alpha1.GetMicroVMResponse{
		Microvm: &flintlocktypes.MicroVM{
			Status: &flintlocktypes.MicroVMStatus{
				State: flintlocktypes.MicroVMStatus_CREATED,
				NetworkInterfaces: map[string]*flintlocktypes.NetworkInterfaceStatus{
					"eth0": {HostDeviceName: "eth0"},
				},
			},
		},
	}, nil
}

func (f *fakeMicroVM) DeleteMicroVM(_ context.Context, req *microvmv1alpha1.DeleteMicroVMRequest) (*emptypb.Empty, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	f.deleted = append(f.deleted, req.GetUid())
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
// called once per ExecCommand stream after the start message is received; a
// nil respond always succeeds with exit code 0.
type fakeMicroVMExec struct {
	microvmexecv1alpha1.UnimplementedMicroVMExecServer

	respond func(start *microvmexecv1alpha1.ExecStart) (exitCode int32, execErr string)
}

func (f *fakeMicroVMExec) ExecCommand(stream microvmexecv1alpha1.MicroVMExec_ExecCommandServer) error {
	req, err := stream.Recv()
	if err != nil {
		return err
	}
	start := req.GetStart()

	respond := f.respond
	if respond == nil {
		respond = func(*microvmexecv1alpha1.ExecStart) (int32, string) { return 0, "" }
	}
	exitCode, execErr := respond(start)

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
// MicroVMExec) on a real loopback listener, dials it as a single-host
// flintlockclient.Pool named "host-a", and returns the pool.
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

// spyNotifier records notification calls.
type spyNotifier struct {
	claimed []string
	deleted []string
}

func (s *spyNotifier) NotifyVMClaimed(poolName, poolNamespace string) {
	s.claimed = append(s.claimed, fmt.Sprintf("%s/%s", poolNamespace, poolName))
}

func (s *spyNotifier) NotifyVMDeleted(poolName, poolNamespace string) {
	s.deleted = append(s.deleted, fmt.Sprintf("%s/%s", poolNamespace, poolName))
}
