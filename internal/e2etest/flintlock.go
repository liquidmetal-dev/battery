//go:build e2e

// Package e2etest holds test doubles and fixtures shared by battery's
// automated end-to-end suites (cmd/poolmgrd/e2e_test.go and
// cmd/poolmgrctl/e2e_test.go): a fake flintlock (MicroVM + MicroVMExec)
// double, and a minimal, valid PoolSpec fixture builder.
//
// It carries the same "e2e" build tag as its callers, so it never enters
// the default `go test ./...`/`go build ./...` build - it exists purely to
// avoid duplicating these helpers between the two e2e test files.
package e2etest

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/config"
	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
)

// FakeMicroVM is a minimal flintlock MicroVM service that reports every
// created microvm as CREATED on the very first GetMicroVM call: e2e suites
// use this to prove poolmgrd's/poolmgrctl's own lifecycle logic, not
// flintlock's provisioning latency.
type FakeMicroVM struct {
	microvmv1alpha1.UnimplementedMicroVMServer

	mu      sync.Mutex
	nextUID int
}

// CreateMicroVM implements the flintlock MicroVM CreateMicroVM RPC.
func (f *FakeMicroVM) CreateMicroVM(_ context.Context, req *microvmv1alpha1.CreateMicroVMRequest) (*microvmv1alpha1.CreateMicroVMResponse, error) {
	spec, _ := proto.Clone(req.GetMicrovm()).(*flintlocktypes.MicroVMSpec)
	if spec == nil {
		spec = &flintlocktypes.MicroVMSpec{}
	}

	f.mu.Lock()
	f.nextUID++
	uid := fmt.Sprintf("e2e-vm-%d", f.nextUID)
	f.mu.Unlock()

	spec.Uid = &uid
	return &microvmv1alpha1.CreateMicroVMResponse{
		Microvm: &flintlocktypes.MicroVM{
			Spec:   spec,
			Status: &flintlocktypes.MicroVMStatus{State: flintlocktypes.MicroVMStatus_CREATED},
		},
	}, nil
}

// GetMicroVM implements the flintlock MicroVM GetMicroVM RPC.
func (f *FakeMicroVM) GetMicroVM(_ context.Context, _ *microvmv1alpha1.GetMicroVMRequest) (*microvmv1alpha1.GetMicroVMResponse, error) {
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

// DeleteMicroVM implements the flintlock MicroVM DeleteMicroVM RPC.
func (f *FakeMicroVM) DeleteMicroVM(context.Context, *microvmv1alpha1.DeleteMicroVMRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

// FakeMicroVMExec is a minimal flintlock MicroVMExec service: every
// ExecCommand call succeeds immediately with exit code 0, satisfying
// flintlockclient.WaitReady's guest-agent readiness probe.
type FakeMicroVMExec struct {
	microvmexecv1alpha1.UnimplementedMicroVMExecServer
}

// ExecCommand implements the flintlock MicroVMExec ExecCommand RPC.
func (f *FakeMicroVMExec) ExecCommand(stream microvmexecv1alpha1.MicroVMExec_ExecCommandServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return stream.Send(&microvmexecv1alpha1.ExecCommandResponse{
		Payload: &microvmexecv1alpha1.ExecCommandResponse_ExitCode{ExitCode: 0},
	})
}

// StartFakeFlintlock starts the fake flintlock (MicroVM + MicroVMExec) on a
// real loopback listener and dials it as a single-host
// flintlockclient.Pool named "host-a".
func StartFakeFlintlock(t *testing.T) *flintlockclient.Pool {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	microvmv1alpha1.RegisterMicroVMServer(srv, &FakeMicroVM{})
	microvmexecv1alpha1.RegisterMicroVMExecServer(srv, &FakeMicroVMExec{})
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

// MinSizeThresholdPoolSpec returns a minimal, valid PoolSpec on host-a that
// keeps exactly minSize VMs warm via MIN_SIZE_THRESHOLD.
func MinSizeThresholdPoolSpec(name string, size, minSize int32) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:            name,
		Namespace:       "e2e",
		Size:            size,
		FlintlockHosts:  []string{"host-a"},
		MicrovmTemplate: &flintlocktypes.MicroVMSpec{Vcpu: 1, MemoryInMb: 1024},
		ReplenishmentStrategy: &poolmgrv1alpha1.ReplenishmentStrategy{
			Type:    poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
			MinSize: proto.Int32(minSize),
		},
		HookFailurePolicy:        poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE,
		HeartbeatInterval:        durationpb.New(30 * time.Second),
		HeartbeatExpiryThreshold: durationpb.New(90 * time.Second),
	}
}
