package hostagent_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/hostagent"
)

func TestWaitReadySucceedsImmediately(t *testing.T) {
	runner := &fakeRunner{}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)

	_, err := server.WaitReady(context.Background(), &poolmgrv1alpha1.WaitReadyRequest{
		VsockPath: "/run/flintlock/a.vsock",
		Timeout:   durationpb.New(time.Second),
	})
	if err != nil {
		t.Fatalf("WaitReady returned error: %v", err)
	}
	if got := runner.pingCallCount(); got != 1 {
		t.Fatalf("expected exactly 1 ping call, got %d", got)
	}
}

func TestWaitReadySucceedsAfterRetries(t *testing.T) {
	runner := &fakeRunner{
		pingErrs: []error{errors.New("not ready"), errors.New("not ready"), nil},
	}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)

	_, err := server.WaitReady(context.Background(), &poolmgrv1alpha1.WaitReadyRequest{
		VsockPath: "/run/flintlock/a.vsock",
		Timeout:   durationpb.New(time.Second),
	})
	if err != nil {
		t.Fatalf("WaitReady returned error: %v", err)
	}
	if got := runner.pingCallCount(); got != 3 {
		t.Fatalf("expected exactly 3 ping calls, got %d", got)
	}
}

func TestWaitReadyTimesOut(t *testing.T) {
	runner := &fakeRunner{
		pingErrs: []error{errors.New("never ready")},
	}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)

	_, err := server.WaitReady(context.Background(), &poolmgrv1alpha1.WaitReadyRequest{
		VsockPath: "/run/flintlock/a.vsock",
		Timeout:   durationpb.New(20 * time.Millisecond),
	})
	if err == nil {
		t.Fatal("expected an error when the guest-agent never becomes ready")
	}
	if got := status.Code(err); got != codes.DeadlineExceeded {
		t.Fatalf("expected codes.DeadlineExceeded, got %v", got)
	}
}

func TestWaitReadyRejectsEmptyVsockPath(t *testing.T) {
	runner := &fakeRunner{}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)

	_, err := server.WaitReady(context.Background(), &poolmgrv1alpha1.WaitReadyRequest{
		VsockPath: "",
		Timeout:   durationpb.New(time.Second),
	})
	if err == nil {
		t.Fatal("expected an error for an empty vsock_path")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("expected codes.InvalidArgument, got %v", got)
	}
}

func TestWaitReadyUsesConfiguredPort(t *testing.T) {
	runner := &fakeRunner{}
	server := hostagent.NewServer(runner, 1024, time.Millisecond)

	_, err := server.WaitReady(context.Background(), &poolmgrv1alpha1.WaitReadyRequest{
		VsockPath: "/run/flintlock/a.vsock",
		Timeout:   durationpb.New(time.Second),
	})
	if err != nil {
		t.Fatalf("WaitReady returned error: %v", err)
	}

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if len(runner.pingCalls) != 1 || runner.pingCalls[0].port != 1024 || runner.pingCalls[0].vsockPath != "/run/flintlock/a.vsock" {
		t.Fatalf("unexpected ping call recorded: %+v", runner.pingCalls)
	}
}
