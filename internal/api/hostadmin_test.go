package api_test

import (
	"context"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func seedTestHost(ctx context.Context, t *testing.T, st store.Store, name string) {
	t.Helper()
	host := &poolmgrv1alpha1.Host{Name: name, Address: name + ":8443", UpdatedAt: timestamppb.Now()}
	if err := st.UpsertHostIfMissing(ctx, host); err != nil {
		t.Fatalf("UpsertHostIfMissing(%q) error = %v", name, err)
	}
}

func TestCordonAndUncordonHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	s := api.NewHostAdminServer(st)

	cordoned, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-a", Reason: "kernel upgrade"})
	if err != nil {
		t.Fatalf("CordonHost() error = %v", err)
	}
	if !cordoned.GetCordoned() || cordoned.GetCordonedReason() != "kernel upgrade" {
		t.Errorf("CordonHost() = %+v, want cordoned=true reason=%q", cordoned, "kernel upgrade")
	}

	uncordoned, err := s.UncordonHost(ctx, &poolmgrv1alpha1.UncordonHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("UncordonHost() error = %v", err)
	}
	if uncordoned.GetCordoned() {
		t.Errorf("UncordonHost() cordoned = true, want false")
	}
}

func TestCordonHostIdempotent(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	s := api.NewHostAdminServer(st)

	if _, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("CordonHost() error = %v", err)
	}
	if _, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("CordonHost() second call error = %v", err)
	}
}

func TestCordonHostUnknownHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewHostAdminServer(st)

	_, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "missing"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("CordonHost() error = %v, want FailedPrecondition", err)
	}
}

func TestUncordonHostUnknownHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewHostAdminServer(st)

	_, err := s.UncordonHost(ctx, &poolmgrv1alpha1.UncordonHostRequest{Name: "missing"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("UncordonHost() error = %v, want FailedPrecondition", err)
	}
}

func TestCordonHostRequiresName(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewHostAdminServer(st)

	_, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("CordonHost() error = %v, want InvalidArgument", err)
	}
}

func TestListHosts(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	seedTestHost(ctx, t, st, "host-b")
	s := api.NewHostAdminServer(st)

	if _, err := s.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-b"}); err != nil {
		t.Fatalf("CordonHost() error = %v", err)
	}

	resp, err := s.ListHosts(ctx, &poolmgrv1alpha1.ListHostsRequest{})
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(resp.GetHosts()) != 2 {
		t.Fatalf("ListHosts() = %d hosts, want 2", len(resp.GetHosts()))
	}
	if resp.GetHosts()[0].GetHost().GetName() != "host-a" || resp.GetHosts()[0].GetVmCount() != 0 {
		t.Errorf("ListHosts()[0] = %+v, want host-a with 0 VMs", resp.GetHosts()[0])
	}
	if !resp.GetHosts()[1].GetHost().GetCordoned() {
		t.Errorf("ListHosts()[1] = %+v, want host-b cordoned", resp.GetHosts()[1])
	}
}

// TestListHosts_VmCountIncludesDeletingAndReservations: vm_count is what an
// operator reads to decide when a cordoned host is empty, so it must include
// VMs whose deletion is still pending or that are quarantined (both still
// exist on the host) and placements that are reserved but not yet recorded.
func TestListHosts_VmCountIncludesDeletingAndReservations(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")

	deleting := sampleAvailableVM("vm-deleting", "pool-a")
	deleting.Phase = poolmgrv1alpha1.VMPhase_DELETING
	quarantined := sampleAvailableVM("vm-quarantined", "pool-a")
	quarantined.Phase = poolmgrv1alpha1.VMPhase_QUARANTINED
	for _, vm := range []*poolmgrv1alpha1.VMRecord{deleting, quarantined} {
		if err := st.CreateVM(ctx, vm); err != nil {
			t.Fatalf("CreateVM(%s) error = %v", vm.GetUid(), err)
		}
	}
	if err := st.ReservePlacement(ctx, "placement-1", "host-a", "pool-a", "default"); err != nil {
		t.Fatalf("ReservePlacement() error = %v", err)
	}

	resp, err := api.NewHostAdminServer(st).ListHosts(ctx, &poolmgrv1alpha1.ListHostsRequest{})
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(resp.GetHosts()) != 1 {
		t.Fatalf("ListHosts() = %d hosts, want 1", len(resp.GetHosts()))
	}
	if got := resp.GetHosts()[0].GetVmCount(); got != 3 {
		t.Errorf("ListHosts() host-a vm_count = %d, want 3 (DELETING + QUARANTINED + reservation)", got)
	}
}
