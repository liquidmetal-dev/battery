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

func TestDrainAndUndrainHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	s := api.NewHostAdminServer(st)

	drained, err := s.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-a", Reason: "kernel upgrade"})
	if err != nil {
		t.Fatalf("DrainHost() error = %v", err)
	}
	if !drained.GetDrained() || drained.GetDrainedReason() != "kernel upgrade" {
		t.Errorf("DrainHost() = %+v, want drained=true reason=%q", drained, "kernel upgrade")
	}

	undrained, err := s.UndrainHost(ctx, &poolmgrv1alpha1.UndrainHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("UndrainHost() error = %v", err)
	}
	if undrained.GetDrained() {
		t.Errorf("UndrainHost() drained = true, want false")
	}
}

func TestDrainHostIdempotent(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	s := api.NewHostAdminServer(st)

	if _, err := s.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("DrainHost() error = %v", err)
	}
	if _, err := s.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("DrainHost() second call error = %v", err)
	}
}

func TestDrainHostUnknownHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewHostAdminServer(st)

	_, err := s.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "missing"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("DrainHost() error = %v, want FailedPrecondition", err)
	}
}

func TestUndrainHostUnknownHost(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewHostAdminServer(st)

	_, err := s.UndrainHost(ctx, &poolmgrv1alpha1.UndrainHostRequest{Name: "missing"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("UndrainHost() error = %v, want FailedPrecondition", err)
	}
}

func TestDrainHostRequiresName(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewHostAdminServer(st)

	_, err := s.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("DrainHost() error = %v, want InvalidArgument", err)
	}
}

func TestListHosts(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	seedTestHost(ctx, t, st, "host-b")
	s := api.NewHostAdminServer(st)

	if _, err := s.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-b"}); err != nil {
		t.Fatalf("DrainHost() error = %v", err)
	}

	resp, err := s.ListHosts(ctx, &poolmgrv1alpha1.ListHostsRequest{})
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(resp.GetHosts()) != 2 {
		t.Fatalf("ListHosts() = %d hosts, want 2", len(resp.GetHosts()))
	}
	if resp.GetHosts()[0].GetHost().GetName() != "host-a" || resp.GetHosts()[0].GetActiveVmCount() != 0 {
		t.Errorf("ListHosts()[0] = %+v, want host-a with 0 active VMs", resp.GetHosts()[0])
	}
	if !resp.GetHosts()[1].GetHost().GetDrained() {
		t.Errorf("ListHosts()[1] = %+v, want host-b drained", resp.GetHosts()[1])
	}
}
