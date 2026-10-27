package api_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// lockedNotifier records NotifyVMDeleted calls. DrainHost deletes VMs
// concurrently, so unlike spyNotifier it takes a lock.
type lockedNotifier struct {
	mu      sync.Mutex
	deleted []string
}

func (n *lockedNotifier) NotifyVMDeleted(poolName, poolNamespace string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.deleted = append(n.deleted, poolNamespace+"/"+poolName)
}

func (n *lockedNotifier) calls() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.deleted)
}

// drainFixture is a HostAdminServer over a fake flintlock on host-a, with
// pool-a in the store. The pool names no host, so RemoveHost depends only on
// host-a's VMs.
type drainFixture struct {
	st       store.Store
	vm       *fakeMicroVM
	notifier *lockedNotifier
	server   *api.HostAdminServer
}

func newDrainFixture(t *testing.T) drainFixture {
	t.Helper()
	ctx := context.Background()

	st := openTestStore(t)
	seedTestHost(ctx, t, st, "host-a")
	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	pool.FlintlockHosts = nil
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, &fakeMicroVMExec{})
	notifier := &lockedNotifier{}
	return drainFixture{st: st, vm: vm, notifier: notifier, server: api.NewHostAdminServer(st, flint, notifier)}
}

// createVM stores a VM of pool-a on host-a in phase. A LEASED VM also gets
// a lease row.
func (f drainFixture) createVM(t *testing.T, uid string, phase poolmgrv1alpha1.VMPhase) {
	t.Helper()
	ctx := context.Background()

	vm := sampleAvailableVM(uid, "pool-a")
	vm.Phase = phase
	if phase == poolmgrv1alpha1.VMPhase_LEASED {
		leaseID := "lease-" + uid
		vm.LeaseId = &leaseID
		now := timestamppb.Now()
		if err := f.st.CreateLease(ctx, &poolmgrv1alpha1.LeaseRecord{
			LeaseId: leaseID, VmUid: uid, PoolName: "pool-a", PoolNamespace: "default",
			ClaimedAt: now, LastHeartbeatAt: now, ExpiresAt: timestamppb.New(time.Now().Add(time.Hour)),
		}); err != nil {
			t.Fatalf("CreateLease() error = %v", err)
		}
	}
	if err := f.st.CreateVM(ctx, vm); err != nil {
		t.Fatalf("CreateVM(%q) error = %v", uid, err)
	}
}

func (f drainFixture) drainEvents(t *testing.T) []string {
	t.Helper()
	events, err := f.st.ListEventsSince(context.Background(), "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	var uids []string
	for _, e := range events {
		if e.GetType() == poolmgrv1alpha1.EventType_VM_DELETED_ON_HOST_DRAIN {
			uids = append(uids, e.GetVmUid())
		}
	}
	slices.Sort(uids)
	return uids
}

func TestDrainHost(t *testing.T) {
	ctx := context.Background()
	f := newDrainFixture(t)
	f.createVM(t, "vm-available", poolmgrv1alpha1.VMPhase_AVAILABLE)
	f.createVM(t, "vm-failed", poolmgrv1alpha1.VMPhase_FAILED)
	f.createVM(t, "vm-provisioning", poolmgrv1alpha1.VMPhase_PROVISIONING)
	f.createVM(t, "vm-quarantined", poolmgrv1alpha1.VMPhase_QUARANTINED)
	f.createVM(t, "vm-leased", poolmgrv1alpha1.VMPhase_LEASED)

	host, err := f.server.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-a", Reason: "scale-down"})
	if err != nil {
		t.Fatalf("DrainHost() error = %v", err)
	}
	if !host.GetCordoned() || !host.GetDraining() || host.GetCordonedReason() != "scale-down" || host.GetDrainingAt() == nil {
		t.Errorf("DrainHost() = %+v, want cordoned and draining with reason %q", host, "scale-down")
	}

	wantDeleted := []string{"vm-available", "vm-failed", "vm-provisioning", "vm-quarantined"}
	deleted := f.vm.deletedUIDs()
	slices.Sort(deleted)
	if !slices.Equal(deleted, wantDeleted) {
		t.Errorf("DeleteMicroVM calls = %v, want %v", deleted, wantDeleted)
	}
	for _, uid := range wantDeleted {
		if _, err := f.st.GetVM(ctx, uid); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("GetVM(%q) error = %v, want ErrNotFound", uid, err)
		}
	}
	if got := f.drainEvents(t); !slices.Equal(got, wantDeleted) {
		t.Errorf("VM_DELETED_ON_HOST_DRAIN events for %v, want %v", got, wantDeleted)
	}
	if got := f.notifier.calls(); len(got) != len(wantDeleted) {
		t.Errorf("NotifyVMDeleted calls = %v, want one per deleted VM", got)
	}

	leased, err := f.st.GetVM(ctx, "vm-leased")
	if err != nil {
		t.Fatalf("GetVM(vm-leased) error = %v", err)
	}
	if leased.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED || leased.GetLeaseId() != "lease-vm-leased" {
		t.Errorf("leased VM = %+v, want it untouched", leased)
	}

	hs, err := f.server.GetHost(ctx, &poolmgrv1alpha1.GetHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if hs.GetVmCount() != 1 || !hs.GetHost().GetDraining() {
		t.Errorf("GetHost() = %+v, want draining with vm_count 1 (the leased VM)", hs)
	}

	// The leased VM's lease ends; with nothing left, RemoveHost succeeds.
	if err := f.st.DeleteVM(ctx, "vm-leased"); err != nil {
		t.Fatalf("DeleteVM() error = %v", err)
	}
	if _, err := f.server.RemoveHost(ctx, &poolmgrv1alpha1.RemoveHostRequest{Name: "host-a"}); err != nil {
		t.Errorf("RemoveHost() after drain error = %v", err)
	}
}

func TestDrainHostIdempotent(t *testing.T) {
	ctx := context.Background()
	f := newDrainFixture(t)
	f.createVM(t, "vm-1", poolmgrv1alpha1.VMPhase_AVAILABLE)

	first, err := f.server.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("DrainHost() error = %v", err)
	}
	second, err := f.server.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("DrainHost() second call error = %v", err)
	}
	if !second.GetDraining() || !second.GetDrainingAt().AsTime().Equal(first.GetDrainingAt().AsTime()) {
		t.Errorf("DrainHost() second call = %+v, want draining since %v", second, first.GetDrainingAt())
	}
	if got := f.vm.deletedUIDs(); !slices.Equal(got, []string{"vm-1"}) {
		t.Errorf("DeleteMicroVM calls = %v, want [vm-1] once", got)
	}
	if got := f.drainEvents(t); !slices.Equal(got, []string{"vm-1"}) {
		t.Errorf("VM_DELETED_ON_HOST_DRAIN events for %v, want [vm-1] once", got)
	}
}

func TestDrainHostDeleteFailureLeavesVMDeleting(t *testing.T) {
	ctx := context.Background()
	f := newDrainFixture(t)
	f.vm.deleteErr = status.Error(codes.Unavailable, "host unreachable")
	f.createVM(t, "vm-1", poolmgrv1alpha1.VMPhase_AVAILABLE)

	if _, err := f.server.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("DrainHost() error = %v, want OK with the delete left to the sweeper", err)
	}
	vm, err := f.st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Errorf("phase = %v, want DELETING", vm.GetPhase())
	}
	if len(f.notifier.calls()) != 0 {
		t.Errorf("NotifyVMDeleted calls = %v, want none", f.notifier.calls())
	}
}

func TestDrainHostUnknownHost(t *testing.T) {
	_, err := newHostAdmin(t, openTestStore(t)).DrainHost(context.Background(), &poolmgrv1alpha1.DrainHostRequest{Name: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("DrainHost() error = %v, want NotFound", err)
	}
}

func TestDrainHostRequiresName(t *testing.T) {
	_, err := newHostAdmin(t, openTestStore(t)).DrainHost(context.Background(), &poolmgrv1alpha1.DrainHostRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("DrainHost() error = %v, want InvalidArgument", err)
	}
}

func TestUncordonHostEndsDrain(t *testing.T) {
	ctx := context.Background()
	f := newDrainFixture(t)

	if _, err := f.server.DrainHost(ctx, &poolmgrv1alpha1.DrainHostRequest{Name: "host-a"}); err != nil {
		t.Fatalf("DrainHost() error = %v", err)
	}
	// CordonHost on a draining host keeps the drain.
	cordoned, err := f.server.CordonHost(ctx, &poolmgrv1alpha1.CordonHostRequest{Name: "host-a", Reason: "still going"})
	if err != nil {
		t.Fatalf("CordonHost() error = %v", err)
	}
	if !cordoned.GetDraining() {
		t.Errorf("CordonHost() = %+v, want still draining", cordoned)
	}

	host, err := f.server.UncordonHost(ctx, &poolmgrv1alpha1.UncordonHostRequest{Name: "host-a"})
	if err != nil {
		t.Fatalf("UncordonHost() error = %v", err)
	}
	if host.GetCordoned() || host.GetDraining() || host.GetDrainingAt() != nil {
		t.Errorf("UncordonHost() = %+v, want neither cordoned nor draining", host)
	}
}
