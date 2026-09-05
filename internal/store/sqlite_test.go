package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func openTestStore(t *testing.T) Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "poolmgr.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return s
}

func samplePoolSpec(name string) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:           name,
		Namespace:      "default",
		Size:           3,
		FlintlockHosts: []string{"host-a", "host-b"},
		MicrovmTemplate: &flintlocktypes.MicroVMSpec{
			Vcpu: 2,
		},
		ReplenishmentStrategy: &poolmgrv1alpha1.ReplenishmentStrategy{
			Type: poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
		},
		CreateCommands:           []string{"echo hello"},
		PreLeaseCommands:         []string{"echo world"},
		HookFailurePolicy:        poolmgrv1alpha1.HookFailurePolicy_QUARANTINE,
		HeartbeatInterval:        durationpb.New(30_000_000_000),
		HeartbeatExpiryThreshold: durationpb.New(90_000_000_000),
	}
}

func TestCreateAndGetPool(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := samplePoolSpec("pool-a")
	if err := s.CreatePool(ctx, want); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	got, err := s.GetPool(ctx, "pool-a", "default")
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}

	if got.Name != want.Name || got.Namespace != want.Namespace {
		t.Errorf("GetPool() name/namespace = %q/%q, want %q/%q", got.Name, got.Namespace, want.Name, want.Namespace)
	}
	if got.Size != want.Size {
		t.Errorf("GetPool() size = %d, want %d", got.Size, want.Size)
	}
	if len(got.FlintlockHosts) != 2 || got.FlintlockHosts[0] != "host-a" {
		t.Errorf("GetPool() flintlock hosts = %v, want %v", got.FlintlockHosts, want.FlintlockHosts)
	}
	if got.MicrovmTemplate == nil || got.MicrovmTemplate.Vcpu != 2 {
		t.Errorf("GetPool() microvm template = %+v, want vcpu=2", got.MicrovmTemplate)
	}
	if got.ReplenishmentStrategy == nil || got.ReplenishmentStrategy.Type != poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD {
		t.Errorf("GetPool() replenishment strategy = %+v, want type=MIN_SIZE_THRESHOLD", got.ReplenishmentStrategy)
	}
	if got.HookFailurePolicy != poolmgrv1alpha1.HookFailurePolicy_QUARANTINE {
		t.Errorf("GetPool() hook failure policy = %v, want QUARANTINE", got.HookFailurePolicy)
	}
	if got.HeartbeatInterval.AsDuration() != want.HeartbeatInterval.AsDuration() {
		t.Errorf("GetPool() heartbeat interval = %v, want %v", got.HeartbeatInterval.AsDuration(), want.HeartbeatInterval.AsDuration())
	}
}

func TestGetPoolNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.GetPool(ctx, "missing", "default"); err != ErrNotFound {
		t.Errorf("GetPool() error = %v, want ErrNotFound", err)
	}
}

func TestListPools(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreatePool(ctx, samplePoolSpec("pool-b")); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := s.CreatePool(ctx, samplePoolSpec("pool-a")); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	got, err := s.ListPools(ctx)
	if err != nil {
		t.Fatalf("ListPools() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListPools() returned %d pools, want 2", len(got))
	}
	if got[0].Name != "pool-a" || got[1].Name != "pool-b" {
		t.Errorf("ListPools() names = [%s, %s], want [pool-a, pool-b]", got[0].Name, got[1].Name)
	}
}

func TestUpdatePool(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	p := samplePoolSpec("pool-a")
	if err := s.CreatePool(ctx, p); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	p.Size = 10
	if err := s.UpdatePool(ctx, p); err != nil {
		t.Fatalf("UpdatePool() error = %v", err)
	}

	got, err := s.GetPool(ctx, "pool-a", "default")
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	if got.Size != 10 {
		t.Errorf("GetPool() size = %d, want 10", got.Size)
	}
}

func TestUpdatePoolNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.UpdatePool(ctx, samplePoolSpec("missing")); err != ErrNotFound {
		t.Errorf("UpdatePool() error = %v, want ErrNotFound", err)
	}
}

func TestDeletePool(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreatePool(ctx, samplePoolSpec("pool-a")); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := s.DeletePool(ctx, "pool-a", "default"); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}

	if _, err := s.GetPool(ctx, "pool-a", "default"); err != ErrNotFound {
		t.Errorf("GetPool() after delete error = %v, want ErrNotFound", err)
	}
}

func TestDeletePoolNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.DeletePool(ctx, "missing", "default"); err != ErrNotFound {
		t.Errorf("DeletePool() error = %v, want ErrNotFound", err)
	}
}

func sampleVMRecord(uid, poolName string, phase poolmgrv1alpha1.VMPhase) *poolmgrv1alpha1.VMRecord {
	now := timestamppb.New(time.Unix(1_700_000_000, 0))
	return &poolmgrv1alpha1.VMRecord{
		Uid:           uid,
		PoolName:      poolName,
		FlintlockHost: "host-a",
		Phase:         phase,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func TestCreateAndGetVM(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := sampleVMRecord("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_PROVISIONING)
	if err := s.CreateVM(ctx, want); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	got, err := s.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetVM() = %+v, want %+v", got, want)
	}
}

func TestGetVMNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.GetVM(ctx, "missing"); err != ErrNotFound {
		t.Errorf("GetVM() error = %v, want ErrNotFound", err)
	}
}

func TestCreateVMWithLeaseID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := sampleVMRecord("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_LEASED)
	leaseID := "lease-1"
	want.LeaseId = &leaseID
	if err := s.CreateVM(ctx, want); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	got, err := s.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if got.GetLeaseId() != "lease-1" {
		t.Errorf("GetVM() lease id = %q, want %q", got.GetLeaseId(), "lease-1")
	}
}

func TestListVMsByPool(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_AVAILABLE)))
	must(s.CreateVM(ctx, sampleVMRecord("vm-2", "pool-a", poolmgrv1alpha1.VMPhase_LEASED)))
	must(s.CreateVM(ctx, sampleVMRecord("vm-3", "pool-b", poolmgrv1alpha1.VMPhase_AVAILABLE)))

	all, err := s.ListVMsByPool(ctx, "pool-a", nil)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListVMsByPool(pool-a, nil) returned %d vms, want 2", len(all))
	}

	available := poolmgrv1alpha1.VMPhase_AVAILABLE
	filtered, err := s.ListVMsByPool(ctx, "pool-a", &available)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].Uid != "vm-1" {
		t.Fatalf("ListVMsByPool(pool-a, AVAILABLE) = %+v, want [vm-1]", filtered)
	}
}

func TestUpdateVM(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	v := sampleVMRecord("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_PROVISIONING)
	if err := s.CreateVM(ctx, v); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	v.Phase = poolmgrv1alpha1.VMPhase_AVAILABLE
	if err := s.UpdateVM(ctx, v); err != nil {
		t.Fatalf("UpdateVM() error = %v", err)
	}

	got, err := s.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if got.Phase != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Errorf("GetVM() phase = %v, want AVAILABLE", got.Phase)
	}
}

func TestUpdateVMNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.UpdateVM(ctx, sampleVMRecord("missing", "pool-a", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != ErrNotFound {
		t.Errorf("UpdateVM() error = %v, want ErrNotFound", err)
	}
}

func TestDeleteVM(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}
	if err := s.DeleteVM(ctx, "vm-1"); err != nil {
		t.Fatalf("DeleteVM() error = %v", err)
	}
	if _, err := s.GetVM(ctx, "vm-1"); err != ErrNotFound {
		t.Errorf("GetVM() after delete error = %v, want ErrNotFound", err)
	}
}

func TestDeleteVMNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.DeleteVM(ctx, "missing"); err != ErrNotFound {
		t.Errorf("DeleteVM() error = %v, want ErrNotFound", err)
	}
}

func TestClaimAvailableVM(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_LEASED)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}
	if err := s.CreateVM(ctx, sampleVMRecord("vm-2", "pool-a", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	claimed, err := s.ClaimAvailableVM(ctx, "pool-a")
	if err != nil {
		t.Fatalf("ClaimAvailableVM() error = %v", err)
	}
	if claimed.Uid != "vm-2" {
		t.Fatalf("ClaimAvailableVM() claimed %q, want vm-2", claimed.Uid)
	}
	if claimed.Phase != poolmgrv1alpha1.VMPhase_LEASED {
		t.Errorf("ClaimAvailableVM() phase = %v, want LEASED", claimed.Phase)
	}

	got, err := s.GetVM(ctx, "vm-2")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if got.Phase != poolmgrv1alpha1.VMPhase_LEASED {
		t.Errorf("GetVM() phase after claim = %v, want LEASED", got.Phase)
	}
}

func TestClaimAvailableVMNoneAvailable(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_LEASED)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.ClaimAvailableVM(ctx, "pool-a"); err != ErrNoAvailableVM {
		t.Errorf("ClaimAvailableVM() error = %v, want ErrNoAvailableVM", err)
	}
}

func TestClaimAvailableVMConcurrent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	const available = 5
	const attempts = 20
	for i := 0; i < available; i++ {
		uid := "vm-" + string(rune('a'+i))
		if err := s.CreateVM(ctx, sampleVMRecord(uid, "pool-a", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
			t.Fatalf("CreateVM() error = %v", err)
		}
	}

	results := make(chan error, attempts)
	claimedUIDs := make(chan string, attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			vm, err := s.ClaimAvailableVM(ctx, "pool-a")
			if err != nil {
				results <- err
				claimedUIDs <- ""
				return
			}
			results <- nil
			claimedUIDs <- vm.Uid
		}()
	}

	seen := map[string]int{}
	successes := 0
	for i := 0; i < attempts; i++ {
		err := <-results
		uid := <-claimedUIDs
		if err == nil {
			successes++
			seen[uid]++
		} else if err != ErrNoAvailableVM {
			t.Errorf("ClaimAvailableVM() unexpected error = %v", err)
		}
	}

	if successes != available {
		t.Errorf("successful claims = %d, want %d", successes, available)
	}
	for uid, count := range seen {
		if count != 1 {
			t.Errorf("vm %q claimed %d times, want exactly 1", uid, count)
		}
	}
}

func sampleLeaseRecord(leaseID, vmUID, poolName string, expiresAt time.Time) *poolmgrv1alpha1.LeaseRecord {
	claimedAt := time.Unix(1_700_000_000, 0)
	return &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         leaseID,
		VmUid:           vmUID,
		PoolName:        poolName,
		ClaimedAt:       timestamppb.New(claimedAt),
		LastHeartbeatAt: timestamppb.New(claimedAt),
		ExpiresAt:       timestamppb.New(expiresAt),
	}
}

func TestCreateAndGetLease(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := sampleLeaseRecord("lease-1", "vm-1", "pool-a", time.Unix(1_700_001_000, 0))
	if err := s.CreateLease(ctx, want); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}

	got, err := s.GetLease(ctx, "lease-1")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetLease() = %+v, want %+v", got, want)
	}
}

func TestGetLeaseNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.GetLease(ctx, "missing"); err != ErrNotFound {
		t.Errorf("GetLease() error = %v, want ErrNotFound", err)
	}
}

func TestUpdateLeaseHeartbeat(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	l := sampleLeaseRecord("lease-1", "vm-1", "pool-a", time.Unix(1_700_001_000, 0))
	if err := s.CreateLease(ctx, l); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}

	newHeartbeat := time.Unix(1_700_000_500, 0)
	if err := s.UpdateLeaseHeartbeat(ctx, "lease-1", newHeartbeat); err != nil {
		t.Fatalf("UpdateLeaseHeartbeat() error = %v", err)
	}

	got, err := s.GetLease(ctx, "lease-1")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if !got.LastHeartbeatAt.AsTime().Equal(newHeartbeat) {
		t.Errorf("GetLease() last_heartbeat_at = %v, want %v", got.LastHeartbeatAt.AsTime(), newHeartbeat)
	}
}

func TestUpdateLeaseHeartbeatNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.UpdateLeaseHeartbeat(ctx, "missing", time.Now()); err != ErrNotFound {
		t.Errorf("UpdateLeaseHeartbeat() error = %v, want ErrNotFound", err)
	}
}

func TestDeleteLease(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	l := sampleLeaseRecord("lease-1", "vm-1", "pool-a", time.Unix(1_700_001_000, 0))
	if err := s.CreateLease(ctx, l); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}
	if err := s.DeleteLease(ctx, "lease-1"); err != nil {
		t.Fatalf("DeleteLease() error = %v", err)
	}
	if _, err := s.GetLease(ctx, "lease-1"); err != ErrNotFound {
		t.Errorf("GetLease() after delete error = %v, want ErrNotFound", err)
	}
}

func TestDeleteLeaseNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.DeleteLease(ctx, "missing"); err != ErrNotFound {
		t.Errorf("DeleteLease() error = %v, want ErrNotFound", err)
	}
}

func TestListExpiredLeases(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.CreateLease(ctx, sampleLeaseRecord("lease-expired", "vm-1", "pool-a", time.Unix(1_700_000_000, 0))))
	must(s.CreateLease(ctx, sampleLeaseRecord("lease-active", "vm-2", "pool-a", time.Unix(1_800_000_000, 0))))

	now := time.Unix(1_750_000_000, 0)
	expired, err := s.ListExpiredLeases(ctx, now)
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if len(expired) != 1 || expired[0].LeaseId != "lease-expired" {
		t.Fatalf("ListExpiredLeases() = %+v, want [lease-expired]", expired)
	}
}

func sampleEvent(poolName, vmUID string, eventType poolmgrv1alpha1.EventType) *poolmgrv1alpha1.Event {
	return &poolmgrv1alpha1.Event{
		PoolName:    poolName,
		VmUid:       vmUID,
		Type:        eventType,
		CreatedAt:   timestamppb.New(time.Unix(1_700_000_000, 0)),
		PayloadJson: `{"foo":"bar"}`,
	}
}

func TestAppendEventAssignsID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	e := sampleEvent("pool-a", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	if err := s.AppendEvent(ctx, e); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if e.Id == 0 {
		t.Errorf("AppendEvent() did not assign an id")
	}
}

func TestListEventsSince(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	e1 := sampleEvent("pool-a", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e1))
	e2 := sampleEvent("pool-a", "vm-1", poolmgrv1alpha1.EventType_VM_AVAILABLE)
	must(s.AppendEvent(ctx, e2))
	e3 := sampleEvent("pool-b", "vm-2", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e3))

	got, err := s.ListEventsSince(ctx, "pool-a", e1.Id)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	if len(got) != 1 || got[0].Id != e2.Id {
		t.Fatalf("ListEventsSince(pool-a, %d) = %+v, want [event id %d]", e1.Id, got, e2.Id)
	}
	if got[0].PayloadJson != `{"foo":"bar"}` {
		t.Errorf("ListEventsSince() payload_json = %q, want %q", got[0].PayloadJson, `{"foo":"bar"}`)
	}
}

func TestListEventsSinceZeroReturnsAll(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.AppendEvent(ctx, sampleEvent("pool-a", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)))
	must(s.AppendEvent(ctx, sampleEvent("pool-a", "vm-1", poolmgrv1alpha1.EventType_VM_AVAILABLE)))

	got, err := s.ListEventsSince(ctx, "pool-a", 0)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListEventsSince(pool-a, 0) returned %d events, want 2", len(got))
	}
}
