package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
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

func TestCreatePoolNilDuration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*poolmgrv1alpha1.PoolSpec)
	}{
		{"nil HeartbeatInterval", func(p *poolmgrv1alpha1.PoolSpec) { p.HeartbeatInterval = nil }},
		{"nil HeartbeatExpiryThreshold", func(p *poolmgrv1alpha1.PoolSpec) { p.HeartbeatExpiryThreshold = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			ctx := context.Background()

			p := samplePoolSpec("pool-a")
			tt.mutate(p)
			if err := s.CreatePool(ctx, p); err == nil {
				t.Fatalf("CreatePool() with %s error = nil, want error", tt.name)
			}
		})
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

func sampleVMRecord(uid, poolName, poolNamespace string, phase poolmgrv1alpha1.VMPhase) *poolmgrv1alpha1.VMRecord {
	now := timestamppb.New(time.Unix(1_700_000_000, 0))
	return &poolmgrv1alpha1.VMRecord{
		Uid:           uid,
		PoolName:      poolName,
		PoolNamespace: poolNamespace,
		FlintlockHost: "host-a",
		Phase:         phase,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func TestCreateAndGetVM(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_PROVISIONING)
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

	want := sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_LEASED)
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

func TestCreateVMNilCreatedAt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	v := sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_PROVISIONING)
	v.CreatedAt = nil
	if err := s.CreateVM(ctx, v); err == nil {
		t.Fatal("CreateVM() with nil CreatedAt error = nil, want error")
	}
}

func TestCreateVMNilUpdatedAt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	v := sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_PROVISIONING)
	v.UpdatedAt = nil
	if err := s.CreateVM(ctx, v); err == nil {
		t.Fatal("CreateVM() with nil UpdatedAt error = nil, want error")
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
	must(s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)))
	must(s.CreateVM(ctx, sampleVMRecord("vm-2", "pool-a", "default", poolmgrv1alpha1.VMPhase_LEASED)))
	must(s.CreateVM(ctx, sampleVMRecord("vm-3", "pool-b", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)))

	all, err := s.ListVMsByPool(ctx, "pool-a", "default", nil)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("ListVMsByPool(pool-a, default, nil) returned %d vms, want 2", len(all))
	}

	available := poolmgrv1alpha1.VMPhase_AVAILABLE
	filtered, err := s.ListVMsByPool(ctx, "pool-a", "default", &available)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(filtered) != 1 || filtered[0].Uid != "vm-1" {
		t.Fatalf("ListVMsByPool(pool-a, default, AVAILABLE) = %+v, want [vm-1]", filtered)
	}
}

func TestListVMsByPoolNamespaceIsolation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.CreateVM(ctx, sampleVMRecord("vm-ns1", "pool-a", "ns-1", poolmgrv1alpha1.VMPhase_AVAILABLE)))
	must(s.CreateVM(ctx, sampleVMRecord("vm-ns2", "pool-a", "ns-2", poolmgrv1alpha1.VMPhase_AVAILABLE)))

	got, err := s.ListVMsByPool(ctx, "pool-a", "ns-1", nil)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(got) != 1 || got[0].Uid != "vm-ns1" {
		t.Fatalf("ListVMsByPool(pool-a, ns-1, nil) = %+v, want [vm-ns1]", got)
	}
}

func TestListVMsByPhase(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_DELETING)))
	must(s.CreateVM(ctx, sampleVMRecord("vm-2", "pool-b", "default", poolmgrv1alpha1.VMPhase_DELETING)))
	must(s.CreateVM(ctx, sampleVMRecord("vm-3", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)))

	got, err := s.ListVMsByPhase(ctx, poolmgrv1alpha1.VMPhase_DELETING)
	if err != nil {
		t.Fatalf("ListVMsByPhase() error = %v", err)
	}
	if len(got) != 2 || got[0].Uid != "vm-1" || got[1].Uid != "vm-2" {
		t.Fatalf("ListVMsByPhase(DELETING) = %+v, want [vm-1, vm-2]", got)
	}
}

func TestUpdateVM(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	v := sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_PROVISIONING)
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

	if err := s.UpdateVM(ctx, sampleVMRecord("missing", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != ErrNotFound {
		t.Errorf("UpdateVM() error = %v, want ErrNotFound", err)
	}
}

func TestDeleteVM(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
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

	if err := s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_LEASED)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}
	if err := s.CreateVM(ctx, sampleVMRecord("vm-2", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	claimed, err := s.ClaimAvailableVM(ctx, "pool-a", "default")
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

	if err := s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_LEASED)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.ClaimAvailableVM(ctx, "pool-a", "default"); err != ErrNoAvailableVM {
		t.Errorf("ClaimAvailableVM() error = %v, want ErrNoAvailableVM", err)
	}
}

func TestClaimAvailableVMNamespaceIsolation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateVM(ctx, sampleVMRecord("vm-ns1", "pool-a", "ns-1", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}
	if err := s.CreateVM(ctx, sampleVMRecord("vm-ns2", "pool-a", "ns-2", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	claimed, err := s.ClaimAvailableVM(ctx, "pool-a", "ns-1")
	if err != nil {
		t.Fatalf("ClaimAvailableVM() error = %v", err)
	}
	if claimed.Uid != "vm-ns1" {
		t.Fatalf("ClaimAvailableVM(pool-a, ns-1) claimed %q, want vm-ns1", claimed.Uid)
	}

	untouched, err := s.GetVM(ctx, "vm-ns2")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if untouched.Phase != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Errorf("GetVM(vm-ns2) phase = %v, want AVAILABLE (unaffected by claim in ns-1)", untouched.Phase)
	}
}

func TestClaimAvailableVMLostRaceReturnsErr(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.ClaimAvailableVM(ctx, "pool-a", "default"); err != nil {
		t.Fatalf("first ClaimAvailableVM() error = %v", err)
	}

	// The only VM in the pool is now LEASED; a second claim must not silently
	// re-claim it (the UPDATE's phase=AVAILABLE guard must reject 0 rows affected).
	if _, err := s.ClaimAvailableVM(ctx, "pool-a", "default"); err != ErrNoAvailableVM {
		t.Errorf("second ClaimAvailableVM() error = %v, want ErrNoAvailableVM", err)
	}
}

func TestClaimAvailableVMConcurrent(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	const available = 5
	const attempts = 20
	for i := 0; i < available; i++ {
		uid := "vm-" + string(rune('a'+i))
		if err := s.CreateVM(ctx, sampleVMRecord(uid, "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
			t.Fatalf("CreateVM() error = %v", err)
		}
	}

	results := make(chan error, attempts)
	claimedUIDs := make(chan string, attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			vm, err := s.ClaimAvailableVM(ctx, "pool-a", "default")
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

func sampleLeaseRecord(leaseID, vmUID, poolName, poolNamespace string, expiresAt time.Time) *poolmgrv1alpha1.LeaseRecord {
	claimedAt := time.Unix(1_700_000_000, 0)
	return &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         leaseID,
		VmUid:           vmUID,
		PoolName:        poolName,
		PoolNamespace:   poolNamespace,
		ClaimedAt:       timestamppb.New(claimedAt),
		LastHeartbeatAt: timestamppb.New(claimedAt),
		ExpiresAt:       timestamppb.New(expiresAt),
	}
}

func TestCreateAndGetLease(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
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

func TestCreateLeaseNilTimestamp(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*poolmgrv1alpha1.LeaseRecord)
	}{
		{"nil ClaimedAt", func(l *poolmgrv1alpha1.LeaseRecord) { l.ClaimedAt = nil }},
		{"nil LastHeartbeatAt", func(l *poolmgrv1alpha1.LeaseRecord) { l.LastHeartbeatAt = nil }},
		{"nil ExpiresAt", func(l *poolmgrv1alpha1.LeaseRecord) { l.ExpiresAt = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			ctx := context.Background()

			l := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
			tt.mutate(l)
			if err := s.CreateLease(ctx, l); err == nil {
				t.Fatalf("CreateLease() with %s error = nil, want error", tt.name)
			}
		})
	}
}

func TestUpdateLeaseHeartbeat(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	l := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
	if err := s.CreateLease(ctx, l); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}

	newHeartbeat := time.Unix(1_700_000_500, 0)
	newExpiresAt := time.Unix(1_700_002_000, 0)
	if err := s.UpdateLeaseHeartbeat(ctx, "lease-1", newHeartbeat, newExpiresAt); err != nil {
		t.Fatalf("UpdateLeaseHeartbeat() error = %v", err)
	}

	got, err := s.GetLease(ctx, "lease-1")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if !got.LastHeartbeatAt.AsTime().Equal(newHeartbeat) {
		t.Errorf("GetLease() last_heartbeat_at = %v, want %v", got.LastHeartbeatAt.AsTime(), newHeartbeat)
	}
	if !got.ExpiresAt.AsTime().Equal(newExpiresAt) {
		t.Errorf("GetLease() expires_at = %v, want %v", got.ExpiresAt.AsTime(), newExpiresAt)
	}
}

func TestUpdateLeaseHeartbeatNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.UpdateLeaseHeartbeat(ctx, "missing", time.Now(), time.Now()); err != ErrNotFound {
		t.Errorf("UpdateLeaseHeartbeat() error = %v, want ErrNotFound", err)
	}
}

func TestDeleteLease(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	l := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
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
	must(s.CreateLease(ctx, sampleLeaseRecord("lease-expired", "vm-1", "pool-a", "default", time.Unix(1_700_000_000, 0))))
	must(s.CreateLease(ctx, sampleLeaseRecord("lease-active", "vm-2", "pool-a", "default", time.Unix(1_800_000_000, 0))))

	now := time.Unix(1_750_000_000, 0)
	expired, err := s.ListExpiredLeases(ctx, now)
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if len(expired) != 1 || expired[0].LeaseId != "lease-expired" {
		t.Fatalf("ListExpiredLeases() = %+v, want [lease-expired]", expired)
	}
}

func TestListLeases(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.CreateLease(ctx, sampleLeaseRecord("lease-a1", "vm-a1", "pool-a", "default", time.Unix(1_700_000_100, 0))))
	must(s.CreateLease(ctx, sampleLeaseRecord("lease-a2", "vm-a2", "pool-a", "default", time.Unix(1_700_000_200, 0))))
	must(s.CreateLease(ctx, sampleLeaseRecord("lease-b1", "vm-b1", "pool-b", "default", time.Unix(1_700_000_300, 0))))

	t.Run("unfiltered returns all leases across pools", func(t *testing.T) {
		got, err := s.ListLeases(ctx, nil)
		if err != nil {
			t.Fatalf("ListLeases(nil) error = %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("ListLeases(nil) len = %d, want 3: %+v", len(got), got)
		}
		wantIDs := []string{"lease-a1", "lease-a2", "lease-b1"}
		for i, l := range got {
			if l.GetLeaseId() != wantIDs[i] {
				t.Fatalf("ListLeases(nil)[%d] = %q, want %q (order by lease_id)", i, l.GetLeaseId(), wantIDs[i])
			}
		}
	})

	t.Run("filtered to a pool with leases returns only that pool's leases", func(t *testing.T) {
		got, err := s.ListLeases(ctx, &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"})
		if err != nil {
			t.Fatalf("ListLeases(pool-a) error = %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("ListLeases(pool-a) len = %d, want 2: %+v", len(got), got)
		}
		for _, l := range got {
			if l.GetPoolName() != "pool-a" {
				t.Fatalf("ListLeases(pool-a) returned lease from pool %q", l.GetPoolName())
			}
		}
	})

	t.Run("filtered to a pool with no leases returns an empty slice, not an error", func(t *testing.T) {
		got, err := s.ListLeases(ctx, &poolmgrv1alpha1.PoolRef{Name: "pool-empty", Namespace: "default"})
		if err != nil {
			t.Fatalf("ListLeases(pool-empty) error = %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("ListLeases(pool-empty) len = %d, want 0: %+v", len(got), got)
		}
	})
}

func TestDeleteLeaseIfExpired_Success(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	now := time.Unix(1_750_000_000, 0)
	l := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", now.Add(-time.Second))
	if err := s.CreateLease(ctx, l); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}

	got, err := s.DeleteLeaseIfExpired(ctx, "lease-1", now)
	if err != nil {
		t.Fatalf("DeleteLeaseIfExpired() error = %v", err)
	}
	if got.GetLeaseId() != "lease-1" {
		t.Fatalf("DeleteLeaseIfExpired() returned %+v, want lease-1", got)
	}
	if _, err := s.GetLease(ctx, "lease-1"); err != ErrNotFound {
		t.Errorf("GetLease() after DeleteLeaseIfExpired error = %v, want ErrNotFound", err)
	}
}

func TestDeleteLeaseIfExpired_NotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.DeleteLeaseIfExpired(ctx, "missing", time.Now()); err != ErrNotFound {
		t.Errorf("DeleteLeaseIfExpired() error = %v, want ErrNotFound", err)
	}
}

// TestDeleteLeaseIfExpired_RenewedSinceObserved is the deterministic
// interleaving test for the TOCTOU race between a sweeper reading an
// expired lease from ListExpiredLeases and a concurrent Heartbeat renewing
// it before the sweeper acts: the renewal is simulated by calling
// UpdateLeaseHeartbeat between the lease becoming "expired as of now" and
// the DeleteLeaseIfExpired call that would otherwise delete it.
func TestDeleteLeaseIfExpired_RenewedSinceObserved(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	now := time.Unix(1_750_000_000, 0)
	l := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", now.Add(-time.Second))
	if err := s.CreateLease(ctx, l); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}

	// Simulate a Heartbeat landing after the sweeper observed the lease as
	// expired (via ListExpiredLeases) but before it claims it for deletion.
	renewedExpiry := now.Add(time.Hour)
	if err := s.UpdateLeaseHeartbeat(ctx, "lease-1", now, renewedExpiry); err != nil {
		t.Fatalf("UpdateLeaseHeartbeat() error = %v", err)
	}

	if _, err := s.DeleteLeaseIfExpired(ctx, "lease-1", now); err != ErrLeaseNotExpired {
		t.Fatalf("DeleteLeaseIfExpired() error = %v, want ErrLeaseNotExpired", err)
	}

	got, err := s.GetLease(ctx, "lease-1")
	if err != nil {
		t.Fatalf("GetLease() after renewed DeleteLeaseIfExpired error = %v", err)
	}
	if !got.GetExpiresAt().AsTime().Equal(renewedExpiry) {
		t.Fatalf("GetLease() expires_at = %v, want %v (renewal must survive)", got.GetExpiresAt().AsTime(), renewedExpiry)
	}
}

func sampleEvent(poolName, poolNamespace, vmUID string, eventType poolmgrv1alpha1.EventType) *poolmgrv1alpha1.Event {
	return &poolmgrv1alpha1.Event{
		PoolName:      poolName,
		PoolNamespace: poolNamespace,
		VmUid:         vmUID,
		Type:          eventType,
		CreatedAt:     timestamppb.New(time.Unix(1_700_000_000, 0)),
		PayloadJson:   `{"foo":"bar"}`,
	}
}

func TestAppendEventAssignsID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	e := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	if err := s.AppendEvent(ctx, e); err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	if e.Id == 0 {
		t.Errorf("AppendEvent() did not assign an id")
	}
}

func TestAppendEventNilCreatedAt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	e := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	e.CreatedAt = nil
	if err := s.AppendEvent(ctx, e); err == nil {
		t.Fatal("AppendEvent() with nil CreatedAt error = nil, want error")
	}

	got, err := s.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ListEventsSince() after rejected AppendEvent = %+v, want no rows inserted", got)
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
	e1 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e1))
	e2 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_AVAILABLE)
	must(s.AppendEvent(ctx, e2))
	e3 := sampleEvent("pool-b", "default", "vm-2", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e3))

	got, err := s.ListEventsSince(ctx, "pool-a", "default", e1.Id, 100)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	if len(got) != 1 || got[0].Id != e2.Id {
		t.Fatalf("ListEventsSince(pool-a, default, %d) = %+v, want [event id %d]", e1.Id, got, e2.Id)
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
	must(s.AppendEvent(ctx, sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)))
	must(s.AppendEvent(ctx, sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_AVAILABLE)))

	got, err := s.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("ListEventsSince(pool-a, default, 0) returned %d events, want 2", len(got))
	}
}

func TestListEventsSinceRespectsLimit(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	e1 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e1))
	e2 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_AVAILABLE)
	must(s.AppendEvent(ctx, e2))
	e3 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_CLAIMED)
	must(s.AppendEvent(ctx, e3))

	got, err := s.ListEventsSince(ctx, "pool-a", "default", 0, 2)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	if len(got) != 2 || got[0].Id != e1.Id || got[1].Id != e2.Id {
		t.Fatalf("ListEventsSince(0, limit=2) = %+v, want [event id %d, event id %d]", got, e1.Id, e2.Id)
	}

	got, err = s.ListEventsSince(ctx, "pool-a", "default", got[len(got)-1].Id, 2)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	if len(got) != 1 || got[0].Id != e3.Id {
		t.Fatalf("ListEventsSince(%d, limit=2) = %+v, want [event id %d]", e2.Id, got, e3.Id)
	}
}

func TestListAllEventsSinceRespectsLimit(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	e1 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e1))
	e2 := sampleEvent("pool-b", "default", "vm-2", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e2))
	e3 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_AVAILABLE)
	must(s.AppendEvent(ctx, e3))

	got, err := s.ListAllEventsSince(ctx, 0, 2)
	if err != nil {
		t.Fatalf("ListAllEventsSince() error = %v", err)
	}
	if len(got) != 2 || got[0].Id != e1.Id || got[1].Id != e2.Id {
		t.Fatalf("ListAllEventsSince(0, limit=2) = %+v, want [event id %d, event id %d]", got, e1.Id, e2.Id)
	}

	got, err = s.ListAllEventsSince(ctx, got[len(got)-1].Id, 2)
	if err != nil {
		t.Fatalf("ListAllEventsSince() error = %v", err)
	}
	if len(got) != 1 || got[0].Id != e3.Id {
		t.Fatalf("ListAllEventsSince(%d, limit=2) = %+v, want [event id %d]", e2.Id, got, e3.Id)
	}
}

func TestListAllEventsSinceOrdersAcrossPools(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	e1 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e1))
	e2 := sampleEvent("pool-b", "default", "vm-2", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e2))
	e3 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_AVAILABLE)
	must(s.AppendEvent(ctx, e3))

	got, err := s.ListAllEventsSince(ctx, 0, 100)
	if err != nil {
		t.Fatalf("ListAllEventsSince() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("ListAllEventsSince(0) returned %d events, want 3", len(got))
	}
	wantIDs := []int64{e1.Id, e2.Id, e3.Id}
	for i, id := range wantIDs {
		if got[i].Id != id {
			t.Errorf("ListAllEventsSince(0)[%d].Id = %d, want %d", i, got[i].Id, id)
		}
	}
}

func TestListAllEventsSinceRespectsSinceID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	e1 := sampleEvent("pool-a", "default", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e1))
	e2 := sampleEvent("pool-b", "default", "vm-2", poolmgrv1alpha1.EventType_VM_PROVISIONED)
	must(s.AppendEvent(ctx, e2))

	got, err := s.ListAllEventsSince(ctx, e1.Id, 100)
	if err != nil {
		t.Fatalf("ListAllEventsSince() error = %v", err)
	}
	if len(got) != 1 || got[0].Id != e2.Id {
		t.Fatalf("ListAllEventsSince(%d) = %+v, want [event id %d]", e1.Id, got, e2.Id)
	}
}

func TestListEventsSinceNamespaceIsolation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.AppendEvent(ctx, sampleEvent("pool-a", "ns-1", "vm-1", poolmgrv1alpha1.EventType_VM_PROVISIONED)))
	must(s.AppendEvent(ctx, sampleEvent("pool-a", "ns-2", "vm-2", poolmgrv1alpha1.EventType_VM_PROVISIONED)))

	got, err := s.ListEventsSince(ctx, "pool-a", "ns-1", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	if len(got) != 1 || got[0].VmUid != "vm-1" {
		t.Fatalf("ListEventsSince(pool-a, ns-1, 0) = %+v, want [vm-1's event]", got)
	}
}

func sampleHost(name string) *poolmgrv1alpha1.Host {
	now := timestamppb.New(time.Unix(1_700_000_000, 0))
	return &poolmgrv1alpha1.Host{
		Name:      name,
		Address:   name + ".example.com:8443",
		UpdatedAt: now,
		Tls: &poolmgrv1alpha1.HostTLS{
			CaFile:   "/etc/poolmgr/" + name + "/ca.pem",
			CertFile: "/etc/poolmgr/" + name + "/cert.pem",
			KeyFile:  "/etc/poolmgr/" + name + "/key.pem",
		},
	}
}

func TestCreateHost(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := sampleHost("host-a")
	if err := s.CreateHost(ctx, want); err != nil {
		t.Fatalf("CreateHost() error = %v", err)
	}

	got, err := s.GetHost(ctx, "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetHost() = %+v, want %+v", got, want)
	}

	list, err := s.ListHosts(ctx)
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(list) != 1 || !proto.Equal(list[0], want) {
		t.Errorf("ListHosts() = %+v, want [%+v]", list, want)
	}
}

func TestCreateHostWithoutTLSReadsBackEmptyTLS(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	h := sampleHost("host-a")
	h.Tls = nil
	if err := s.CreateHost(ctx, h); err != nil {
		t.Fatalf("CreateHost() error = %v", err)
	}

	got, err := s.GetHost(ctx, "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if got.GetTls() == nil || !proto.Equal(got.GetTls(), &poolmgrv1alpha1.HostTLS{}) {
		t.Errorf("GetHost() tls = %v, want a set, empty HostTLS", got.GetTls())
	}
}

func TestCreateHostDuplicate(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateHost(ctx, sampleHost("host-a")); err != nil {
		t.Fatalf("CreateHost() error = %v", err)
	}
	dup := sampleHost("host-a")
	dup.Address = "other:8443"
	if err := s.CreateHost(ctx, dup); !errors.Is(err, ErrHostExists) {
		t.Fatalf("CreateHost() duplicate error = %v, want ErrHostExists", err)
	}

	got, err := s.GetHost(ctx, "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if got.GetAddress() != "host-a.example.com:8443" {
		t.Errorf("GetHost() address = %q after rejected duplicate, want the original", got.GetAddress())
	}
}

func TestUpdateHost(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateHost(ctx, sampleHost("host-a")); err != nil {
		t.Fatalf("CreateHost() error = %v", err)
	}
	cordoned, err := s.SetHostCordoned(ctx, "host-a", true, "maintenance")
	if err != nil {
		t.Fatalf("SetHostCordoned() error = %v", err)
	}

	// Cordon fields and updated_at on the request are ignored.
	update := &poolmgrv1alpha1.Host{
		Name:      "host-a",
		Address:   "new:8443",
		Tls:       &poolmgrv1alpha1.HostTLS{Insecure: true},
		UpdatedAt: timestamppb.New(time.Unix(1, 0)),
	}
	got, err := s.UpdateHost(ctx, update)
	if err != nil {
		t.Fatalf("UpdateHost() error = %v", err)
	}
	if got.GetAddress() != "new:8443" || !proto.Equal(got.GetTls(), update.GetTls()) {
		t.Errorf("UpdateHost() = %+v, want address %q and tls %v", got, "new:8443", update.GetTls())
	}
	if !got.GetCordoned() || got.GetCordonedReason() != "maintenance" ||
		!proto.Equal(got.GetCordonedAt(), cordoned.GetCordonedAt()) {
		t.Errorf("UpdateHost() = %+v, want cordon state preserved from %+v", got, cordoned)
	}
	if got.GetUpdatedAt().AsTime().Before(cordoned.GetUpdatedAt().AsTime()) {
		t.Errorf("UpdateHost() updated_at = %v, want at or after %v", got.GetUpdatedAt().AsTime(), cordoned.GetUpdatedAt().AsTime())
	}

	stored, err := s.GetHost(ctx, "host-a")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	if !proto.Equal(stored, got) {
		t.Errorf("GetHost() = %+v, want what UpdateHost returned %+v", stored, got)
	}
}

func TestUpdateHostNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.UpdateHost(ctx, sampleHost("missing")); !errors.Is(err, ErrNotFound) {
		t.Errorf("UpdateHost() error = %v, want ErrNotFound", err)
	}
}

func TestDeleteHost(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateHost(ctx, sampleHost("host-a")); err != nil {
		t.Fatalf("CreateHost() error = %v", err)
	}
	if err := s.DeleteHost(ctx, "host-a"); err != nil {
		t.Fatalf("DeleteHost() error = %v", err)
	}
	if _, err := s.GetHost(ctx, "host-a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetHost() after delete error = %v, want ErrNotFound", err)
	}
	if err := s.DeleteHost(ctx, "host-a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("DeleteHost() second call error = %v, want ErrNotFound", err)
	}
}

func TestDeleteHostRefusesHostInUse(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(ctx context.Context, t *testing.T, s Store)
		wantPools []string
		wantVMs   int32
	}{
		{
			name: "named by pools",
			setup: func(ctx context.Context, t *testing.T, s Store) {
				other := samplePoolSpec("pool-b")
				other.Namespace = "team-x"
				unrelated := samplePoolSpec("pool-c")
				unrelated.FlintlockHosts = []string{"host-b"}
				for _, p := range []*poolmgrv1alpha1.PoolSpec{samplePoolSpec("pool-a"), other, unrelated} {
					if err := s.CreatePool(ctx, p); err != nil {
						t.Fatalf("CreatePool(%s) error = %v", p.GetName(), err)
					}
				}
			},
			wantPools: []string{"default/pool-a", "team-x/pool-b"},
		},
		{
			name: "vm record",
			setup: func(ctx context.Context, t *testing.T, s Store) {
				if err := s.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_DELETING)); err != nil {
					t.Fatalf("CreateVM() error = %v", err)
				}
			},
			wantVMs: 1,
		},
		{
			name: "placement reservation",
			setup: func(ctx context.Context, t *testing.T, s Store) {
				if err := s.ReservePlacement(ctx, "placement-1", "host-a", "pool-a", "default"); err != nil {
					t.Fatalf("ReservePlacement() error = %v", err)
				}
			},
			wantVMs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := openTestStore(t)
			ctx := context.Background()

			if err := s.CreateHost(ctx, sampleHost("host-a")); err != nil {
				t.Fatalf("CreateHost() error = %v", err)
			}
			tt.setup(ctx, t, s)

			err := s.DeleteHost(ctx, "host-a")
			var inUse *HostInUseError
			if !errors.As(err, &inUse) {
				t.Fatalf("DeleteHost() error = %v, want *HostInUseError", err)
			}
			if !slices.Equal(inUse.Pools, tt.wantPools) || inUse.VMCount != tt.wantVMs {
				t.Errorf("DeleteHost() error = %+v, want pools %v and %d vms", inUse, tt.wantPools, tt.wantVMs)
			}
			if _, err := s.GetHost(ctx, "host-a"); err != nil {
				t.Errorf("GetHost() after refused delete error = %v, want the host kept", err)
			}
		})
	}
}

func TestGetHostNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.GetHost(ctx, "missing"); err != ErrNotFound {
		t.Errorf("GetHost() error = %v, want ErrNotFound", err)
	}
}

func TestListHosts(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.CreateHost(ctx, sampleHost("host-b")))
	must(s.CreateHost(ctx, sampleHost("host-a")))

	got, err := s.ListHosts(ctx)
	if err != nil {
		t.Fatalf("ListHosts() error = %v", err)
	}
	if len(got) != 2 || got[0].GetName() != "host-a" || got[1].GetName() != "host-b" {
		t.Fatalf("ListHosts() = %+v, want [host-a, host-b] ordered by name", got)
	}
}

func TestSetHostCordoned(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateHost(ctx, sampleHost("host-a")); err != nil {
		t.Fatalf("CreateHost() error = %v", err)
	}

	cordoned, err := s.SetHostCordoned(ctx, "host-a", true, "kernel upgrade")
	if err != nil {
		t.Fatalf("SetHostCordoned(true) error = %v", err)
	}
	if !cordoned.GetCordoned() || cordoned.GetCordonedReason() != "kernel upgrade" || cordoned.GetCordonedAt() == nil {
		t.Errorf("SetHostCordoned(true) = %+v, want cordoned=true reason=%q cordoned_at set", cordoned, "kernel upgrade")
	}

	uncordoned, err := s.SetHostCordoned(ctx, "host-a", false, "")
	if err != nil {
		t.Fatalf("SetHostCordoned(false) error = %v", err)
	}
	if uncordoned.GetCordoned() || uncordoned.GetCordonedReason() != "" || uncordoned.GetCordonedAt() != nil {
		t.Errorf("SetHostCordoned(false) = %+v, want cordoned=false, reason and cordoned_at cleared", uncordoned)
	}
}

func TestSetHostCordonedNotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.SetHostCordoned(ctx, "missing", true, ""); err != ErrNotFound {
		t.Errorf("SetHostCordoned() error = %v, want ErrNotFound", err)
	}
}

func TestCountVMsByHost(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	// Every phase counts: a VM in DELETING or QUARANTINED still exists on the
	// host, and a maintenance decision has to see it.
	phases := []poolmgrv1alpha1.VMPhase{
		poolmgrv1alpha1.VMPhase_AVAILABLE,
		poolmgrv1alpha1.VMPhase_LEASED,
		poolmgrv1alpha1.VMPhase_DELETING,
		poolmgrv1alpha1.VMPhase_QUARANTINED,
		poolmgrv1alpha1.VMPhase_FAILED,
	}
	for i, phase := range phases {
		vm := sampleVMRecord(fmt.Sprintf("vm-%d", i), "pool-a", "default", phase)
		vm.FlintlockHost = "host-a"
		must(s.CreateVM(ctx, vm))
	}
	other := sampleVMRecord("vm-b", "pool-b", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)
	other.FlintlockHost = "host-b"
	must(s.CreateVM(ctx, other))
	must(s.CreateHost(ctx, sampleHost("host-a")))
	must(s.ReservePlacement(ctx, "placement-1", "host-a", "pool-a", "default"))

	for host, want := range map[string]int32{"host-a": 6, "host-b": 1, "host-c": 0} {
		got, err := s.CountVMsByHost(ctx, host)
		if err != nil {
			t.Fatalf("CountVMsByHost(%q) error = %v", host, err)
		}
		if got != want {
			t.Errorf("CountVMsByHost(%q) = %d, want %d", host, got, want)
		}
	}
}

func TestReservePlacement_CordonedHostFails(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateHost(ctx, sampleHost("host-a")); err != nil {
		t.Fatalf("setup error = %v", err)
	}
	if _, err := s.SetHostCordoned(ctx, "host-a", true, "maintenance"); err != nil {
		t.Fatalf("SetHostCordoned() error = %v", err)
	}

	err := s.ReservePlacement(ctx, "placement-1", "host-a", "pool-a", "default")
	if !errors.Is(err, ErrHostCordoned) {
		t.Fatalf("ReservePlacement() on cordoned host error = %v, want ErrHostCordoned", err)
	}
	got, err := s.CountVMsByHost(ctx, "host-a")
	if err != nil {
		t.Fatalf("CountVMsByHost() error = %v", err)
	}
	if got != 0 {
		t.Errorf("CountVMsByHost() = %d after refused reservation, want 0", got)
	}
}

// TestReservePlacement_UnregisteredHostFails: a host with no registry row,
// e.g. one RemoveHost deleted after PickHost chose it, must refuse the
// placement and record nothing.
func TestReservePlacement_UnregisteredHostFails(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	err := s.ReservePlacement(ctx, "placement-1", "host-unregistered", "pool-a", "default")
	if !errors.Is(err, ErrHostNotRegistered) {
		t.Fatalf("ReservePlacement() on unregistered host error = %v, want ErrHostNotRegistered", err)
	}
	got, err := s.CountVMsByHost(ctx, "host-unregistered")
	if err != nil {
		t.Fatalf("CountVMsByHost() error = %v", err)
	}
	if got != 0 {
		t.Errorf("CountVMsByHost() = %d, want 0 (nothing reserved)", got)
	}
}

func TestReserveAndReleasePlacement(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreateHost(ctx, sampleHost("host-a")); err != nil {
		t.Fatalf("setup error = %v", err)
	}
	if err := s.ReservePlacement(ctx, "placement-1", "host-a", "pool-a", "default"); err != nil {
		t.Fatalf("ReservePlacement() error = %v", err)
	}
	count := func() int32 {
		t.Helper()
		got, err := s.CountVMsByHost(ctx, "host-a")
		if err != nil {
			t.Fatalf("CountVMsByHost() error = %v", err)
		}
		return got
	}
	if got := count(); got != 1 {
		t.Fatalf("CountVMsByHost() = %d after reserve, want 1", got)
	}

	if err := s.ReleasePlacement(ctx, "placement-1"); err != nil {
		t.Fatalf("ReleasePlacement() error = %v", err)
	}
	if got := count(); got != 0 {
		t.Fatalf("CountVMsByHost() = %d after release, want 0", got)
	}
	// Releasing again (or releasing an id that never existed) is not an
	// error: Provision releases on every exit path, including after an
	// explicit release.
	if err := s.ReleasePlacement(ctx, "placement-1"); err != nil {
		t.Fatalf("ReleasePlacement() second call error = %v, want nil", err)
	}
}

func TestClearPlacements(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("setup error = %v", err)
		}
	}
	must(s.CreateHost(ctx, sampleHost("host-a")))
	must(s.CreateHost(ctx, sampleHost("host-b")))
	must(s.ReservePlacement(ctx, "placement-1", "host-a", "pool-a", "default"))
	must(s.ReservePlacement(ctx, "placement-2", "host-b", "pool-a", "default"))

	if err := s.ClearPlacements(ctx); err != nil {
		t.Fatalf("ClearPlacements() error = %v", err)
	}
	for _, host := range []string{"host-a", "host-b"} {
		got, err := s.CountVMsByHost(ctx, host)
		if err != nil {
			t.Fatalf("CountVMsByHost(%q) error = %v", host, err)
		}
		if got != 0 {
			t.Errorf("CountVMsByHost(%q) = %d after clear, want 0", host, got)
		}
	}
}

func TestCreateLeaseRequestID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
	want.RequestId = "req-1"
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

	leases, err := s.ListLeases(ctx, nil)
	if err != nil {
		t.Fatalf("ListLeases() error = %v", err)
	}
	if len(leases) != 1 || leases[0].GetRequestId() != "req-1" {
		t.Errorf("ListLeases() = %+v, want one lease with request id %q", leases, "req-1")
	}

	expired, err := s.ListExpiredLeases(ctx, time.Unix(1_800_000_000, 0))
	if err != nil {
		t.Fatalf("ListExpiredLeases() error = %v", err)
	}
	if len(expired) != 1 || expired[0].GetRequestId() != "req-1" {
		t.Errorf("ListExpiredLeases() = %+v, want one lease with request id %q", expired, "req-1")
	}

	deleted, err := s.DeleteLeaseIfExpired(ctx, "lease-1", time.Unix(1_800_000_000, 0))
	if err != nil {
		t.Fatalf("DeleteLeaseIfExpired() error = %v", err)
	}
	if deleted.GetRequestId() != "req-1" {
		t.Errorf("DeleteLeaseIfExpired() request id = %q, want %q", deleted.GetRequestId(), "req-1")
	}
}

func TestCreateLeaseDuplicateRequestID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	first := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
	first.RequestId = "req-1"
	if err := s.CreateLease(ctx, first); err != nil {
		t.Fatalf("CreateLease(first) error = %v", err)
	}

	second := sampleLeaseRecord("lease-2", "vm-2", "pool-a", "default", time.Unix(1_700_001_000, 0))
	second.RequestId = "req-1"
	if err := s.CreateLease(ctx, second); !errors.Is(err, ErrDuplicateRequestID) {
		t.Fatalf("CreateLease(second) error = %v, want ErrDuplicateRequestID", err)
	}
	if _, err := s.GetLease(ctx, "lease-2"); err != ErrNotFound {
		t.Errorf("GetLease(lease-2) error = %v, want ErrNotFound", err)
	}

	// Once the first lease ends, its request ID is free again.
	if err := s.DeleteLease(ctx, "lease-1"); err != nil {
		t.Fatalf("DeleteLease() error = %v", err)
	}
	if err := s.CreateLease(ctx, second); err != nil {
		t.Errorf("CreateLease(second) after delete error = %v", err)
	}
}

func TestCreateLeaseDuplicateLeaseIDIsNotDuplicateRequestID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	first := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
	first.RequestId = "req-1"
	if err := s.CreateLease(ctx, first); err != nil {
		t.Fatalf("CreateLease(first) error = %v", err)
	}

	second := sampleLeaseRecord("lease-1", "vm-2", "pool-a", "default", time.Unix(1_700_001_000, 0))
	second.RequestId = "req-2"
	err := s.CreateLease(ctx, second)
	if err == nil || errors.Is(err, ErrDuplicateRequestID) {
		t.Errorf("CreateLease() with duplicate lease id error = %v, want a non-ErrDuplicateRequestID error", err)
	}
}

func TestCreateLeaseEmptyRequestIDs(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for _, id := range []string{"lease-1", "lease-2"} {
		if err := s.CreateLease(ctx, sampleLeaseRecord(id, "vm-"+id, "pool-a", "default", time.Unix(1_700_001_000, 0))); err != nil {
			t.Fatalf("CreateLease(%s) error = %v", id, err)
		}
	}

	got, err := s.GetLease(ctx, "lease-1")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if got.GetRequestId() != "" {
		t.Errorf("GetLease() request id = %q, want empty", got.GetRequestId())
	}
	if _, err := s.GetLeaseByRequestID(ctx, ""); err != ErrNotFound {
		t.Errorf("GetLeaseByRequestID(\"\") error = %v, want ErrNotFound", err)
	}
}

func TestGetLeaseByRequestID(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	want := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
	want.RequestId = "req-1"
	if err := s.CreateLease(ctx, want); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}

	got, err := s.GetLeaseByRequestID(ctx, "req-1")
	if err != nil {
		t.Fatalf("GetLeaseByRequestID() error = %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetLeaseByRequestID() = %+v, want %+v", got, want)
	}

	if _, err := s.GetLeaseByRequestID(ctx, "missing"); err != ErrNotFound {
		t.Errorf("GetLeaseByRequestID(missing) error = %v, want ErrNotFound", err)
	}
}

// schemaVersion returns s's PRAGMA user_version.
func schemaVersion(t *testing.T, s Store) int {
	t.Helper()
	var v int
	if err := s.(*sqliteStore).db.QueryRow(`PRAGMA user_version;`).Scan(&v); err != nil {
		t.Fatalf("read user_version error = %v", err)
	}
	return v
}

func TestOpenFreshDBIsAtLatestVersion(t *testing.T) {
	s := openTestStore(t)

	if got, want := schemaVersion(t, s), len(migrations); got != want {
		t.Errorf("user_version = %d, want %d", got, want)
	}
}

func TestOpenUpgradesVersionZeroDB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "poolmgr.db")

	// Build a database as a binary from before migrations existed would
	// have: only the baseline schema, user_version 0, and a lease row.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatalf("apply baseline schema error = %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO leases (lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at)
		VALUES ('lease-old', 'vm-1', 'pool-a', 'default', 1, 2, 3)`); err != nil {
		t.Fatalf("insert lease error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if got, want := schemaVersion(t, s), len(migrations); got != want {
		t.Errorf("user_version = %d, want %d", got, want)
	}

	got, err := s.GetLease(ctx, "lease-old")
	if err != nil {
		t.Fatalf("GetLease() error = %v", err)
	}
	if got.GetVmUid() != "vm-1" || got.GetExpiresAt().AsTime().UnixNano() != 3 || got.GetRequestId() != "" {
		t.Errorf("GetLease() = %+v, want the pre-upgrade row with no request id", got)
	}

	l := sampleLeaseRecord("lease-new", "vm-2", "pool-a", "default", time.Unix(1_700_001_000, 0))
	l.RequestId = "req-1"
	if err := s.CreateLease(ctx, l); err != nil {
		t.Fatalf("CreateLease() after upgrade error = %v", err)
	}
	l.LeaseId = "lease-dup"
	if err := s.CreateLease(ctx, l); !errors.Is(err, ErrDuplicateRequestID) {
		t.Errorf("CreateLease() duplicate after upgrade error = %v, want ErrDuplicateRequestID", err)
	}
}

func TestOpenUpgradesVersionOneDB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "poolmgr.db")

	// Build a database as a binary with only migration 1 would have: the
	// six-column hosts table, user_version 1, and a cordoned host row.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatalf("apply baseline schema error = %v", err)
	}
	if _, err := db.Exec(migrations[0]); err != nil {
		t.Fatalf("apply migration 1 error = %v", err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 1;`); err != nil {
		t.Fatalf("set user_version error = %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO hosts (name, address, cordoned, cordoned_reason, cordoned_at, updated_at)
		VALUES ('host-old', 'old:8443', 1, 'maintenance', 5, 6)`); err != nil {
		t.Fatalf("insert host error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if got, want := schemaVersion(t, s), len(migrations); got != want {
		t.Errorf("user_version = %d, want %d", got, want)
	}

	got, err := s.GetHost(ctx, "host-old")
	if err != nil {
		t.Fatalf("GetHost() error = %v", err)
	}
	want := &poolmgrv1alpha1.Host{
		Name:           "host-old",
		Address:        "old:8443",
		Cordoned:       true,
		CordonedReason: "maintenance",
		CordonedAt:     timestamppb.New(time.Unix(0, 5)),
		UpdatedAt:      timestamppb.New(time.Unix(0, 6)),
		Tls:            &poolmgrv1alpha1.HostTLS{},
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetHost() = %+v, want the pre-upgrade row with default TLS %+v", got, want)
	}

	if err := s.CreateHost(ctx, sampleHost("host-new")); err != nil {
		t.Fatalf("CreateHost() after upgrade error = %v", err)
	}
}

func TestOpenMigratedDBIsNoop(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "poolmgr.db")

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	want := sampleLeaseRecord("lease-1", "vm-1", "pool-a", "default", time.Unix(1_700_001_000, 0))
	want.RequestId = "req-1"
	if err := s.CreateLease(ctx, want); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Reopening must not re-run migration 1: its ALTER TABLE would fail on
	// the column that already exists.
	s, err = Open(path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if got, want := schemaVersion(t, s), len(migrations); got != want {
		t.Errorf("user_version = %d, want %d", got, want)
	}
	got, err := s.GetLeaseByRequestID(ctx, "req-1")
	if err != nil {
		t.Fatalf("GetLeaseByRequestID() error = %v", err)
	}
	if !proto.Equal(got, want) {
		t.Errorf("GetLeaseByRequestID() = %+v, want %+v", got, want)
	}
}

func TestOpenRejectsNewerSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "poolmgr.db")

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA user_version = %d;`, len(migrations)+1)); err != nil {
		t.Fatalf("set user_version error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	s, err := Open(path)
	if err == nil {
		_ = s.Close()
		t.Fatal("Open() error = nil, want an error for a newer schema version")
	}
}
