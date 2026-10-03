package api_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func TestCreateGetPool(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	created, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec})
	if err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if got := created.GetStatus(); got.GetAvailableCount() != 0 || got.GetLeasedCount() != 0 ||
		got.GetProvisioningCount() != 0 || got.GetQuarantinedCount() != 0 {
		t.Errorf("CreatePool() status = %+v, want all-zero", got)
	}

	got, err := s.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	if got.GetSpec().GetName() != "pool-a" {
		t.Errorf("GetPool() name = %q, want pool-a", got.GetSpec().GetName())
	}
}

func TestCreatePoolForcesAllowGuestAgent(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	spec.MicrovmTemplate.AllowGuestAgent = false

	created, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec})
	if err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if !created.GetSpec().GetMicrovmTemplate().GetAllowGuestAgent() {
		t.Errorf("CreatePool() allow_guest_agent = false, want forced true")
	}

	got, err := s.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	if !got.GetSpec().GetMicrovmTemplate().GetAllowGuestAgent() {
		t.Errorf("GetPool() allow_guest_agent = false, want forced true (persisted)")
	}
}

// TestCreatePoolNilSpec confirms a nil Spec is rejected as InvalidArgument
// rather than panicking: spec.GetName()/GetNamespace() are nil-safe
// generated getters, so validatePoolSpec's first checks catch a nil spec
// before any code reaches a direct (non-getter) field access.
func TestCreatePoolNilSpec(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	_, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreatePool() with nil spec error = %v, want InvalidArgument", err)
	}
}

func TestCreatePoolValidation(t *testing.T) {
	base := func() *poolmgrv1alpha1.PoolSpec {
		return samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	}

	tests := []struct {
		name    string
		mutate  func(*poolmgrv1alpha1.PoolSpec)
		wantErr codes.Code
	}{
		{"empty name", func(s *poolmgrv1alpha1.PoolSpec) { s.Name = "" }, codes.InvalidArgument},
		{"empty namespace", func(s *poolmgrv1alpha1.PoolSpec) { s.Namespace = "" }, codes.InvalidArgument},
		{"unspecified strategy", func(s *poolmgrv1alpha1.PoolSpec) {
			s.ReplenishmentStrategy = &poolmgrv1alpha1.ReplenishmentStrategy{}
		}, codes.InvalidArgument},
		{"min_size_threshold with no min_size", func(s *poolmgrv1alpha1.PoolSpec) {
			s.ReplenishmentStrategy = &poolmgrv1alpha1.ReplenishmentStrategy{
				Type: poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
			}
		}, codes.InvalidArgument},
		{"unspecified hook failure policy", func(s *poolmgrv1alpha1.PoolSpec) {
			s.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy_HOOK_FAILURE_POLICY_UNSPECIFIED
		}, codes.InvalidArgument},
		{"unknown hook failure policy", func(s *poolmgrv1alpha1.PoolSpec) {
			s.HookFailurePolicy = poolmgrv1alpha1.HookFailurePolicy(99)
		}, codes.InvalidArgument},
		{"nil microvm template", func(s *poolmgrv1alpha1.PoolSpec) { s.MicrovmTemplate = nil }, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := openTestStore(t)
			s := api.NewPoolAdminServer(st, nil, nil)

			spec := base()
			tt.mutate(spec)

			_, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec})
			if status.Code(err) != tt.wantErr {
				t.Fatalf("CreatePool() error = %v, want code %v", err, tt.wantErr)
			}
		})
	}
}

func TestCreatePoolAlreadyExists(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	_, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)})
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("CreatePool() duplicate error = %v, want AlreadyExists", err)
	}
}

func TestGetUpdateDeletePoolNotFound(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	ref := &poolmgrv1alpha1.PoolRef{Name: "missing", Namespace: "default"}

	if _, err := s.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetPool() error = %v, want NotFound", err)
	}
	if _, err := s.UpdatePool(ctx, &poolmgrv1alpha1.UpdatePoolRequest{Spec: samplePool("missing", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)}); status.Code(err) != codes.NotFound {
		t.Errorf("UpdatePool() error = %v, want NotFound", err)
	}
	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("DeletePool() error = %v, want NotFound", err)
	}
}

func TestUpdatePool(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	update := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE, []string{"echo hi"})
	update.Size = 5
	update.HeartbeatInterval = durationpb.New(60 * time.Second)
	update.MicrovmTemplate.AllowGuestAgent = false

	got, err := s.UpdatePool(ctx, &poolmgrv1alpha1.UpdatePoolRequest{Spec: update})
	if err != nil {
		t.Fatalf("UpdatePool() error = %v", err)
	}
	if got.GetSpec().GetSize() != 5 {
		t.Errorf("UpdatePool() size = %d, want 5", got.GetSpec().GetSize())
	}
	if got.GetSpec().GetHookFailurePolicy() != poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE {
		t.Errorf("UpdatePool() hook_failure_policy = %v, want DELETE_AND_REPLACE", got.GetSpec().GetHookFailurePolicy())
	}
	if !got.GetSpec().GetMicrovmTemplate().GetAllowGuestAgent() {
		t.Errorf("UpdatePool() allow_guest_agent = false, want forced true")
	}
}

func TestListPoolsNamespaceFilter(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	a := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	b := samplePool("pool-b", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	b.Namespace = "other"

	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: a}); err != nil {
		t.Fatalf("CreatePool(a) error = %v", err)
	}
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: b}); err != nil {
		t.Fatalf("CreatePool(b) error = %v", err)
	}

	all, err := s.ListPools(ctx, &poolmgrv1alpha1.ListPoolsRequest{})
	if err != nil {
		t.Fatalf("ListPools() error = %v", err)
	}
	if len(all.GetPools()) != 2 {
		t.Fatalf("ListPools() len = %d, want 2", len(all.GetPools()))
	}

	ns := "default"
	filtered, err := s.ListPools(ctx, &poolmgrv1alpha1.ListPoolsRequest{Namespace: &ns})
	if err != nil {
		t.Fatalf("ListPools(namespace) error = %v", err)
	}
	if len(filtered.GetPools()) != 1 || filtered.GetPools()[0].GetSpec().GetName() != "pool-a" {
		t.Fatalf("ListPools(namespace=default) = %+v, want just pool-a", filtered.GetPools())
	}
}

func TestDeletePool(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	ref := &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}
	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: ref}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}
	if _, err := s.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: ref}); status.Code(err) != codes.NotFound {
		t.Errorf("GetPool() after delete error = %v, want NotFound", err)
	}
}

func TestGetPoolStatusCounts(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	vms := []*poolmgrv1alpha1.VMRecord{
		sampleAvailableVM("vm-available", "pool-a"),
		withPhase(sampleAvailableVM("vm-leased", "pool-a"), poolmgrv1alpha1.VMPhase_LEASED),
		withPhase(sampleAvailableVM("vm-provisioning", "pool-a"), poolmgrv1alpha1.VMPhase_PROVISIONING),
		withPhase(sampleAvailableVM("vm-quarantined", "pool-a"), poolmgrv1alpha1.VMPhase_QUARANTINED),
	}
	for _, vm := range vms {
		if err := st.CreateVM(ctx, vm); err != nil {
			t.Fatalf("CreateVM(%s) error = %v", vm.GetUid(), err)
		}
	}

	got, err := s.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	want := &poolmgrv1alpha1.PoolStatus{AvailableCount: 1, LeasedCount: 1, ProvisioningCount: 1, QuarantinedCount: 1}
	gotStatus := got.GetStatus()
	if gotStatus.GetAvailableCount() != want.GetAvailableCount() ||
		gotStatus.GetLeasedCount() != want.GetLeasedCount() ||
		gotStatus.GetProvisioningCount() != want.GetProvisioningCount() ||
		gotStatus.GetQuarantinedCount() != want.GetQuarantinedCount() {
		t.Errorf("GetPool() status = %+v, want %+v", gotStatus, want)
	}
}

func withPhase(vm *poolmgrv1alpha1.VMRecord, phase poolmgrv1alpha1.VMPhase) *poolmgrv1alpha1.VMRecord {
	vm.Phase = phase
	return vm
}

func TestCreatePool_StartsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 1 || lifecycle.started[0] != "pool-a" {
		t.Fatalf("started = %v, want [pool-a]", lifecycle.started)
	}
}

func TestCreatePool_ValidationFailure_DoesNotStartReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, nil, lifecycle)

	spec := samplePool("", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil) // empty name is invalid
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err == nil {
		t.Fatal("expected CreatePool to fail validation")
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 0 {
		t.Fatalf("started = %v, want none", lifecycle.started)
	}
}

func TestDeletePool_StopsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 1 || lifecycle.stopped[0] != "pool-a" {
		t.Fatalf("stopped = %v, want [pool-a]", lifecycle.stopped)
	}
}

func TestUpdatePool_RestartsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	update := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	update.Size = 5
	if _, err := s.UpdatePool(ctx, &poolmgrv1alpha1.UpdatePoolRequest{Spec: update}); err != nil {
		t.Fatalf("UpdatePool() error = %v", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 2 || lifecycle.started[0] != "pool-a" || lifecycle.started[1] != "pool-a" {
		t.Fatalf("started = %v, want [pool-a pool-a]", lifecycle.started)
	}
	if len(lifecycle.stopped) != 1 || lifecycle.stopped[0] != "pool-a" {
		t.Fatalf("stopped = %v, want [pool-a]", lifecycle.stopped)
	}
}

// pausingStore wraps a store.Store and, on its first UpdatePool call only,
// blocks after the underlying write completes until resume is closed. Used
// to force a controlled window between a concurrent UpdatePool RPC's store
// write and its PoolLifecycle transition, to prove per-pool serialization
// (see TestUpdatePool_ConcurrentUpdates_Serialized).
type pausingStore struct {
	store.Store
	once   sync.Once
	paused chan struct{}
	resume chan struct{}
}

func (p *pausingStore) UpdatePool(ctx context.Context, spec *poolmgrv1alpha1.PoolSpec) error {
	if err := p.Store.UpdatePool(ctx, spec); err != nil {
		return err
	}
	p.once.Do(func() {
		close(p.paused)
		<-p.resume
	})
	return nil
}

// TestUpdatePool_ConcurrentUpdates_Serialized reproduces the race from PR
// review: without per-pool serialization, two concurrent UpdatePool calls
// for the same pool can persist their specs in one order but call
// StopReconciler/StartReconciler in the other order, leaving the store and
// the live reconciler permanently disagreeing about which spec is current.
// This forces update A to pause between its store write and its lifecycle
// transition, then asserts update B cannot complete (or even reach its own
// store write) while A holds the pool's lock - proving the two can never
// interleave - and that the store and the reconciler's last-started spec
// agree once both finish.
func TestUpdatePool_ConcurrentUpdates_Serialized(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	setup := api.NewPoolAdminServer(st, nil, lifecycle)
	if _, err := setup.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	ps := &pausingStore{Store: st, paused: make(chan struct{}), resume: make(chan struct{})}
	s := api.NewPoolAdminServer(ps, nil, lifecycle)

	specA := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	specA.Size = 3
	specB := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	specB.Size = 5

	doneA := make(chan error, 1)
	go func() {
		_, err := s.UpdatePool(ctx, &poolmgrv1alpha1.UpdatePoolRequest{Spec: specA})
		doneA <- err
	}()

	select {
	case <-ps.paused:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update A to pause after its store write")
	}

	doneB := make(chan error, 1)
	go func() {
		_, err := s.UpdatePool(ctx, &poolmgrv1alpha1.UpdatePoolRequest{Spec: specB})
		doneB <- err
	}()

	select {
	case err := <-doneB:
		t.Fatalf("UpdatePool B returned (err=%v) while A was still paused mid-transition - not serialized", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(ps.resume)

	if err := <-doneA; err != nil {
		t.Fatalf("UpdatePool A: %v", err)
	}
	if err := <-doneB; err != nil {
		t.Fatalf("UpdatePool B: %v", err)
	}

	final, err := st.GetPool(ctx, "pool-a", "default")
	if err != nil {
		t.Fatalf("GetPool: %v", err)
	}
	if final.GetSize() != specB.GetSize() {
		t.Fatalf("store spec.Size = %d, want %d (B, the last update to complete)", final.GetSize(), specB.GetSize())
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 3 {
		t.Fatalf("started = %v, want 3 entries (create, A's restart, B's restart)", lifecycle.started)
	}
}

func TestCreatePool_StartReconcilerFails_StillReturnsSuccess(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{startErr: errors.New("boom")}
	s := api.NewPoolAdminServer(st, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	got, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec})
	if err != nil {
		t.Fatalf("CreatePool() error = %v, want nil despite StartReconciler failure", err)
	}
	if got == nil {
		t.Fatal("CreatePool() returned nil response, want non-nil despite StartReconciler failure")
	}
}

// newDeletePoolFixture creates pool-a in a fresh store, served by a
// PoolAdminServer wired to a fake flintlock and a recording lifecycle.
func newDeletePoolFixture(t *testing.T) (*api.PoolAdminServer, store.Store, *fakeMicroVM, *fakePoolLifecycle) {
	t.Helper()

	st := openTestStore(t)
	fakeVM := &fakeMicroVM{}
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, startFakeFlintlock(t, fakeVM, &fakeMicroVMExec{}), lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(context.Background(), &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	return s, st, fakeVM, lifecycle
}

// leasedVMWithLease stores a LEASED VM in pool-a together with its lease row.
func leasedVMWithLease(t *testing.T, st store.Store, uid, leaseID string) {
	t.Helper()
	ctx := context.Background()

	vm := withPhase(sampleAvailableVM(uid, "pool-a"), poolmgrv1alpha1.VMPhase_LEASED)
	vm.LeaseId = proto.String(leaseID)
	if err := st.CreateVM(ctx, vm); err != nil {
		t.Fatalf("CreateVM(%s) error = %v", uid, err)
	}
	now := time.Now()
	if err := st.CreateLease(ctx, &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         leaseID,
		VmUid:           uid,
		PoolName:        "pool-a",
		PoolNamespace:   "default",
		ClaimedAt:       timestamppb.New(now),
		LastHeartbeatAt: timestamppb.New(now),
		ExpiresAt:       timestamppb.New(now.Add(time.Hour)),
	}); err != nil {
		t.Fatalf("CreateLease(%s) error = %v", leaseID, err)
	}
}

var poolARef = &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}

func TestDeletePool_DrainsUnleasedVMs(t *testing.T) {
	// Every phase other than the two leased ones is drained without force,
	// including DELETING and FAILED, which CountVMs doesn't tally.
	phases := []poolmgrv1alpha1.VMPhase{
		poolmgrv1alpha1.VMPhase_PROVISIONING,
		poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING,
		poolmgrv1alpha1.VMPhase_AVAILABLE,
		poolmgrv1alpha1.VMPhase_DELETING,
		poolmgrv1alpha1.VMPhase_QUARANTINED,
		poolmgrv1alpha1.VMPhase_FAILED,
	}

	ctx := context.Background()
	s, st, fakeVM, lifecycle := newDeletePoolFixture(t)

	var wantUIDs []string
	for _, phase := range phases {
		uid := "vm-" + phase.String()
		wantUIDs = append(wantUIDs, uid)
		if err := st.CreateVM(ctx, withPhase(sampleAvailableVM(uid, "pool-a"), phase)); err != nil {
			t.Fatalf("CreateVM(%s) error = %v", uid, err)
		}
	}
	slices.Sort(wantUIDs)

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}

	if _, err := s.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: poolARef}); status.Code(err) != codes.NotFound {
		t.Errorf("GetPool() after delete error = %v, want NotFound", err)
	}
	remaining, err := st.ListVMsByPool(ctx, "pool-a", "default", nil)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("VM rows left after delete = %+v, want none", remaining)
	}

	gotDeleted := fakeVM.deletedUIDs()
	slices.Sort(gotDeleted)
	if !slices.Equal(gotDeleted, wantUIDs) {
		t.Errorf("flintlock deleted uids = %v, want %v", gotDeleted, wantUIDs)
	}

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	var gotEventUIDs []string
	for _, e := range events {
		if e.GetType() != poolmgrv1alpha1.EventType_VM_DELETED_ON_POOL_DELETE {
			t.Errorf("event type = %v, want VM_DELETED_ON_POOL_DELETE", e.GetType())
		}
		gotEventUIDs = append(gotEventUIDs, e.GetVmUid())
	}
	slices.Sort(gotEventUIDs)
	if !slices.Equal(gotEventUIDs, wantUIDs) {
		t.Errorf("event vm uids = %v, want %v", gotEventUIDs, wantUIDs)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 1 || lifecycle.stopped[0] != "pool-a" {
		t.Errorf("stopped = %v, want [pool-a]", lifecycle.stopped)
	}
	if len(lifecycle.started) != 1 {
		t.Errorf("started = %v, want only the CreatePool start", lifecycle.started)
	}
}

func TestDeletePool_LeasedVMWithoutForce_Refused(t *testing.T) {
	ctx := context.Background()
	s, st, fakeVM, lifecycle := newDeletePoolFixture(t)

	leasedVMWithLease(t, st, "vm-leased", "lease-1")
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-available", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeletePool() error = %v, want FailedPrecondition", err)
	}

	if _, err := st.GetPool(ctx, "pool-a", "default"); err != nil {
		t.Errorf("GetPool() after refusal error = %v, want the pool still present", err)
	}
	if vm, err := st.GetVM(ctx, "vm-available"); err != nil || vm.GetPhase() != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Errorf("vm-available after refusal = %+v, err %v, want AVAILABLE", vm, err)
	}
	if _, err := st.GetLease(ctx, "lease-1"); err != nil {
		t.Errorf("GetLease() after refusal error = %v, want the lease still present", err)
	}
	if got := fakeVM.deletedUIDs(); len(got) != 0 {
		t.Errorf("flintlock deleted uids = %v, want none", got)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 0 {
		t.Errorf("stopped = %v, want none", lifecycle.stopped)
	}
}

func TestDeletePool_PreLeaseHookRunningWithoutForce_Refused(t *testing.T) {
	ctx := context.Background()
	s, st, _, _ := newDeletePoolFixture(t)

	// A ClaimVM that has claimed its VM but not yet created the lease row.
	if err := st.CreateVM(ctx, withPhase(sampleAvailableVM("vm-claiming", "pool-a"), poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeletePool() error = %v, want FailedPrecondition", err)
	}
}

func TestDeletePool_LeasedVMWithForce_DeletesVMAndLease(t *testing.T) {
	ctx := context.Background()
	s, st, fakeVM, _ := newDeletePoolFixture(t)

	leasedVMWithLease(t, st, "vm-leased", "lease-1")

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef, Force: true}); err != nil {
		t.Fatalf("DeletePool(force) error = %v", err)
	}

	if _, err := st.GetPool(ctx, "pool-a", "default"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetPool() error = %v, want ErrNotFound", err)
	}
	if _, err := st.GetVM(ctx, "vm-leased"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetVM() error = %v, want ErrNotFound", err)
	}
	if _, err := st.GetLease(ctx, "lease-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetLease() error = %v, want ErrNotFound", err)
	}
	if got := fakeVM.deletedUIDs(); len(got) != 1 || got[0] != "vm-leased" {
		t.Errorf("flintlock deleted uids = %v, want [vm-leased]", got)
	}
}

func TestDeletePool_FlintlockDeleteFails_SweeperFinishes(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	fakeVM := &fakeMicroVM{deleteErr: status.Error(codes.Unavailable, "host down")}
	flint := startFakeFlintlock(t, fakeVM, &fakeMicroVMExec{})
	s := api.NewPoolAdminServer(st, flint, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v, want nil despite the flintlock failure", err)
	}
	if _, err := st.GetPool(ctx, "pool-a", "default"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetPool() error = %v, want ErrNotFound", err)
	}
	vm, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v, want the row kept for retry", err)
	}
	if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Fatalf("vm-1 phase = %v, want DELETING", vm.GetPhase())
	}

	// The host comes back: the sweeper's pending-deletion retry finishes the
	// job even though the VM's pool no longer exists.
	fakeVM.mu.Lock()
	fakeVM.deleteErr = nil
	fakeVM.mu.Unlock()

	reconciler.NewSweeper(st, flint, 0, 0, nil, nil).Tick(ctx, time.Now())

	if _, err := st.GetVM(ctx, "vm-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetVM() after sweep error = %v, want ErrNotFound", err)
	}
	if got := fakeVM.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
		t.Errorf("flintlock deleted uids = %v, want [vm-1]", got)
	}
}

func TestDeletePool_NilFlintlockLeavesVMsDeleting(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}
	vm, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Errorf("vm-1 phase = %v, want DELETING", vm.GetPhase())
	}
}

// tombstoneHookStore wraps a store.Store to intercept DeletePoolAndMarkVMs.
// If refuse is set the call returns that error without touching the store;
// otherwise before (if set) runs, then the real call, and after is invoked
// once it has succeeded.
type tombstoneHookStore struct {
	store.Store
	refuse error
	before func()
	after  func()
}

func (h *tombstoneHookStore) DeletePoolAndMarkVMs(ctx context.Context, name, namespace string, force bool) ([]*poolmgrv1alpha1.VMRecord, error) {
	if h.refuse != nil {
		return nil, h.refuse
	}
	if h.before != nil {
		h.before()
	}
	vms, err := h.Store.DeletePoolAndMarkVMs(ctx, name, namespace, force)
	if err == nil && h.after != nil {
		h.after()
	}
	return vms, err
}

func TestDeletePool_ClaimRacesIn_RestartsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	// The pre-check sees no leased VM, then the tombstone finds one: a
	// ClaimVM landed in between.
	hooked := &tombstoneHookStore{Store: st, refuse: store.ErrPoolHasLeasedVMs}
	s := api.NewPoolAdminServer(hooked, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeletePool() error = %v, want FailedPrecondition", err)
	}
	if _, err := st.GetPool(ctx, "pool-a", "default"); err != nil {
		t.Errorf("GetPool() error = %v, want the pool still present", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 1 {
		t.Errorf("stopped = %v, want [pool-a]", lifecycle.stopped)
	}
	if len(lifecycle.started) != 2 {
		t.Errorf("started = %v, want 2 entries (create, restart after the refusal)", lifecycle.started)
	}
}

func TestDeletePool_StopWaitFails_RestartsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{stopWaitErr: context.DeadlineExceeded}
	s := api.NewPoolAdminServer(st, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("DeletePool() error = %v, want DeadlineExceeded", err)
	}
	if _, err := st.GetPool(ctx, "pool-a", "default"); err != nil {
		t.Errorf("GetPool() error = %v, want the pool still present", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 2 {
		t.Errorf("started = %v, want 2 entries (create, restart after the failed wait)", lifecycle.started)
	}
}

func TestDeletePool_ClientCancelStillDeletesVMs(t *testing.T) {
	st := openTestStore(t)
	fakeVM := &fakeMicroVM{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The client goes away the moment the tombstone has committed.
	hooked := &tombstoneHookStore{Store: st, after: cancel}
	s := api.NewPoolAdminServer(hooked, startFakeFlintlock(t, fakeVM, &fakeMicroVMExec{}), nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}

	if got := fakeVM.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
		t.Errorf("flintlock deleted uids = %v, want [vm-1]", got)
	}
	if _, err := st.GetVM(context.Background(), "vm-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetVM() error = %v, want ErrNotFound", err)
	}
}

// TestDeletePool_HungHostDoesNotStarveOtherVMs: one unresponsive flintlock
// host must not hold up the pool's other VMs, nor cost them their
// VM_DELETED_ON_POOL_DELETE event.
func TestDeletePool_HungHostDoesNotStarveOtherVMs(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	gate := make(chan struct{})
	fakeVM := &fakeMicroVM{hangUID: "vm-1", hangGate: gate}
	s := api.NewPoolAdminServer(st, startFakeFlintlock(t, fakeVM, &fakeMicroVMExec{}), nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	for _, uid := range []string{"vm-1", "vm-2", "vm-3"} {
		if err := st.CreateVM(ctx, sampleAvailableVM(uid, "pool-a")); err != nil {
			t.Fatalf("CreateVM(%s) error = %v", uid, err)
		}
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
		done <- err
	}()

	// While vm-1's delete is still hanging, the other two are deleted and
	// all three have their event.
	deadline := time.Now().Add(5 * time.Second)
	for {
		deleted := fakeVM.deletedUIDs()
		slices.Sort(deleted)
		events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
		if err != nil {
			t.Fatalf("ListEventsSince() error = %v", err)
		}
		if slices.Equal(deleted, []string{"vm-2", "vm-3"}) && len(events) == 3 {
			break
		}
		if time.Now().After(deadline) {
			close(gate)
			t.Fatalf("with vm-1 hanging: flintlock deleted %v, want [vm-2 vm-3]; %d events, want 3", deleted, len(events))
		}
		time.Sleep(10 * time.Millisecond)
	}

	close(gate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("DeletePool() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for DeletePool to return")
	}
	if remaining, err := st.ListVMsByPool(ctx, "pool-a", "default", nil); err != nil || len(remaining) != 0 {
		t.Errorf("VM rows left = %+v, err %v, want none", remaining, err)
	}
}

// TestDeletePool_CancelledDuringTombstone_ReportsCancelled: a client that
// goes away before the tombstone commits gets CANCELLED, not INTERNAL, and
// the pool is left as it was with its reconciler running again.
func TestDeletePool_CancelledDuringTombstone_ReportsCancelled(t *testing.T) {
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hooked := &tombstoneHookStore{Store: st, before: cancel}
	s := api.NewPoolAdminServer(hooked, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("DeletePool() error = %v, want Canceled", err)
	}
	if _, err := st.GetPool(context.Background(), "pool-a", "default"); err != nil {
		t.Errorf("GetPool() error = %v, want the pool still present", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 2 {
		t.Errorf("started = %v, want 2 entries (create, restart after the cancelled delete)", lifecycle.started)
	}
}

// TestDeletePool_VMRowAlreadyGone_NoFailureLogged: the Sweeper retries
// DELETING VMs on its own schedule, so it can finish one between the
// tombstone and DeletePool's inline delete. That VM is deleted, which is the
// outcome DeletePool wanted, so it must not be logged as a failed delete.
func TestDeletePool_VMRowAlreadyGone_NoFailureLogged(t *testing.T) {
	var logs bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(orig) })

	ctx := context.Background()
	st := openTestStore(t)
	fakeVM := &fakeMicroVM{}
	hooked := &tombstoneHookStore{Store: st, after: func() {
		if err := st.DeleteVM(ctx, "vm-1"); err != nil {
			t.Errorf("DeleteVM() error = %v", err)
		}
	}}
	s := api.NewPoolAdminServer(hooked, startFakeFlintlock(t, fakeVM, &fakeMicroVMExec{}), nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}

	if strings.Contains(logs.String(), "microvm delete failed") {
		t.Errorf("DeletePool logged a failed delete for a VM that was already gone:\n%s", logs.String())
	}
}
