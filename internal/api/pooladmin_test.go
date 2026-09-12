package api_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func TestCreateGetPool(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil)

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
	s := api.NewPoolAdminServer(st, nil)

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
	s := api.NewPoolAdminServer(st, nil)

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
			s := api.NewPoolAdminServer(st, nil)

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
	s := api.NewPoolAdminServer(st, nil)

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
	s := api.NewPoolAdminServer(st, nil)

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
	s := api.NewPoolAdminServer(st, nil)

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
	s := api.NewPoolAdminServer(st, nil)

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
	s := api.NewPoolAdminServer(st, nil)

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

func TestDeletePoolBlockedWithVMs(t *testing.T) {
	// CountVMs only tallies AVAILABLE/LEASED/PROVISIONING/QUARANTINED, so
	// DeletePool's guard must not be built on top of it - a pool whose only
	// VM is DELETING or FAILED must still be blocked.
	phases := []poolmgrv1alpha1.VMPhase{
		poolmgrv1alpha1.VMPhase_AVAILABLE,
		poolmgrv1alpha1.VMPhase_DELETING,
		poolmgrv1alpha1.VMPhase_FAILED,
	}

	for _, phase := range phases {
		t.Run(phase.String(), func(t *testing.T) {
			ctx := context.Background()
			st := openTestStore(t)
			s := api.NewPoolAdminServer(st, nil)

			spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
			if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
				t.Fatalf("CreatePool() error = %v", err)
			}
			if err := st.CreateVM(ctx, withPhase(sampleAvailableVM("vm-1", "pool-a"), phase)); err != nil {
				t.Fatalf("CreateVM() error = %v", err)
			}

			ref := &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}
			_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: ref})
			if status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("DeletePool() with VM in phase %s error = %v, want FailedPrecondition", phase, err)
			}
		})
	}
}

func TestGetPoolStatusCounts(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil)

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

// stubAutoscalerLifecycle is a PoolLifecycle whose AutoscalerSnapshot returns fixed values,
// for asserting that PoolAdminServer surfaces them in PoolStatus.
type stubAutoscalerLifecycle struct {
	fakePoolLifecycle
	claimsPerSec float64
	lastScaledAt time.Time
}

func (s *stubAutoscalerLifecycle) AutoscalerSnapshot(string, string) (float64, time.Time, bool) {
	return s.claimsPerSec, s.lastScaledAt, true
}

func TestGetPoolStatus_SurfacesAutoscalerSnapshot(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lastScaledAt := time.Now().Truncate(time.Second)
	lifecycle := &stubAutoscalerLifecycle{claimsPerSec: 1.5, lastScaledAt: lastScaledAt}
	s := api.NewPoolAdminServer(st, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	got, err := s.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}

	status := got.GetStatus()
	if status.GetObservedClaimsPerSec() != 1.5 {
		t.Errorf("GetPool() observed_claims_per_sec = %v, want 1.5", status.GetObservedClaimsPerSec())
	}
	if !status.GetLastScaledAt().AsTime().Equal(lastScaledAt) {
		t.Errorf("GetPool() last_scaled_at = %v, want %v", status.GetLastScaledAt().AsTime(), lastScaledAt)
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
	s := api.NewPoolAdminServer(st, lifecycle)

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
	s := api.NewPoolAdminServer(st, lifecycle)

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
	s := api.NewPoolAdminServer(st, lifecycle)

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
	s := api.NewPoolAdminServer(st, lifecycle)

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
	setup := api.NewPoolAdminServer(st, lifecycle)
	if _, err := setup.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	ps := &pausingStore{Store: st, paused: make(chan struct{}), resume: make(chan struct{})}
	s := api.NewPoolAdminServer(ps, lifecycle)

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
	s := api.NewPoolAdminServer(st, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	got, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec})
	if err != nil {
		t.Fatalf("CreatePool() error = %v, want nil despite StartReconciler failure", err)
	}
	if got == nil {
		t.Fatal("CreatePool() returned nil response, want non-nil despite StartReconciler failure")
	}
}

func TestDeletePool_VMsStillPresent_DoesNotStopReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}}); err == nil {
		t.Fatal("expected DeletePool to fail with VMs still present")
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 0 {
		t.Fatalf("stopped = %v, want none", lifecycle.stopped)
	}
}
