package api_test

import (
	"context"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/liquidmetal-dev/battery/internal/api"
)

func TestCreateGetPool(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st)

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
	s := api.NewPoolAdminServer(st)

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
		{"nil microvm template", func(s *poolmgrv1alpha1.PoolSpec) { s.MicrovmTemplate = nil }, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := openTestStore(t)
			s := api.NewPoolAdminServer(st)

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
	s := api.NewPoolAdminServer(st)

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
	s := api.NewPoolAdminServer(st)

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
	s := api.NewPoolAdminServer(st)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	update := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE, []string{"echo hi"})
	update.Size = 5
	update.HeartbeatInterval = durationpb.New(60)
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
	s := api.NewPoolAdminServer(st)

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
	s := api.NewPoolAdminServer(st)

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
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	ref := &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}
	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: ref})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeletePool() with VMs error = %v, want FailedPrecondition", err)
	}
}

func TestGetPoolStatusCounts(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st)

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
