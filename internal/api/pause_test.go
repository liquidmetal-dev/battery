package api_test

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/metrics"
)

// pausedAvailableVM is sampleAvailableVM recorded as paused, with the fake
// flintlock told it is paused too.
func pausedAvailableVM(fake *fakeMicroVM, uid, poolName string) *poolmgrv1alpha1.VMRecord {
	vm := sampleAvailableVM(uid, poolName)
	vm.Paused = true
	fake.paused = map[string]bool{uid: true}
	return vm
}

// refuseExecWhilePaused answers every exec with an error while fake thinks
// the VM is paused, as flintlock does, and counts the execs it answers.
func refuseExecWhilePaused(fake *fakeMicroVM, uid string, execs *atomic.Int32) *fakeMicroVMExec {
	return &fakeMicroVMExec{
		respond: func(*microvmexecv1alpha1.ExecStart) (int32, string) {
			execs.Add(1)
			if fake.isPaused(uid) {
				return 0, "microvm is paused"
			}
			return 0, ""
		},
	}
}

func claimPoolA(ctx context.Context, s *api.LeaseServer) (*poolmgrv1alpha1.ClaimVMResponse, error) {
	return s.ClaimVM(ctx, &poolmgrv1alpha1.ClaimVMRequest{Pool: &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}})
}

func TestClaimVM_PausedVM_ResumedBeforePreLeaseHooks(t *testing.T) {
	tests := []struct {
		name               string
		pauseWhenAvailable bool
		preLeaseCommands   []string
	}{
		{name: "with pre-lease hooks", pauseWhenAvailable: true, preLeaseCommands: []string{"do-thing"}},
		{name: "without pre-lease hooks", pauseWhenAvailable: true},
		// The VM's paused flag decides, not the pool's option: a VM paused
		// before the option was turned off still needs its resume.
		{name: "option turned off since the pause", preLeaseCommands: []string{"do-thing"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeMicroVM{}
			var execs atomic.Int32
			flint := startFakeFlintlock(t, fake, refuseExecWhilePaused(fake, "vm-1", &execs))
			st := openTestStore(t)
			ctx := context.Background()

			pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, tt.preLeaseCommands)
			pool.PauseWhenAvailable = tt.pauseWhenAvailable
			if err := st.CreatePool(ctx, pool); err != nil {
				t.Fatalf("CreatePool: %v", err)
			}
			if err := st.CreateVM(ctx, pausedAvailableVM(fake, "vm-1", "pool-a")); err != nil {
				t.Fatalf("CreateVM: %v", err)
			}

			reg := metrics.NewRegistry()
			s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, reg)
			if _, err := claimPoolA(ctx, s); err != nil {
				t.Fatalf("ClaimVM: %v", err)
			}

			if got := fake.resumedUIDs(); len(got) != 1 || got[0] != "vm-1" {
				t.Fatalf("ResumeMicroVM calls = %v, want one for vm-1", got)
			}
			if got, want := int(execs.Load()), len(tt.preLeaseCommands); got != want {
				t.Fatalf("execs = %d, want %d", got, want)
			}
			gotVM, err := st.GetVM(ctx, "vm-1")
			if err != nil {
				t.Fatalf("GetVM: %v", err)
			}
			if gotVM.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED {
				t.Fatalf("phase = %v, want LEASED", gotVM.GetPhase())
			}
			if gotVM.GetPaused() {
				t.Fatalf("paused = true, want false once the claim resumed the VM")
			}
			if body := scrapeMetrics(t, reg); !strings.Contains(body, `poolmgr_vm_power_change_duration_seconds_count{change="resume",pool_name="pool-a",pool_namespace="default"} 1`) {
				t.Fatalf("expected 1 resume duration observation, got:\n%s", body)
			}
		})
	}
}

// TestClaimVM_RunningVM_NotResumed covers a pool that has the option and a
// VM that is not paused, such as one provisioned before the option was
// turned on: the claim makes no ResumeMicroVM call, so it costs nothing.
func TestClaimVM_RunningVM_NotResumed(t *testing.T) {
	fake := &fakeMicroVM{}
	flint := startFakeFlintlock(t, fake, &fakeMicroVMExec{})
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, []string{"do-thing"})
	pool.PauseWhenAvailable = true
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, nil, nil)
	if _, err := claimPoolA(ctx, s); err != nil {
		t.Fatalf("ClaimVM: %v", err)
	}
	if got := fake.resumedUIDs(); len(got) != 0 {
		t.Fatalf("ResumeMicroVM calls = %v, want none", got)
	}
}

func TestClaimVM_ResumeFails_AppliesHookFailurePolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy poolmgrv1alpha1.HookFailurePolicy
	}{
		{name: "quarantine", policy: poolmgrv1alpha1.HookFailurePolicy_QUARANTINE},
		{name: "delete and replace", policy: poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeMicroVM{resumeErr: status.Error(codes.FailedPrecondition, "the vmm of microvm vm-1 is not running")}
			var execs atomic.Int32
			flint := startFakeFlintlock(t, fake, refuseExecWhilePaused(fake, "vm-1", &execs))
			st := openTestStore(t)
			ctx := context.Background()

			pool := samplePool("pool-a", tt.policy, []string{"do-thing"})
			pool.PauseWhenAvailable = true
			if err := st.CreatePool(ctx, pool); err != nil {
				t.Fatalf("CreatePool: %v", err)
			}
			if err := st.CreateVM(ctx, pausedAvailableVM(fake, "vm-1", "pool-a")); err != nil {
				t.Fatalf("CreateVM: %v", err)
			}

			notifier := &spyNotifier{}
			reg := metrics.NewRegistry()
			s := api.NewLeaseServer(st, flint, api.HookExecConfig{}, notifier, reg)
			_, err := claimPoolA(ctx, s)
			if status.Code(err) != codes.Internal {
				t.Fatalf("ClaimVM error = %v, want Internal", err)
			}
			if n := execs.Load(); n != 0 {
				t.Fatalf("execs = %d, want no pre-lease hook after a failed resume", n)
			}

			gotVM, err := st.GetVM(ctx, "vm-1")
			switch tt.policy {
			case poolmgrv1alpha1.HookFailurePolicy_QUARANTINE:
				if err != nil {
					t.Fatalf("GetVM: %v", err)
				}
				if gotVM.GetPhase() != poolmgrv1alpha1.VMPhase_QUARANTINED {
					t.Fatalf("phase = %v, want QUARANTINED", gotVM.GetPhase())
				}
			default:
				if err == nil {
					t.Fatalf("GetVM = %v, want the VM deleted", gotVM)
				}
				if got := fake.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
					t.Fatalf("DeleteMicroVM calls = %v, want one for vm-1", got)
				}
				if len(notifier.deleted) != 1 {
					t.Fatalf("NotifyVMDeleted calls = %v, want 1", notifier.deleted)
				}
			}

			if leases, err := st.ListLeases(ctx, nil); err != nil || len(leases) != 0 {
				t.Fatalf("ListLeases = %v, %v, want no lease", leases, err)
			}
			events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
			if err != nil {
				t.Fatalf("ListEventsSince: %v", err)
			}
			if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_HOOK_FAILED {
				t.Fatalf("events = %+v, want one VM_HOOK_FAILED", events)
			}
			if body := scrapeMetrics(t, reg); !strings.Contains(body, `poolmgr_hook_failures_total{hook="pre_lease",pool_name="pool-a",pool_namespace="default"} 1`) {
				t.Fatalf("expected 1 pre_lease hook failure recorded, got:\n%s", body)
			}
		})
	}
}
