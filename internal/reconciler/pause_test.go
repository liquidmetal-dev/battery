package reconciler_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
)

func TestProvision_PauseWhenAvailable_PausesAfterCreateCommands(t *testing.T) {
	vm := &fakeMicroVM{}
	var pausedDuringHooks bool
	exec := &fakeMicroVMExec{
		respond: func(*microvmexecv1alpha1.ExecStart) ([]byte, []byte, int32, string, error) {
			// The guest agent can't answer a paused VM, so every exec,
			// the readiness probe included, must come before the pause.
			if len(vm.pausedUIDs()) > 0 {
				pausedDuringHooks = true
			}
			return nil, nil, 0, "", nil
		},
	}
	flint := startFakeFlintlock(t, vm, exec)
	st := openTestStoreWithHost(t, "host-a")

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
	pool.CreateCommands = []string{"echo hi"}
	pool.PauseWhenAvailable = true

	reg := metrics.NewRegistry()
	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), reg)
	if err := p.Provision(context.Background(), pool); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 {
		t.Fatalf("expected 1 VM record, got %d", len(vms))
	}
	if got := vms[0].GetPhase(); got != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Fatalf("phase = %v, want AVAILABLE", got)
	}
	if !vms[0].GetPaused() {
		t.Fatalf("paused = false, want the AVAILABLE VM recorded as paused")
	}
	if got := vm.pausedUIDs(); len(got) != 1 || got[0] != vms[0].GetUid() {
		t.Fatalf("PauseMicroVM calls = %v, want one for %s", got, vms[0].GetUid())
	}
	if pausedDuringHooks {
		t.Fatalf("the VM was paused before its create commands finished")
	}
	if body := scrapeMetrics(t, reg); !strings.Contains(body, `poolmgr_vm_power_change_duration_seconds_count{change="pause",pool_name="pool-a",pool_namespace="default"} 1`) {
		t.Fatalf("expected 1 pause duration observation, got:\n%s", body)
	}
}

func TestProvision_WithoutPauseWhenAvailable_DoesNotPause(t *testing.T) {
	vm := &fakeMicroVM{}
	flint := startFakeFlintlock(t, vm, alwaysReadyExec())
	st := openTestStoreWithHost(t, "host-a")

	pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})

	p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), nil)
	if err := p.Provision(context.Background(), pool); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	if got := vm.pausedUIDs(); len(got) != 0 {
		t.Fatalf("PauseMicroVM calls = %v, want none", got)
	}
	vms := onlyVMsInPool(t, st, "pool-a")
	if len(vms) != 1 || vms[0].GetPaused() {
		t.Fatalf("VMs = %v, want one VM not recorded as paused", vms)
	}
}

func TestProvision_PauseFails_AppliesHookFailurePolicy(t *testing.T) {
	tests := []struct {
		name   string
		policy poolmgrv1alpha1.HookFailurePolicy
	}{
		{name: "delete and replace", policy: poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE},
		{name: "quarantine", policy: poolmgrv1alpha1.HookFailurePolicy_QUARANTINE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vm := &fakeMicroVM{pauseErr: status.Error(codes.FailedPrecondition, "microvm is not running")}
			flint := startFakeFlintlock(t, vm, alwaysReadyExec())
			st := openTestStoreWithHost(t, "host-a")

			pool := samplePool("pool-a", poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD, 1, []string{"host-a"})
			pool.PauseWhenAvailable = true
			pool.HookFailurePolicy = tt.policy

			reg := metrics.NewRegistry()
			p := reconciler.NewProvisioner(st, flint, fastProvisionConfig(), reg)
			err := p.Provision(context.Background(), pool)
			if !errors.Is(err, reconciler.ErrPauseFailed) {
				t.Fatalf("Provision() error = %v, want ErrPauseFailed", err)
			}

			vms := onlyVMsInPool(t, st, "pool-a")
			switch tt.policy {
			case poolmgrv1alpha1.HookFailurePolicy_QUARANTINE:
				if len(vms) != 1 || vms[0].GetPhase() != poolmgrv1alpha1.VMPhase_QUARANTINED {
					t.Fatalf("VMs = %v, want one QUARANTINED VM", vms)
				}
				if vms[0].GetPaused() {
					t.Fatalf("paused = true, want false: the pause failed")
				}
			default:
				if len(vms) != 0 {
					t.Fatalf("VMs = %v, want none after DELETE_AND_REPLACE", vms)
				}
				if got := vm.deletedUIDs(); len(got) != 1 {
					t.Fatalf("DeleteMicroVM calls = %v, want 1", got)
				}
			}

			events, err := st.ListEventsSince(context.Background(), "pool-a", "default", 0, 100)
			if err != nil {
				t.Fatalf("ListEventsSince: %v", err)
			}
			for _, e := range events {
				if e.GetType() == poolmgrv1alpha1.EventType_VM_AVAILABLE {
					t.Fatalf("got VM_AVAILABLE for a VM whose pause failed: %+v", events)
				}
			}
			if len(events) == 0 || events[len(events)-1].GetType() != poolmgrv1alpha1.EventType_VM_HOOK_FAILED {
				t.Fatalf("events = %+v, want VM_HOOK_FAILED last", events)
			}
			if body := scrapeMetrics(t, reg); !strings.Contains(body, `poolmgr_hook_failures_total{hook="create",pool_name="pool-a",pool_namespace="default"} 1`) {
				t.Fatalf("expected 1 create hook failure recorded, got:\n%s", body)
			}
		})
	}
}
