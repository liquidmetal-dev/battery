package reconciler

import (
	"context"
	"fmt"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
)

// powerChangePause/powerChangeResume label
// poolmgr_vm_power_change_duration_seconds.
const (
	powerChangePause  = "pause"
	powerChangeResume = "resume"
)

// powerChangeTimeout bounds one PauseMicroVM or ResumeMicroVM call, so an
// unresponsive flintlockd can't hold up a Provision or a claim for good.
// flintlock answers both once the VMM has made the change, which takes
// milliseconds. A var so tests can shorten it.
var powerChangeTimeout = 10 * time.Second

// PauseVM pauses vm's microvm in flintlock. It does not change vm or its
// store row: the caller records vm.Paused with the write that follows.
// Pausing a paused microvm succeeds, so a retry is safe.
func PauseVM(ctx context.Context, flint *flintlockclient.Pool, vm *poolmgrv1alpha1.VMRecord, m *metrics.Registry) error {
	return changePower(ctx, flint, vm, powerChangePause, m)
}

// ResumeVM resumes vm's microvm in flintlock. It does not change vm or its
// store row: the caller clears vm.Paused with the write that follows.
// Resuming a running microvm succeeds, so a retry is safe.
func ResumeVM(ctx context.Context, flint *flintlockclient.Pool, vm *poolmgrv1alpha1.VMRecord, m *metrics.Registry) error {
	return changePower(ctx, flint, vm, powerChangeResume, m)
}

func changePower(ctx context.Context, flint *flintlockclient.Pool, vm *poolmgrv1alpha1.VMRecord, change string, m *metrics.Registry) error {
	client, err := flint.Client(vm.GetFlintlockHost())
	if err != nil {
		return fmt.Errorf("reconciler: %s microvm: %w", change, err)
	}

	ctx, cancel := context.WithTimeout(ctx, powerChangeTimeout)
	defer cancel()

	start := time.Now()
	switch change {
	case powerChangePause:
		_, err = client.PauseMicroVM(ctx, &microvmv1alpha1.PauseMicroVMRequest{Uid: vm.GetUid()})
	default:
		_, err = client.ResumeMicroVM(ctx, &microvmv1alpha1.ResumeMicroVMRequest{Uid: vm.GetUid()})
	}
	if err != nil {
		return fmt.Errorf("reconciler: %s microvm: %w", change, err)
	}
	if m != nil {
		m.ObservePowerChangeDuration(change, vm.GetPoolName(), vm.GetPoolNamespace(), time.Since(start))
	}
	return nil
}
