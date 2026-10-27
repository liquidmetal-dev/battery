package reconciler

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// drainDeleteTimeout bounds each of DeleteDrainedVMs' flintlock deletes,
// which run detached from the caller's cancellation, and its event writes.
const drainDeleteTimeout = 30 * time.Second

// DeleteDrainedVMs emits VM_DELETED_ON_HOST_DRAIN for, and deletes, each of
// vms, which a host drain (store.DrainHost or
// store.MarkDrainingHostsVMsDeleting) has just marked DELETING. It follows
// DeletePool's deletion of a pool's VMs:
//
//   - It runs on a context detached from ctx's cancellation. The VMs are
//     already DELETING, so a caller that goes away must not stop them being
//     deleted.
//   - Every event is written before any flintlock call, and the deletes run
//     concurrently, each bounded by drainDeleteTimeout, so one unresponsive
//     host cannot hold up the rest.
//   - A failed delete is only logged. The VM stays DELETING, and the
//     Sweeper's pending-deletion scan retries it.
//
// notifier (nil-safe) is told of each VM deleted, so its pool can refill on
// another host straight away rather than on its next tick.
func DeleteDrainedVMs(ctx context.Context, st store.Store, flint *flintlockclient.Pool, vms []*poolmgrv1alpha1.VMRecord, notifier DeletionNotifier) {
	if len(vms) == 0 {
		return
	}
	ctx = context.WithoutCancel(ctx)

	eventCtx, cancel := context.WithTimeout(ctx, drainDeleteTimeout)
	for _, vm := range vms {
		EmitEvent(eventCtx, st, vmPool(vm), vm.GetUid(), poolmgrv1alpha1.EventType_VM_DELETED_ON_HOST_DRAIN)
	}
	cancel()

	if flint == nil {
		return
	}

	var wg sync.WaitGroup
	for _, vm := range vms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			log := slog.Default().With("pool", vm.GetPoolName(), "namespace", vm.GetPoolNamespace(),
				"microvm_uid", vm.GetUid(), "flintlock_host", vm.GetFlintlockHost())

			deleteCtx, cancel := context.WithTimeout(ctx, drainDeleteTimeout)
			defer cancel()
			err := EnsureVMDeleted(deleteCtx, st, flint, vm)
			switch {
			case err == nil:
				if notifier != nil {
					notifier.NotifyVMDeleted(vm.GetPoolName(), vm.GetPoolNamespace())
				}
			case errors.Is(err, store.ErrNotFound):
				// The Sweeper's pending-deletion scan got to this DELETING
				// VM first, and accounts for it.
			default:
				log.WarnContext(ctx, "reconciler: drain: microvm delete failed, leaving it for the sweeper", "error", err)
			}
		}()
	}
	wg.Wait()
}

// vmPool returns a PoolSpec that names vm's pool, for EmitEvent, which reads
// only the name and namespace.
func vmPool(vm *poolmgrv1alpha1.VMRecord) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{Name: vm.GetPoolName(), Namespace: vm.GetPoolNamespace()}
}
