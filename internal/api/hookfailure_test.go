package api_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmexecv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvmexec/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/liquidmetal-dev/battery/internal/api"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// TestClaimVM_PreLeaseHookFailureDeleteNotConfirmed_LeftDeleting: when a
// pre-lease hook fails under DELETE_AND_REPLACE and flintlock doesn't
// confirm the delete (it fails, or hangs past the cleanup timeout), the VM
// must stay DELETING: not dropped (the microvm would leak) and not left
// PRE_LEASE_HOOK_RUNNING. The pool doesn't count it, and the Sweeper deletes
// it once flintlock answers.
func TestClaimVM_PreLeaseHookFailureDeleteNotConfirmed_LeftDeleting(t *testing.T) {
	tests := []struct {
		name    string
		fake    *fakeMicroVM
		recover func(*fakeMicroVM)
	}{
		{
			name: "delete fails",
			fake: &fakeMicroVM{deleteErr: status.Error(codes.Unavailable, "flintlock host unreachable")},
			recover: func(f *fakeMicroVM) {
				f.mu.Lock()
				defer f.mu.Unlock()
				f.deleteErr = nil
			},
		},
		{
			name:    "delete hangs past the cleanup timeout",
			fake:    &fakeMicroVM{hangUID: "vm-1", hangGate: make(chan struct{})},
			recover: func(f *fakeMicroVM) { close(f.hangGate) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exec := &fakeMicroVMExec{
				respond: func(*microvmexecv1alpha1.ExecStart) (int32, string) { return 1, "" },
			}
			flint := startFakeFlintlock(t, tt.fake, exec)
			st := openTestStore(t)
			ctx := context.Background()

			pool := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_DELETE_AND_REPLACE, []string{"do-thing"})
			setupPoolWithVMs(t, st, pool, "vm-1")

			notifier := &spyNotifier{}
			s := api.NewLeaseServer(st, flint, api.HookExecConfig{CleanupTimeout: 200 * time.Millisecond}, notifier, nil)
			if _, err := s.ClaimVM(ctx, claimRequest("pool-a", "")); status.Code(err) != codes.Internal {
				t.Fatalf("ClaimVM() error = %v, want Internal", err)
			}

			assertVMPhase(t, st, "vm-1", poolmgrv1alpha1.VMPhase_DELETING)
			if got := tt.fake.deletedUIDs(); len(got) != 0 {
				t.Fatalf("flintlock deleted %v, want none yet", got)
			}
			if !slices.Equal(notifier.deleted, []string{"default/pool-a"}) {
				t.Fatalf("notifications = %v, want one NotifyVMDeleted for default/pool-a", notifier.deleted)
			}
			events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
			if err != nil {
				t.Fatalf("ListEventsSince: %v", err)
			}
			if len(events) != 1 || events[0].GetType() != poolmgrv1alpha1.EventType_VM_HOOK_FAILED {
				t.Fatalf("events = %+v, want one VM_HOOK_FAILED", events)
			}

			counts, err := reconciler.CountVMs(ctx, st, "pool-a", "default")
			if err != nil {
				t.Fatalf("CountVMs: %v", err)
			}
			if counts != (reconciler.VMCounts{}) {
				t.Fatalf("CountVMs() = %+v, want all zero: a DELETING VM must not count toward the pool", counts)
			}
			strategy, err := reconciler.NewStrategy(pool.GetReplenishmentStrategy())
			if err != nil {
				t.Fatalf("NewStrategy: %v", err)
			}
			if got := strategy.DesiredNewVMs(pool, counts); got != 1 {
				t.Fatalf("DesiredNewVMs() = %d, want 1", got)
			}

			tt.recover(tt.fake)
			reconciler.NewSweeper(st, flint, time.Hour, 0, nil, nil).Tick(ctx, time.Now())
			if _, err := st.GetVM(ctx, "vm-1"); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("after a sweeper tick, GetVM(vm-1) error = %v, want ErrNotFound", err)
			}
			if got := tt.fake.deletedUIDs(); !slices.Equal(got, []string{"vm-1"}) {
				t.Fatalf("after a sweeper tick, flintlock deleted %v, want [vm-1]", got)
			}
		})
	}
}
