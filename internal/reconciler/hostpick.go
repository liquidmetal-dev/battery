package reconciler

import (
	"context"
	"errors"
	"fmt"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/store"
)

// ErrNoEligibleHost is returned by PickHost when a pool has no flintlock
// hosts configured, or every host configured for it is currently drained.
var ErrNoEligibleHost = errors.New("reconciler: pool has no eligible flintlock hosts")

// PickHost returns the host from pool.FlintlockHosts, excluding any name
// present in drained, with the fewest non-terminal VMs currently recorded
// for the pool, ties broken by the hosts' order in FlintlockHosts. This is
// deliberately simple: v1 does no real host resource-capacity probing, just
// even spread by VM count. drained may be nil, meaning no host is drained.
func PickHost(ctx context.Context, st store.Store, pool *poolmgrv1alpha1.PoolSpec, drained map[string]bool) (string, error) {
	var hosts []string
	for _, h := range pool.GetFlintlockHosts() {
		if !drained[h] {
			hosts = append(hosts, h)
		}
	}
	if len(hosts) == 0 {
		return "", fmt.Errorf("%w: pool %s/%s", ErrNoEligibleHost, pool.GetNamespace(), pool.GetName())
	}

	vms, err := st.ListVMsByPool(ctx, pool.GetName(), pool.GetNamespace(), nil)
	if err != nil {
		return "", fmt.Errorf("reconciler: PickHost: %w", err)
	}

	counts := make(map[string]int, len(hosts))
	for _, vm := range vms {
		switch vm.GetPhase() {
		case poolmgrv1alpha1.VMPhase_DELETING, poolmgrv1alpha1.VMPhase_FAILED:
			continue
		}
		counts[vm.GetFlintlockHost()]++
	}

	best := hosts[0]
	bestCount := counts[best]
	for _, h := range hosts[1:] {
		if c := counts[h]; c < bestCount {
			best, bestCount = h, c
		}
	}
	return best, nil
}
