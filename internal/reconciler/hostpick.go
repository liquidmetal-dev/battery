package reconciler

import (
	"context"
	"errors"
	"fmt"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/store"
)

// ErrNoEligibleHost is returned by PickHost when a pool has no flintlock
// hosts configured, or every host configured for it is currently cordoned
// or not registered. Provision also wraps it when the host PickHost chose
// was cordoned or removed before the placement could be reserved. Either way
// the reconciler treats it as an expected maintenance-time condition rather
// than a provision failure.
var ErrNoEligibleHost = errors.New("reconciler: pool has no eligible flintlock hosts")

// PickHost returns the host from pool.FlintlockHosts, excluding any name
// present in ineligible, with the fewest non-terminal VMs currently recorded
// for the pool, ties broken by the hosts' order in FlintlockHosts. This is
// deliberately simple: v1 does no real host resource-capacity probing, just
// even spread by VM count. ineligible may be nil, meaning every host is
// eligible; Provision passes ineligibleHosts.
func PickHost(ctx context.Context, st store.Store, pool *poolmgrv1alpha1.PoolSpec, ineligible map[string]bool) (string, error) {
	var hosts []string
	for _, h := range pool.GetFlintlockHosts() {
		if !ineligible[h] {
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

// ineligibleHosts returns the names in pool.FlintlockHosts that PickHost
// must skip: those that are cordoned, and those with no registry row.
// CreatePool, UpdatePool and RemoveHost keep pool specs and the registry
// consistent, so the second set is normally empty; it covers a pool stored
// before that check existed, and a reconciler still running a spec that
// UpdatePool has just replaced.
func ineligibleHosts(ctx context.Context, st store.Store, pool *poolmgrv1alpha1.PoolSpec) (map[string]bool, error) {
	registered, err := st.ListHosts(ctx)
	if err != nil {
		return nil, fmt.Errorf("reconciler: list hosts: %w", err)
	}
	cordoned := make(map[string]bool, len(registered))
	for _, h := range registered {
		cordoned[h.GetName()] = h.GetCordoned()
	}

	ineligible := make(map[string]bool)
	for _, name := range pool.GetFlintlockHosts() {
		if c, ok := cordoned[name]; !ok || c {
			ineligible[name] = true
		}
	}
	return ineligible, nil
}
