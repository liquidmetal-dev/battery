package metrics

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// poolLabels is shared by every per-pool Desc below: a pool name is only
// unique within its namespace (store.GetPool takes both), so pool_name
// alone would collide across namespaces - see pool_namespace.
var poolLabels = []string{"pool_name", "pool_namespace"}

var (
	poolSizeDesc = prometheus.NewDesc(
		"poolmgr_pool_size", "Target size of the pool.", poolLabels, nil)
	poolAvailableDesc = prometheus.NewDesc(
		"poolmgr_pool_available", "Number of AVAILABLE VMs in the pool.", poolLabels, nil)
	poolLeasedDesc = prometheus.NewDesc(
		"poolmgr_pool_leased", "Number of LEASED (or PRE_LEASE_HOOK_RUNNING) VMs in the pool.", poolLabels, nil)
	poolProvisioningDesc = prometheus.NewDesc(
		"poolmgr_pool_provisioning", "Number of PROVISIONING (or CREATE_HOOK_RUNNING) VMs in the pool.", poolLabels, nil)
	poolQuarantinedDesc = prometheus.NewDesc(
		"poolmgr_pool_quarantined", "Number of QUARANTINED VMs in the pool.", poolLabels, nil)
)

// PoolCollector is a pull-based prometheus.Collector that computes the
// per-pool gauges (poolmgr_pool_size/available/leased/provisioning/
// quarantined) fresh from st on every scrape, rather than being
// push-updated by the reconciler's tick loop. This keeps the gauges from
// going stale between ticks and avoids every Reconciler goroutine needing
// to share mutable metric state.
type PoolCollector struct {
	store store.Store
}

// NewPoolCollector returns a PoolCollector backed by st.
func NewPoolCollector(st store.Store) *PoolCollector {
	return &PoolCollector{store: st}
}

// Describe implements prometheus.Collector.
func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- poolSizeDesc
	ch <- poolAvailableDesc
	ch <- poolLeasedDesc
	ch <- poolProvisioningDesc
	ch <- poolQuarantinedDesc
}

// Collect implements prometheus.Collector: it lists every pool and, for
// each, counts its VMs by phase, emitting one gauge sample per metric per
// (pool_name, pool_namespace) pair.
func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	ctx := context.Background()

	pools, err := c.store.ListPools(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "metrics: PoolCollector: ListPools failed", "error", err)
		return
	}

	for _, pool := range pools {
		counts, err := countVMs(ctx, c.store, pool.GetName(), pool.GetNamespace())
		if err != nil {
			slog.ErrorContext(ctx, "metrics: PoolCollector: count VMs failed", "pool", pool.GetName(), "namespace", pool.GetNamespace(), "error", err)
			continue
		}

		name, namespace := pool.GetName(), pool.GetNamespace()
		ch <- prometheus.MustNewConstMetric(poolSizeDesc, prometheus.GaugeValue, float64(pool.GetSize()), name, namespace)
		ch <- prometheus.MustNewConstMetric(poolAvailableDesc, prometheus.GaugeValue, float64(counts.available), name, namespace)
		ch <- prometheus.MustNewConstMetric(poolLeasedDesc, prometheus.GaugeValue, float64(counts.leased), name, namespace)
		ch <- prometheus.MustNewConstMetric(poolProvisioningDesc, prometheus.GaugeValue, float64(counts.provisioning), name, namespace)
		ch <- prometheus.MustNewConstMetric(poolQuarantinedDesc, prometheus.GaugeValue, float64(counts.quarantined), name, namespace)
	}
}

// vmCounts is a phase breakdown of a pool's VMs. Deliberately independent
// of internal/reconciler.VMCounts: internal/reconciler will import this
// package to record push-based metrics, so this package can't import
// internal/reconciler back without a cycle.
type vmCounts struct {
	available    int
	leased       int
	provisioning int
	quarantined  int
}

// countVMs mirrors internal/reconciler.CountVMs's phase breakdown.
func countVMs(ctx context.Context, st store.Store, poolName, poolNamespace string) (vmCounts, error) {
	vms, err := st.ListVMsByPool(ctx, poolName, poolNamespace, nil)
	if err != nil {
		return vmCounts{}, err
	}

	var counts vmCounts
	for _, vm := range vms {
		switch vm.GetPhase() {
		case poolmgrv1alpha1.VMPhase_AVAILABLE:
			counts.available++
		case poolmgrv1alpha1.VMPhase_LEASED, poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING:
			counts.leased++
		case poolmgrv1alpha1.VMPhase_PROVISIONING, poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING:
			counts.provisioning++
		case poolmgrv1alpha1.VMPhase_QUARANTINED:
			counts.quarantined++
		}
	}
	return counts, nil
}
