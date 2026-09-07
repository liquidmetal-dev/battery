package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metricSet holds the pool manager's push-based metrics (counters and
// histograms updated by call sites in the reconciler and API packages, as
// opposed to the pull-based per-pool gauges in pool_collector.go). Every
// metric is labeled by both pool_name and pool_namespace: a pool name is
// only unique within its namespace (store.GetPool takes both), so
// pool_name alone can collide across namespaces.
type metricSet struct {
	vmClaimsTotal                  *prometheus.CounterVec
	vmReleasesTotal                *prometheus.CounterVec
	provisionDuration              *prometheus.HistogramVec
	hookDuration                   *prometheus.HistogramVec
	hookFailuresTotal              *prometheus.CounterVec
	leaseDuration                  *prometheus.HistogramVec
	reconcilerUnexpectedExitsTotal *prometheus.CounterVec
}

func newMetricSet(reg *prometheus.Registry) *metricSet {
	m := &metricSet{
		vmClaimsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "poolmgr_vm_claims_total",
			Help: "Total number of VMs successfully claimed via ClaimVM, labeled by pool_name/pool_namespace.",
		}, []string{"pool_name", "pool_namespace"}),
		vmReleasesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "poolmgr_vm_releases_total",
			Help: "Total number of VMs released, labeled by pool_name/pool_namespace and reason (api|expiry).",
		}, []string{"pool_name", "pool_namespace", "reason"}),
		provisionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "poolmgr_vm_provision_duration_seconds",
			Help: "Duration of the full VM provisioning pipeline (create through AVAILABLE), labeled by pool_name/pool_namespace.",
		}, []string{"pool_name", "pool_namespace"}),
		hookDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "poolmgr_hook_duration_seconds",
			Help: "Duration of hook command execution, labeled by hook (create|pre_lease) and pool_name/pool_namespace.",
		}, []string{"hook", "pool_name", "pool_namespace"}),
		hookFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "poolmgr_hook_failures_total",
			Help: "Total number of hook failures, labeled by hook (create|pre_lease) and pool_name/pool_namespace.",
		}, []string{"hook", "pool_name", "pool_namespace"}),
		leaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "poolmgr_lease_duration_seconds",
			Help: "Duration a VM was leased, from ClaimVM to release/expiry, labeled by pool_name/pool_namespace.",
		}, []string{"pool_name", "pool_namespace"}),
		reconcilerUnexpectedExitsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "poolmgr_reconciler_unexpected_exit_total",
			Help: "Total number of times a pool's Reconciler.Run returned with an error other than context.Canceled, labeled by pool_name/pool_namespace. Expected to stay at 0 - see internal/poolmanager.",
		}, []string{"pool_name", "pool_namespace"}),
	}

	reg.MustRegister(
		m.vmClaimsTotal,
		m.vmReleasesTotal,
		m.provisionDuration,
		m.hookDuration,
		m.hookFailuresTotal,
		m.leaseDuration,
		m.reconcilerUnexpectedExitsTotal,
	)

	return m
}

// RecordVMClaim increments poolmgr_vm_claims_total for poolName/poolNamespace.
func (r *Registry) RecordVMClaim(poolName, poolNamespace string) {
	r.metrics.vmClaimsTotal.WithLabelValues(poolName, poolNamespace).Inc()
}

// RecordVMRelease increments poolmgr_vm_releases_total for
// poolName/poolNamespace, labeled by reason ("api" or "expiry").
func (r *Registry) RecordVMRelease(poolName, poolNamespace, reason string) {
	r.metrics.vmReleasesTotal.WithLabelValues(poolName, poolNamespace, reason).Inc()
}

// ObserveProvisionDuration records d as an observation of
// poolmgr_vm_provision_duration_seconds for poolName/poolNamespace.
func (r *Registry) ObserveProvisionDuration(poolName, poolNamespace string, d time.Duration) {
	r.metrics.provisionDuration.WithLabelValues(poolName, poolNamespace).Observe(d.Seconds())
}

// ObserveHookDuration records d as an observation of
// poolmgr_hook_duration_seconds for hook ("create" or "pre_lease") and
// poolName/poolNamespace.
func (r *Registry) ObserveHookDuration(hook, poolName, poolNamespace string, d time.Duration) {
	r.metrics.hookDuration.WithLabelValues(hook, poolName, poolNamespace).Observe(d.Seconds())
}

// RecordHookFailure increments poolmgr_hook_failures_total for hook
// ("create" or "pre_lease") and poolName/poolNamespace.
func (r *Registry) RecordHookFailure(hook, poolName, poolNamespace string) {
	r.metrics.hookFailuresTotal.WithLabelValues(hook, poolName, poolNamespace).Inc()
}

// ObserveLeaseDuration records d as an observation of
// poolmgr_lease_duration_seconds for poolName/poolNamespace.
func (r *Registry) ObserveLeaseDuration(poolName, poolNamespace string, d time.Duration) {
	r.metrics.leaseDuration.WithLabelValues(poolName, poolNamespace).Observe(d.Seconds())
}

// RecordReconcilerUnexpectedExit increments poolmgr_reconciler_unexpected_exit_total
// for poolName/poolNamespace. A reconciler's Run loop is only ever expected to
// return via context cancellation; any other return is unexpected and is not
// retried (see internal/poolmanager.Manager.StartReconciler).
func (r *Registry) RecordReconcilerUnexpectedExit(poolName, poolNamespace string) {
	r.metrics.reconcilerUnexpectedExitsTotal.WithLabelValues(poolName, poolNamespace).Inc()
}
