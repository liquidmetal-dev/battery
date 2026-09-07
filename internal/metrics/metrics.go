package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// metricSet holds the pool manager's push-based metrics (counters and
// histograms updated by call sites in the reconciler and API packages, as
// opposed to the pull-based per-pool gauges in pool_collector.go).
type metricSet struct {
	vmClaimsTotal     *prometheus.CounterVec
	vmReleasesTotal   *prometheus.CounterVec
	provisionDuration *prometheus.HistogramVec
	hookDuration      *prometheus.HistogramVec
	hookFailuresTotal *prometheus.CounterVec
	leaseDuration     *prometheus.HistogramVec
}

func newMetricSet(reg *prometheus.Registry) *metricSet {
	m := &metricSet{
		vmClaimsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "poolmgr_vm_claims_total",
			Help: "Total number of VMs successfully claimed via ClaimVM, labeled by pool_name.",
		}, []string{"pool_name"}),
		vmReleasesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "poolmgr_vm_releases_total",
			Help: "Total number of VMs released, labeled by pool_name and reason (api|expiry).",
		}, []string{"pool_name", "reason"}),
		provisionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "poolmgr_vm_provision_duration_seconds",
			Help: "Duration of the full VM provisioning pipeline (create through AVAILABLE), labeled by pool_name.",
		}, []string{"pool_name"}),
		hookDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "poolmgr_hook_duration_seconds",
			Help: "Duration of hook command execution, labeled by hook (create|pre_lease) and pool_name.",
		}, []string{"hook", "pool_name"}),
		hookFailuresTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "poolmgr_hook_failures_total",
			Help: "Total number of hook failures, labeled by hook (create|pre_lease) and pool_name.",
		}, []string{"hook", "pool_name"}),
		leaseDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "poolmgr_lease_duration_seconds",
			Help: "Duration a VM was leased, from ClaimVM to release/expiry, labeled by pool_name.",
		}, []string{"pool_name"}),
	}

	reg.MustRegister(
		m.vmClaimsTotal,
		m.vmReleasesTotal,
		m.provisionDuration,
		m.hookDuration,
		m.hookFailuresTotal,
		m.leaseDuration,
	)

	return m
}

// RecordVMClaim increments poolmgr_vm_claims_total for poolName.
func (r *Registry) RecordVMClaim(poolName string) {
	r.metrics.vmClaimsTotal.WithLabelValues(poolName).Inc()
}

// RecordVMRelease increments poolmgr_vm_releases_total for poolName,
// labeled by reason ("api" or "expiry").
func (r *Registry) RecordVMRelease(poolName, reason string) {
	r.metrics.vmReleasesTotal.WithLabelValues(poolName, reason).Inc()
}

// ObserveProvisionDuration records d as an observation of
// poolmgr_vm_provision_duration_seconds for poolName.
func (r *Registry) ObserveProvisionDuration(poolName string, d time.Duration) {
	r.metrics.provisionDuration.WithLabelValues(poolName).Observe(d.Seconds())
}

// ObserveHookDuration records d as an observation of
// poolmgr_hook_duration_seconds for hook ("create" or "pre_lease") and
// poolName.
func (r *Registry) ObserveHookDuration(hook, poolName string, d time.Duration) {
	r.metrics.hookDuration.WithLabelValues(hook, poolName).Observe(d.Seconds())
}

// RecordHookFailure increments poolmgr_hook_failures_total for hook
// ("create" or "pre_lease") and poolName.
func (r *Registry) RecordHookFailure(hook, poolName string) {
	r.metrics.hookFailuresTotal.WithLabelValues(hook, poolName).Inc()
}

// ObserveLeaseDuration records d as an observation of
// poolmgr_lease_duration_seconds for poolName.
func (r *Registry) ObserveLeaseDuration(poolName string, d time.Duration) {
	r.metrics.leaseDuration.WithLabelValues(poolName).Observe(d.Seconds())
}
