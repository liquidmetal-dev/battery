package metrics_test

import (
	"testing"
	"time"

	"github.com/liquidmetal-dev/battery/internal/metrics"
)

func TestRecordVMClaim(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.RecordVMClaim("pool-a", "default")
	reg.RecordVMClaim("pool-a", "default")
	reg.RecordVMClaim("pool-b", "default")

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_vm_claims_total{pool_name="pool-a",pool_namespace="default"} 2`)
	assertContains(t, body, `poolmgr_vm_claims_total{pool_name="pool-b",pool_namespace="default"} 1`)
}

func TestRecordVMClaim_SameNameDifferentNamespace(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.RecordVMClaim("workers", "team-a")
	reg.RecordVMClaim("workers", "team-b")
	reg.RecordVMClaim("workers", "team-b")

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_vm_claims_total{pool_name="workers",pool_namespace="team-a"} 1`)
	assertContains(t, body, `poolmgr_vm_claims_total{pool_name="workers",pool_namespace="team-b"} 2`)
}

func TestRecordVMRelease(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.RecordVMRelease("pool-a", "default", "api")
	reg.RecordVMRelease("pool-a", "default", "expiry")
	reg.RecordVMRelease("pool-a", "default", "expiry")

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_vm_releases_total{pool_name="pool-a",pool_namespace="default",reason="api"} 1`)
	assertContains(t, body, `poolmgr_vm_releases_total{pool_name="pool-a",pool_namespace="default",reason="expiry"} 2`)
}

func TestObserveProvisionDuration(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.ObserveProvisionDuration("pool-a", "default", 2*time.Second)

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_vm_provision_duration_seconds_count{pool_name="pool-a",pool_namespace="default"} 1`)
	assertContains(t, body, `poolmgr_vm_provision_duration_seconds_sum{pool_name="pool-a",pool_namespace="default"} 2`)
}

func TestObserveHookDuration(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.ObserveHookDuration("create", "pool-a", "default", 500*time.Millisecond)
	reg.ObserveHookDuration("pre_lease", "pool-a", "default", 250*time.Millisecond)

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_hook_duration_seconds_count{hook="create",pool_name="pool-a",pool_namespace="default"} 1`)
	assertContains(t, body, `poolmgr_hook_duration_seconds_count{hook="pre_lease",pool_name="pool-a",pool_namespace="default"} 1`)
}

func TestRecordHookFailure(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.RecordHookFailure("create", "pool-a", "default")
	reg.RecordHookFailure("create", "pool-a", "default")
	reg.RecordHookFailure("pre_lease", "pool-a", "default")

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_hook_failures_total{hook="create",pool_name="pool-a",pool_namespace="default"} 2`)
	assertContains(t, body, `poolmgr_hook_failures_total{hook="pre_lease",pool_name="pool-a",pool_namespace="default"} 1`)
}

func TestObserveLeaseDuration(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.ObserveLeaseDuration("pool-a", "default", 90*time.Second)

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_lease_duration_seconds_count{pool_name="pool-a",pool_namespace="default"} 1`)
	assertContains(t, body, `poolmgr_lease_duration_seconds_sum{pool_name="pool-a",pool_namespace="default"} 90`)
}

func TestRegistriesAreIsolated(t *testing.T) {
	regA := metrics.NewRegistry()
	regB := metrics.NewRegistry()

	regA.RecordVMClaim("pool-a", "default")

	assertNotContains(t, scrape(t, regB), "poolmgr_vm_claims_total")
}
