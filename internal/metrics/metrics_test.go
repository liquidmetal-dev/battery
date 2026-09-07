package metrics_test

import (
	"testing"
	"time"

	"github.com/liquidmetal-dev/battery/internal/metrics"
)

func TestRecordVMClaim(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.RecordVMClaim("pool-a")
	reg.RecordVMClaim("pool-a")
	reg.RecordVMClaim("pool-b")

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_vm_claims_total{pool_name="pool-a"} 2`)
	assertContains(t, body, `poolmgr_vm_claims_total{pool_name="pool-b"} 1`)
}

func TestRecordVMRelease(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.RecordVMRelease("pool-a", "api")
	reg.RecordVMRelease("pool-a", "expiry")
	reg.RecordVMRelease("pool-a", "expiry")

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_vm_releases_total{pool_name="pool-a",reason="api"} 1`)
	assertContains(t, body, `poolmgr_vm_releases_total{pool_name="pool-a",reason="expiry"} 2`)
}

func TestObserveProvisionDuration(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.ObserveProvisionDuration("pool-a", 2*time.Second)

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_vm_provision_duration_seconds_count{pool_name="pool-a"} 1`)
	assertContains(t, body, `poolmgr_vm_provision_duration_seconds_sum{pool_name="pool-a"} 2`)
}

func TestObserveHookDuration(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.ObserveHookDuration("create", "pool-a", 500*time.Millisecond)
	reg.ObserveHookDuration("pre_lease", "pool-a", 250*time.Millisecond)

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_hook_duration_seconds_count{hook="create",pool_name="pool-a"} 1`)
	assertContains(t, body, `poolmgr_hook_duration_seconds_count{hook="pre_lease",pool_name="pool-a"} 1`)
}

func TestRecordHookFailure(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.RecordHookFailure("create", "pool-a")
	reg.RecordHookFailure("create", "pool-a")
	reg.RecordHookFailure("pre_lease", "pool-a")

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_hook_failures_total{hook="create",pool_name="pool-a"} 2`)
	assertContains(t, body, `poolmgr_hook_failures_total{hook="pre_lease",pool_name="pool-a"} 1`)
}

func TestObserveLeaseDuration(t *testing.T) {
	reg := metrics.NewRegistry()
	reg.ObserveLeaseDuration("pool-a", 90*time.Second)

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_lease_duration_seconds_count{pool_name="pool-a"} 1`)
	assertContains(t, body, `poolmgr_lease_duration_seconds_sum{pool_name="pool-a"} 90`)
}

func TestRegistriesAreIsolated(t *testing.T) {
	regA := metrics.NewRegistry()
	regB := metrics.NewRegistry()

	regA.RecordVMClaim("pool-a")

	assertNotContains(t, scrape(t, regB), "poolmgr_vm_claims_total")
}
