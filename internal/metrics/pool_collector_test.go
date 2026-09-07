package metrics_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

func openTestStore(t *testing.T) store.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "poolmgr.db")
	s, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return s
}

func samplePool(name string, size int32) *poolmgrv1alpha1.PoolSpec {
	return samplePoolIn(name, "default", size)
}

func samplePoolIn(name, namespace string, size int32) *poolmgrv1alpha1.PoolSpec {
	return &poolmgrv1alpha1.PoolSpec{
		Name:            name,
		Namespace:       namespace,
		Size:            size,
		FlintlockHosts:  []string{"host-a"},
		MicrovmTemplate: &flintlocktypes.MicroVMSpec{Vcpu: 1},
		ReplenishmentStrategy: &poolmgrv1alpha1.ReplenishmentStrategy{
			Type: poolmgrv1alpha1.ReplenishmentStrategyType_MIN_SIZE_THRESHOLD,
		},
		HookFailurePolicy:        poolmgrv1alpha1.HookFailurePolicy_QUARANTINE,
		HeartbeatInterval:        durationpb.New(30_000_000_000),
		HeartbeatExpiryThreshold: durationpb.New(90_000_000_000),
	}
}

func sampleVM(uid, poolName string, phase poolmgrv1alpha1.VMPhase) *poolmgrv1alpha1.VMRecord {
	return sampleVMIn(uid, poolName, "default", phase)
}

func sampleVMIn(uid, poolName, poolNamespace string, phase poolmgrv1alpha1.VMPhase) *poolmgrv1alpha1.VMRecord {
	now := timestamppb.Now()
	return &poolmgrv1alpha1.VMRecord{
		Uid:           uid,
		PoolName:      poolName,
		PoolNamespace: poolNamespace,
		FlintlockHost: "host-a",
		Phase:         phase,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func TestPoolCollector_EmitsPerPoolGauges(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", 3)
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}
	for _, vm := range []*poolmgrv1alpha1.VMRecord{
		sampleVM("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_AVAILABLE),
		sampleVM("vm-2", "pool-a", poolmgrv1alpha1.VMPhase_LEASED),
		sampleVM("vm-3", "pool-a", poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING),
		sampleVM("vm-4", "pool-a", poolmgrv1alpha1.VMPhase_PROVISIONING),
		sampleVM("vm-5", "pool-a", poolmgrv1alpha1.VMPhase_QUARANTINED),
	} {
		if err := st.CreateVM(ctx, vm); err != nil {
			t.Fatalf("CreateVM(%s): %v", vm.GetUid(), err)
		}
	}

	reg := metrics.NewRegistry()
	reg.RegisterPoolCollector(st)

	body := scrape(t, reg)
	assertContains(t, body, `poolmgr_pool_size{pool_name="pool-a",pool_namespace="default"} 3`)
	assertContains(t, body, `poolmgr_pool_available{pool_name="pool-a",pool_namespace="default"} 1`)
	assertContains(t, body, `poolmgr_pool_leased{pool_name="pool-a",pool_namespace="default"} 2`)
	assertContains(t, body, `poolmgr_pool_provisioning{pool_name="pool-a",pool_namespace="default"} 1`)
	assertContains(t, body, `poolmgr_pool_quarantined{pool_name="pool-a",pool_namespace="default"} 1`)
}

// TestPoolCollector_SameNameDifferentNamespace reproduces the P1 review
// finding: two pools sharing a name but not a namespace used to collide
// into one label set, which made promhttp's default handler fail the
// whole scrape with duplicate-metric errors (HTTP 500).
func TestPoolCollector_SameNameDifferentNamespace(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	for _, p := range []*poolmgrv1alpha1.PoolSpec{
		samplePoolIn("workers", "team-a", 1),
		samplePoolIn("workers", "team-b", 2),
	} {
		if err := st.CreatePool(ctx, p); err != nil {
			t.Fatalf("CreatePool(%s/%s): %v", p.GetNamespace(), p.GetName(), err)
		}
	}
	if err := st.CreateVM(ctx, sampleVMIn("vm-1", "workers", "team-a", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	reg := metrics.NewRegistry()
	reg.RegisterPoolCollector(st)

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 (no duplicate-metric collision), got %d:\n%s", w.Code, w.Body.String())
	}

	body := w.Body.String()
	assertContains(t, body, `poolmgr_pool_size{pool_name="workers",pool_namespace="team-a"} 1`)
	assertContains(t, body, `poolmgr_pool_size{pool_name="workers",pool_namespace="team-b"} 2`)
	assertContains(t, body, `poolmgr_pool_available{pool_name="workers",pool_namespace="team-a"} 1`)
	assertContains(t, body, `poolmgr_pool_available{pool_name="workers",pool_namespace="team-b"} 0`)
}

func TestPoolCollector_RecomputesOnEveryScrape(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	pool := samplePool("pool-a", 1)
	if err := st.CreatePool(ctx, pool); err != nil {
		t.Fatalf("CreatePool: %v", err)
	}

	reg := metrics.NewRegistry()
	reg.RegisterPoolCollector(st)

	assertContains(t, scrape(t, reg), `poolmgr_pool_available{pool_name="pool-a",pool_namespace="default"} 0`)

	if err := st.CreateVM(ctx, sampleVM("vm-1", "pool-a", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM: %v", err)
	}

	assertContains(t, scrape(t, reg), `poolmgr_pool_available{pool_name="pool-a",pool_namespace="default"} 1`)
}
