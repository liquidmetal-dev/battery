package store

import (
	"context"
	"database/sql"
	_ "embed"
	"path/filepath"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// schemaV0SQL is schema.sql as it was before rollout support added
// pools.template_hash, pools.rollout_policy and vms.template_hash.
//
//go:embed testdata/schema_v0.sql
var schemaV0SQL string

// openV0Store returns the path of a database created with schemaV0SQL and
// holding pool-a and one of its VMs, as an older poolmgrd would have left it.
// The rows are written through a current store and copied across (old
// columns only), so the test doesn't hand-encode the stored formats.
func openV0Store(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()

	src, err := Open(filepath.Join(dir, "src.db"))
	if err != nil {
		t.Fatalf("Open(src) error = %v", err)
	}
	if err := src.CreatePool(ctx, samplePoolSpec("pool-a")); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := src.CreateVM(ctx, sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}
	if err := src.Close(); err != nil {
		t.Fatalf("Close(src) error = %v", err)
	}

	path := filepath.Join(dir, "v0.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open(v0) error = %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(1) // ATTACH is per-connection

	for _, stmt := range []string{
		schemaV0SQL,
		`ATTACH DATABASE '` + filepath.Join(dir, "src.db") + `' AS src`,
		`INSERT INTO pools SELECT name, namespace, size, flintlock_hosts, microvm_template,
			replenishment_strategy, create_commands, pre_lease_commands, hook_failure_policy,
			heartbeat_interval_ns, heartbeat_expiry_threshold_ns FROM src.pools`,
		`INSERT INTO vms SELECT uid, pool_name, pool_namespace, flintlock_host, phase,
			lease_id, created_at, updated_at FROM src.vms`,
		`DETACH DATABASE src`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("build v0 database: %v\n%s", err, stmt)
		}
	}
	return path
}

func TestOpenMigratesV0Database(t *testing.T) {
	ctx := context.Background()
	path := openV0Store(t)

	// Twice: the second Open must find every column already added.
	for i := range 2 {
		s, err := Open(path)
		if err != nil {
			t.Fatalf("Open() #%d error = %v", i+1, err)
		}

		pool, err := s.GetPool(ctx, "pool-a", "default")
		if err != nil {
			t.Fatalf("GetPool() #%d error = %v", i+1, err)
		}
		if pool.GetTemplateHash() != "" || pool.GetRolloutPolicy() != nil {
			t.Errorf("GetPool() #%d = hash %q, policy %v; want empty hash and nil policy", i+1, pool.GetTemplateHash(), pool.GetRolloutPolicy())
		}
		if pools, err := s.ListPools(ctx); err != nil || len(pools) != 1 {
			t.Errorf("ListPools() #%d = %d pools, err %v; want 1, nil", i+1, len(pools), err)
		}
		vms, err := s.ListVMsByPool(ctx, "pool-a", "default", nil)
		if err != nil {
			t.Fatalf("ListVMsByPool() #%d error = %v", i+1, err)
		}
		if len(vms) != 1 || vms[0].GetTemplateHash() != "" {
			t.Errorf("ListVMsByPool() #%d = %v; want vm-1 with an empty template hash", i+1, vms)
		}

		if err := s.Close(); err != nil {
			t.Fatalf("Close() #%d error = %v", i+1, err)
		}
	}
}

func TestMigratedV0DatabaseStoresRolloutFields(t *testing.T) {
	ctx := context.Background()
	s, err := Open(openV0Store(t))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	pool, err := s.GetPool(ctx, "pool-a", "default")
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	pool.TemplateHash = "hash-2"
	pool.RolloutPolicy = &poolmgrv1alpha1.RolloutPolicy{
		MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Percent{Percent: 50},
	}
	if err := s.UpdatePool(ctx, pool); err != nil {
		t.Fatalf("UpdatePool() error = %v", err)
	}
	vm := sampleVMRecord("vm-2", "pool-a", "default", poolmgrv1alpha1.VMPhase_PROVISIONING)
	vm.TemplateHash = "hash-2"
	if err := s.CreateVM(ctx, vm); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	got, err := s.GetPool(ctx, "pool-a", "default")
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	if got.GetTemplateHash() != "hash-2" || got.GetRolloutPolicy().GetPercent() != 50 {
		t.Errorf("GetPool() = hash %q, percent %d; want hash-2, 50", got.GetTemplateHash(), got.GetRolloutPolicy().GetPercent())
	}
	gotVM, err := s.GetVM(ctx, "vm-2")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if gotVM.GetTemplateHash() != "hash-2" {
		t.Errorf("GetVM() template hash = %q, want hash-2", gotVM.GetTemplateHash())
	}
}
