package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

func TestPoolStoresPauseWhenAvailable(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	p := samplePoolSpec("pool-a")
	p.PauseWhenAvailable = true
	if err := s.CreatePool(ctx, p); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	got, err := s.GetPool(ctx, "pool-a", "default")
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	if !got.GetPauseWhenAvailable() {
		t.Errorf("GetPool() pause_when_available = false, want true")
	}
	pools, err := s.ListPools(ctx)
	if err != nil {
		t.Fatalf("ListPools() error = %v", err)
	}
	if len(pools) != 1 || !pools[0].GetPauseWhenAvailable() {
		t.Errorf("ListPools() = %v, want one pool with pause_when_available", pools)
	}

	p.PauseWhenAvailable = false
	if err := s.UpdatePool(ctx, p); err != nil {
		t.Fatalf("UpdatePool() error = %v", err)
	}
	got, err = s.GetPool(ctx, "pool-a", "default")
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	if got.GetPauseWhenAvailable() {
		t.Errorf("GetPool() after update pause_when_available = true, want false")
	}
}

func TestVMStoresPaused(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreatePool(ctx, samplePoolSpec("pool-a")); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	vm := sampleVMRecord("vm-1", "pool-a", "default", poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING)
	if err := s.CreateVM(ctx, vm); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	vm.Phase = poolmgrv1alpha1.VMPhase_AVAILABLE
	vm.Paused = true
	if err := s.UpdateVM(ctx, vm); err != nil {
		t.Fatalf("UpdateVM() error = %v", err)
	}
	got, err := s.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if !got.GetPaused() {
		t.Errorf("GetVM() paused = false, want true")
	}

	// A claim moves the VM out of AVAILABLE but must hand back its paused
	// flag, since that is what tells ClaimVM to resume it.
	claimed, err := s.ClaimAvailableVM(ctx, "pool-a", "default")
	if err != nil {
		t.Fatalf("ClaimAvailableVM() error = %v", err)
	}
	if !claimed.GetPaused() {
		t.Errorf("ClaimAvailableVM() paused = false, want true")
	}
	listed, err := s.ListVMsByPool(ctx, "pool-a", "default", nil)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(listed) != 1 || !listed[0].GetPaused() {
		t.Errorf("ListVMsByPool() = %v, want one paused VM", listed)
	}
}

func TestOpenUpgradesVersionThreeDB(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "poolmgr.db")

	// Build a database as a binary with only migrations 1 to 3 would have:
	// no pause columns, user_version 3, a pool row and a VM row.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		t.Fatalf("apply baseline schema error = %v", err)
	}
	for i := range 3 {
		if _, err := db.Exec(migrations[i]); err != nil {
			t.Fatalf("apply migration %d error = %v", i+1, err)
		}
	}
	if _, err := db.Exec(`PRAGMA user_version = 3;`); err != nil {
		t.Fatalf("set user_version error = %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO pools (name, namespace, size, flintlock_hosts, microvm_template,
			replenishment_strategy, create_commands, pre_lease_commands,
			hook_failure_policy, heartbeat_interval_ns, heartbeat_expiry_threshold_ns)
		VALUES ('pool-old', 'default', 1, '[]', '{}', '{}', '[]', '[]', 0, 1, 2)`); err != nil {
		t.Fatalf("insert pool error = %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO vms (uid, pool_name, pool_namespace, flintlock_host, phase, lease_id, created_at, updated_at)
		VALUES ('vm-old', 'pool-old', 'default', 'host-a', 2, NULL, 5, 6)`); err != nil {
		t.Fatalf("insert vm error = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if got, want := schemaVersion(t, s), len(migrations); got != want {
		t.Errorf("user_version = %d, want %d", got, want)
	}
	pool, err := s.GetPool(ctx, "pool-old", "default")
	if err != nil {
		t.Fatalf("GetPool() error = %v", err)
	}
	if pool.GetPauseWhenAvailable() {
		t.Errorf("GetPool() pause_when_available = true, want false for a pre-upgrade pool")
	}
	vm, err := s.GetVM(ctx, "vm-old")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if vm.GetPaused() {
		t.Errorf("GetVM() paused = true, want false for a pre-upgrade VM")
	}
}
