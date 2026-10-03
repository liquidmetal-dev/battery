# DeletePool Drains the Pool Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `PoolAdmin.DeletePool` delete a pool that still has VMs, by draining those VMs itself (issue #112).

**Architecture:** `DeletePool` stops the pool's reconciler and waits for it to exit, then runs one store transaction that removes the pool row, marks its VMs `DELETING` and removes its leases. It then deletes each microVM from flintlock inline; any delete that fails is left `DELETING` for the sweeper's existing retry. Leased VMs make the call fail unless the request sets `force`.

**Tech Stack:** Go 1.25, gRPC/protobuf (buf), SQLite (`database/sql`), cobra.

**Spec:** `docs/superpowers/specs/2026-10-03-delete-pool-drain-design.md`

## Global Constraints

- Commits follow Conventional Commits. No agent footers (`Co-Authored-By`, "Generated with", session links) in commit messages or PR descriptions (AGENTS.md).
- A change to `api/proto/poolmgr/v1alpha1/*.proto` updates `internal/poolmgrctl/` in the same change (AGENTS.md).
- No schema migration: `internal/store/schema.sql` and the `migrations` list are untouched.
- The existing `Store.DeletePool` method keeps its signature and behaviour.
- `UpdatePool` keeps using the non-waiting `StopReconciler`.
- Generated code is refreshed only with `hack/generate-proto.sh`; never hand-edit `*.pb.go`.
- "Leased" means VM phase `LEASED` or `PRE_LEASE_HOOK_RUNNING`.
- New proto field: `DeletePoolRequest.force`, `bool`, field number `2`. New enum value: `VM_DELETED_ON_POOL_DELETE = 11`.

## Review Focus

Failure modes the spec implies but does not list as tests. Each has a test in the task named.

1. A pool whose only VMs are `DELETING` or `FAILED` (previously blocked) is deleted without `force`. Task 4, `TestDeletePool_DrainsUnleasedVMs`.
2. The tombstone touches only the named pool: another pool's VMs and leases, including a pool with the same name in another namespace, are unchanged. Task 2, `TestDeletePoolAndMarkVMs_LeavesOtherPoolsAlone`.
3. The client cancels the RPC after the tombstone commits: the microVMs are still deleted from flintlock. Task 4, `TestDeletePool_ClientCancelStillDeletesVMs`.
4. A `PoolAdminServer` built with a nil flintlock pool deletes a pool with VMs without panicking, leaving the VMs `DELETING`. Task 4, `TestDeletePool_NilFlintlockLeavesVMsDeleting`.
5. `StopReconcilerAndWait` for a pool with no running reconciler returns nil immediately. Task 3, `TestManager_StopReconcilerAndWait_UnknownPoolIsNoop`.

## File Structure

| File | Change |
| --- | --- |
| `api/proto/poolmgr/v1alpha1/pooladmin.proto` | `force` field, RPC contract comment |
| `api/proto/poolmgr/v1alpha1/types.proto` | `VM_DELETED_ON_POOL_DELETE` |
| `api/proto/poolmgr/v1alpha1/*.pb.go` | regenerated |
| `internal/poolmgrctl/pool.go`, `pool_test.go` | `--force` flag |
| `internal/store/store.go`, `sqlite.go`, `sqlite_test.go` | `ErrPoolHasLeasedVMs`, `DeletePoolAndMarkVMs` |
| `internal/poolmanager/manager.go`, `manager_test.go` | `StopReconcilerAndWait` |
| `internal/api/pooladmin.go`, `pooladmin_test.go`, `testutil_test.go` | new `DeletePool` flow, `PoolLifecycle` method, constructor parameter |
| `cmd/poolmgrd/main.go`, `cmd/poolmgrd/e2e_test.go`, `cmd/poolmgrctl/e2e_test.go` | constructor wiring, e2e expectation |
| `docs/runbooks/e2e-manual-verification.md` | cleanup step |

---

### Task 1: Proto `force` field, new event type, and `poolmgrctl pool delete --force`

**Files:**
- Modify: `api/proto/poolmgr/v1alpha1/pooladmin.proto`
- Modify: `api/proto/poolmgr/v1alpha1/types.proto`
- Regenerate: `api/proto/poolmgr/v1alpha1/pooladmin.pb.go`, `types.pb.go`
- Modify: `internal/poolmgrctl/pool.go` (`newPoolDeleteCmd`)
- Test: `internal/poolmgrctl/pool_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `DeletePoolRequest.GetForce() bool`; `poolmgrv1alpha1.EventType_VM_DELETED_ON_POOL_DELETE`.

- [ ] **Step 1: Edit the protos**

In `pooladmin.proto`, replace the `DeletePool` RPC line and the `DeletePoolRequest` message:

```proto
  // DeletePool deletes a pool and drains it: every VM the pool owns that is not leased is
  // deleted along with it. If any VM is leased the call fails with FAILED_PRECONDITION,
  // unless force is set. Once it returns OK the pool is gone; a microVM whose flintlock host
  // could not be reached is deleted in the background.
  rpc DeletePool(DeletePoolRequest) returns (google.protobuf.Empty);
```

```proto
message DeletePoolRequest {
  PoolRef ref = 1;
  // Force also deletes the pool's leased VMs and ends their leases.
  bool force = 2;
}
```

In `types.proto`, add to `EventType` after `POOL_SIZE_BELOW_TARGET = 10;`:

```proto
  // VM_DELETED_ON_POOL_DELETE is emitted for each VM removed because its pool was deleted.
  VM_DELETED_ON_POOL_DELETE = 11;
```

- [ ] **Step 2: Regenerate and check the diff**

Run: `hack/generate-proto.sh && git status --short api/`
Expected: only `pooladmin.proto`, `pooladmin.pb.go`, `types.proto`, `types.pb.go` are modified (`pooladmin_grpc.pb.go` may change too, for the RPC comment).

- [ ] **Step 3: Write the failing CLI test**

In `internal/poolmgrctl/pool_test.go`, add `"sync"` and `"google.golang.org/protobuf/types/known/emptypb"` to the imports. Split `bufconnPoolAdmin` so a test can serve any `PoolAdminServer`:

```go
// bufconnPoolAdmin starts a real api.PoolAdminServer backed by a temp
// SQLite store, serves it over an in-memory bufconn listener, and returns
// a dialed *grpc.ClientConn to it.
func bufconnPoolAdmin(t *testing.T) *grpc.ClientConn {
	t.Helper()

	path := filepath.Join(t.TempDir(), "poolmgr.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	return bufconnServe(t, api.NewPoolAdminServer(st, nil))
}

// bufconnServe serves admin over an in-memory bufconn listener and returns a
// dialed *grpc.ClientConn to it.
func bufconnServe(t *testing.T, admin poolmgrv1alpha1.PoolAdminServer) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	poolmgrv1alpha1.RegisterPoolAdminServer(srv, admin)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}
```

Then add at the end of the file:

```go
// recordingPoolAdmin is a PoolAdmin server that records the DeletePool
// requests it receives and always succeeds.
type recordingPoolAdmin struct {
	poolmgrv1alpha1.UnimplementedPoolAdminServer

	mu      sync.Mutex
	deletes []*poolmgrv1alpha1.DeletePoolRequest
}

func (r *recordingPoolAdmin) DeletePool(_ context.Context, req *poolmgrv1alpha1.DeletePoolRequest) (*emptypb.Empty, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deletes = append(r.deletes, req)
	return &emptypb.Empty{}, nil
}

func TestPoolDelete_ForceFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{"default", []string{"--name", "pool-a", "--namespace", "default"}, false},
		{"force", []string{"--name", "pool-a", "--namespace", "default", "--force"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingPoolAdmin{}
			ctx := withTestClients(bufconnServe(t, rec))

			cmd := newPoolDeleteCmd()
			cmd.SetOut(&bytes.Buffer{})
			cmd.SetContext(ctx)
			cmd.SetArgs(tt.args)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}

			rec.mu.Lock()
			defer rec.mu.Unlock()
			if len(rec.deletes) != 1 {
				t.Fatalf("got %d DeletePool requests, want 1", len(rec.deletes))
			}
			if got := rec.deletes[0].GetForce(); got != tt.want {
				t.Errorf("DeletePoolRequest.force = %v, want %v", got, tt.want)
			}
			if ref := rec.deletes[0].GetRef(); ref.GetName() != "pool-a" || ref.GetNamespace() != "default" {
				t.Errorf("DeletePoolRequest.ref = %+v, want default/pool-a", ref)
			}
		})
	}
}
```

- [ ] **Step 4: Run it to verify it fails**

Run: `go test ./internal/poolmgrctl/ -run TestPoolDelete_ForceFlag -v`
Expected: the `force` subtest FAILS with `unknown flag: --force`.

- [ ] **Step 5: Implement the flag**

In `internal/poolmgrctl/pool.go`, change `newPoolDeleteCmd`:

```go
func newPoolDeleteCmd() *cobra.Command {
	var (
		name, namespace string
		force           bool
	)

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a pool and the VMs it owns",
		Long: "Delete a pool and the VMs it owns. The delete is refused while any of the " +
			"pool's VMs is leased, unless --force is given.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			poolAdmin := clientsFromContext(cmd.Context()).poolAdmin

			_, err := poolAdmin.DeletePool(cmd.Context(), &poolmgrv1alpha1.DeletePoolRequest{
				Ref:   &poolmgrv1alpha1.PoolRef{Name: name, Namespace: namespace},
				Force: force,
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "pool %s/%s deleted\n", namespace, name)
			return err
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "pool name")
	cmd.Flags().StringVar(&namespace, "namespace", "", "pool namespace")
	cmd.Flags().BoolVar(&force, "force", false, "also delete leased VMs and end their leases")
```

Leave the rest of the function (the `MarkFlagRequired` calls and `return cmd`) as it is.

- [ ] **Step 6: Run the package tests**

Run: `go test ./internal/poolmgrctl/`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add api/proto/poolmgr/v1alpha1 internal/poolmgrctl
git commit -m "feat(api): add force to DeletePoolRequest and a pool-delete VM event"
```

---

### Task 2: Store tombstone transaction

**Files:**
- Modify: `internal/store/store.go`
- Modify: `internal/store/sqlite.go` (add after `DeletePool`)
- Test: `internal/store/sqlite_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `var ErrPoolHasLeasedVMs error`
  - `Store.DeletePoolAndMarkVMs(ctx context.Context, name, namespace string, force bool) ([]*poolmgrv1alpha1.VMRecord, error)`

Existing test helpers in `sqlite_test.go` to reuse: `openTestStore(t)`, `samplePoolSpec(name)` (namespace `default`), `sampleVMRecord(uid, poolName, poolNamespace, phase)`, `sampleLeaseRecord(leaseID, vmUID, poolName, poolNamespace, expiresAt)`.

- [ ] **Step 1: Write the failing tests**

Append to `internal/store/sqlite_test.go`:

```go
func TestDeletePoolAndMarkVMs(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if err := s.CreatePool(ctx, samplePoolSpec("pool-a")); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	for uid, phase := range map[string]poolmgrv1alpha1.VMPhase{
		"vm-available":    poolmgrv1alpha1.VMPhase_AVAILABLE,
		"vm-provisioning": poolmgrv1alpha1.VMPhase_PROVISIONING,
		"vm-quarantined":  poolmgrv1alpha1.VMPhase_QUARANTINED,
	} {
		if err := s.CreateVM(ctx, sampleVMRecord(uid, "pool-a", "default", phase)); err != nil {
			t.Fatalf("CreateVM(%s) error = %v", uid, err)
		}
	}

	got, err := s.DeletePoolAndMarkVMs(ctx, "pool-a", "default", false)
	if err != nil {
		t.Fatalf("DeletePoolAndMarkVMs() error = %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("DeletePoolAndMarkVMs() returned %d VMs, want 3", len(got))
	}
	for _, vm := range got {
		if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
			t.Errorf("returned VM %s phase = %v, want DELETING", vm.GetUid(), vm.GetPhase())
		}
	}

	if _, err := s.GetPool(ctx, "pool-a", "default"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetPool() after delete error = %v, want ErrNotFound", err)
	}
	stored, err := s.ListVMsByPool(ctx, "pool-a", "default", nil)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(stored) != 3 {
		t.Fatalf("stored VMs = %d, want 3", len(stored))
	}
	for _, vm := range stored {
		if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
			t.Errorf("stored VM %s phase = %v, want DELETING", vm.GetUid(), vm.GetPhase())
		}
	}
}

func TestDeletePoolAndMarkVMs_NotFound(t *testing.T) {
	s := openTestStore(t)

	_, err := s.DeletePoolAndMarkVMs(context.Background(), "missing", "default", false)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeletePoolAndMarkVMs() error = %v, want ErrNotFound", err)
	}
}

func TestDeletePoolAndMarkVMs_LeasedVMs(t *testing.T) {
	leasedPhases := []poolmgrv1alpha1.VMPhase{
		poolmgrv1alpha1.VMPhase_LEASED,
		poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING,
	}

	for _, phase := range leasedPhases {
		t.Run(phase.String(), func(t *testing.T) {
			s := openTestStore(t)
			ctx := context.Background()

			if err := s.CreatePool(ctx, samplePoolSpec("pool-a")); err != nil {
				t.Fatalf("CreatePool() error = %v", err)
			}
			leased := sampleVMRecord("vm-leased", "pool-a", "default", phase)
			leased.LeaseId = proto.String("lease-1")
			if err := s.CreateVM(ctx, leased); err != nil {
				t.Fatalf("CreateVM() error = %v", err)
			}
			if err := s.CreateVM(ctx, sampleVMRecord("vm-available", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE)); err != nil {
				t.Fatalf("CreateVM() error = %v", err)
			}
			if err := s.CreateLease(ctx, sampleLeaseRecord("lease-1", "vm-leased", "pool-a", "default", time.Now().Add(time.Hour))); err != nil {
				t.Fatalf("CreateLease() error = %v", err)
			}

			// Without force: refused, and nothing changes.
			if _, err := s.DeletePoolAndMarkVMs(ctx, "pool-a", "default", false); !errors.Is(err, ErrPoolHasLeasedVMs) {
				t.Fatalf("DeletePoolAndMarkVMs(force=false) error = %v, want ErrPoolHasLeasedVMs", err)
			}
			if _, err := s.GetPool(ctx, "pool-a", "default"); err != nil {
				t.Fatalf("GetPool() after refusal error = %v, want the pool still present", err)
			}
			if vm, err := s.GetVM(ctx, "vm-available"); err != nil || vm.GetPhase() != poolmgrv1alpha1.VMPhase_AVAILABLE {
				t.Fatalf("vm-available after refusal = %+v, err %v, want AVAILABLE", vm, err)
			}
			if vm, err := s.GetVM(ctx, "vm-leased"); err != nil || vm.GetPhase() != phase {
				t.Fatalf("vm-leased after refusal = %+v, err %v, want %v", vm, err, phase)
			}
			if _, err := s.GetLease(ctx, "lease-1"); err != nil {
				t.Fatalf("GetLease() after refusal error = %v, want the lease still present", err)
			}

			// With force: everything goes.
			got, err := s.DeletePoolAndMarkVMs(ctx, "pool-a", "default", true)
			if err != nil {
				t.Fatalf("DeletePoolAndMarkVMs(force=true) error = %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("DeletePoolAndMarkVMs(force=true) returned %d VMs, want 2", len(got))
			}
			for _, vm := range got {
				if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
					t.Errorf("VM %s phase = %v, want DELETING", vm.GetUid(), vm.GetPhase())
				}
				if vm.LeaseId != nil {
					t.Errorf("VM %s lease_id = %q, want cleared", vm.GetUid(), vm.GetLeaseId())
				}
			}
			if _, err := s.GetLease(ctx, "lease-1"); !errors.Is(err, ErrNotFound) {
				t.Errorf("GetLease() after forced delete error = %v, want ErrNotFound", err)
			}
			if _, err := s.GetPool(ctx, "pool-a", "default"); !errors.Is(err, ErrNotFound) {
				t.Errorf("GetPool() after forced delete error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestDeletePoolAndMarkVMs_LeavesOtherPoolsAlone(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	// pool-a exists in two namespaces, and pool-b sits beside it in "default".
	otherNS := samplePoolSpec("pool-a")
	otherNS.Namespace = "other"
	for _, p := range []*poolmgrv1alpha1.PoolSpec{samplePoolSpec("pool-a"), samplePoolSpec("pool-b"), otherNS} {
		if err := s.CreatePool(ctx, p); err != nil {
			t.Fatalf("CreatePool(%s/%s) error = %v", p.GetNamespace(), p.GetName(), err)
		}
	}

	keepLeased := sampleVMRecord("vm-b-leased", "pool-b", "default", poolmgrv1alpha1.VMPhase_LEASED)
	keepLeased.LeaseId = proto.String("lease-b")
	for _, vm := range []*poolmgrv1alpha1.VMRecord{
		sampleVMRecord("vm-a", "pool-a", "default", poolmgrv1alpha1.VMPhase_AVAILABLE),
		sampleVMRecord("vm-other-ns", "pool-a", "other", poolmgrv1alpha1.VMPhase_AVAILABLE),
		keepLeased,
	} {
		if err := s.CreateVM(ctx, vm); err != nil {
			t.Fatalf("CreateVM(%s) error = %v", vm.GetUid(), err)
		}
	}
	if err := s.CreateLease(ctx, sampleLeaseRecord("lease-b", "vm-b-leased", "pool-b", "default", time.Now().Add(time.Hour))); err != nil {
		t.Fatalf("CreateLease() error = %v", err)
	}

	// pool-b's leased VM must not make default/pool-a's delete refuse.
	got, err := s.DeletePoolAndMarkVMs(ctx, "pool-a", "default", false)
	if err != nil {
		t.Fatalf("DeletePoolAndMarkVMs() error = %v", err)
	}
	if len(got) != 1 || got[0].GetUid() != "vm-a" {
		t.Fatalf("DeletePoolAndMarkVMs() returned %+v, want just vm-a", got)
	}

	if vm, err := s.GetVM(ctx, "vm-other-ns"); err != nil || vm.GetPhase() != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Errorf("vm-other-ns = %+v, err %v, want untouched AVAILABLE", vm, err)
	}
	if vm, err := s.GetVM(ctx, "vm-b-leased"); err != nil || vm.GetPhase() != poolmgrv1alpha1.VMPhase_LEASED || vm.GetLeaseId() != "lease-b" {
		t.Errorf("vm-b-leased = %+v, err %v, want untouched LEASED with lease-b", vm, err)
	}
	if _, err := s.GetLease(ctx, "lease-b"); err != nil {
		t.Errorf("GetLease(lease-b) error = %v, want untouched", err)
	}
	for _, ref := range [][2]string{{"pool-b", "default"}, {"pool-a", "other"}} {
		if _, err := s.GetPool(ctx, ref[0], ref[1]); err != nil {
			t.Errorf("GetPool(%s/%s) error = %v, want untouched", ref[1], ref[0], err)
		}
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/store/ -run TestDeletePoolAndMarkVMs`
Expected: build FAILS with `s.DeletePoolAndMarkVMs undefined` and `undefined: ErrPoolHasLeasedVMs`.

- [ ] **Step 3: Add the error and interface method**

In `internal/store/store.go`, after the `ErrHostCordoned` declaration:

```go
// ErrPoolHasLeasedVMs is returned by DeletePoolAndMarkVMs when the pool has a
// leased VM and force was not set.
var ErrPoolHasLeasedVMs = errors.New("store: pool has leased vms")
```

In the `Store` interface, after `DeletePool`:

```go
	// DeletePoolAndMarkVMs deletes the pool row, marks every VM of the pool DELETING
	// (clearing its lease_id) and deletes the pool's lease rows, all in one transaction. It
	// returns the VMs as marked, ordered by uid. Returns ErrNotFound if the pool doesn't
	// exist. Unless force is set, returns ErrPoolHasLeasedVMs and changes nothing if any VM
	// is LEASED or PRE_LEASE_HOOK_RUNNING.
	DeletePoolAndMarkVMs(ctx context.Context, name, namespace string, force bool) ([]*poolmgrv1alpha1.VMRecord, error)
```

- [ ] **Step 4: Implement it**

In `internal/store/sqlite.go`, after `DeletePool`:

```go
func (s *sqliteStore) DeletePoolAndMarkVMs(ctx context.Context, name, namespace string, force bool) ([]*poolmgrv1alpha1.VMRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin delete pool tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// Delete the pool row first: it is a write, so the transaction holds the
	// write lock before the leased check below reads the VMs, and a
	// concurrent ClaimAvailableVM can't slip in between the check and the
	// phase update. A refusal below rolls this delete back.
	res, err := tx.ExecContext(ctx, `DELETE FROM pools WHERE name = ? AND namespace = ?`, name, namespace)
	if err != nil {
		return nil, fmt.Errorf("store: delete pool: %w", err)
	}
	if err := checkRowsAffected(res); err != nil {
		return nil, err
	}

	if !force {
		var leased int
		err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM vms WHERE pool_name = ? AND pool_namespace = ? AND phase IN (?, ?)`,
			name, namespace, int32(poolmgrv1alpha1.VMPhase_LEASED), int32(poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING),
		).Scan(&leased)
		if err != nil {
			return nil, fmt.Errorf("store: count leased vms: %w", err)
		}
		if leased > 0 {
			return nil, ErrPoolHasLeasedVMs
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE vms SET phase = ?, lease_id = NULL, updated_at = ? WHERE pool_name = ? AND pool_namespace = ?`,
		int32(poolmgrv1alpha1.VMPhase_DELETING), time.Now().UnixNano(), name, namespace,
	); err != nil {
		return nil, fmt.Errorf("store: mark pool vms deleting: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE pool_name = ? AND pool_namespace = ?`, name, namespace); err != nil {
		return nil, fmt.Errorf("store: delete pool leases: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT uid, pool_name, pool_namespace, flintlock_host, phase, lease_id, created_at, updated_at
		FROM vms WHERE pool_name = ? AND pool_namespace = ? ORDER BY uid`, name, namespace)
	if err != nil {
		return nil, fmt.Errorf("store: query pool vms: %w", err)
	}
	var vms []*poolmgrv1alpha1.VMRecord
	for rows.Next() {
		var row vmRow
		if err := rows.Scan(&row.uid, &row.poolName, &row.poolNamespace, &row.flintlockHost, &row.phase, &row.leaseID, &row.createdAt, &row.updatedAt); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("store: scan vm: %w", err)
		}
		vms = append(vms, rowToVM(row))
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("store: iterate vms: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("store: close vms: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: commit delete pool tx: %w", err)
	}
	return vms, nil
}
```

- [ ] **Step 5: Run the store tests**

Run: `go test ./internal/store/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/store
git commit -m "feat(store): delete a pool and mark its VMs deleting in one transaction"
```

---

### Task 3: `Manager.StopReconcilerAndWait`

**Files:**
- Modify: `internal/poolmanager/manager.go`
- Test: `internal/poolmanager/manager_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `(*Manager).StopReconcilerAndWait(ctx context.Context, name, namespace string) error`

- [ ] **Step 1: Write the failing tests**

In `internal/poolmanager/manager_test.go`, give `fakeRunner` a gate that holds `Run` open after cancellation. Add the field:

```go
	// exitGate, if set, holds Run open after its ctx is done until the gate
	// is closed, standing in for a reconciler finishing in-flight work.
	exitGate chan struct{}
```

and change `Run` to:

```go
func (f *fakeRunner) Run(ctx context.Context) error {
	<-ctx.Done()

	f.mu.Lock()
	f.cancelled = true
	err := f.runErr
	gate := f.exitGate
	f.mu.Unlock()

	if gate != nil {
		<-gate
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}
```

Append the tests:

```go
func TestManager_StopReconcilerAndWait_WaitsForRunnerExit(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	m := New(context.Background(), nil, nil, nil)
	if err := m.StartReconciler(testPool("pool-a")); err != nil {
		t.Fatalf("StartReconciler: %v", err)
	}
	gate := make(chan struct{})
	fakes["pool-a"].mu.Lock()
	fakes["pool-a"].exitGate = gate
	fakes["pool-a"].mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- m.StopReconcilerAndWait(context.Background(), "pool-a", "default") }()

	select {
	case err := <-done:
		t.Fatalf("StopReconcilerAndWait returned (err=%v) before the runner exited", err)
	case <-time.After(100 * time.Millisecond):
	}
	if m.Running("pool-a", "default") {
		t.Fatal("expected pool-a to be forgotten while its runner is still exiting")
	}

	close(gate)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StopReconcilerAndWait() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for StopReconcilerAndWait to return")
	}
}

func TestManager_StopReconcilerAndWait_ContextExpires(t *testing.T) {
	fakes := map[string]*fakeRunner{}
	withFakeReconciler(t, fakes)

	m := New(context.Background(), nil, nil, nil)
	if err := m.StartReconciler(testPool("pool-a")); err != nil {
		t.Fatalf("StartReconciler: %v", err)
	}
	gate := make(chan struct{})
	t.Cleanup(func() { close(gate) })
	fakes["pool-a"].mu.Lock()
	fakes["pool-a"].exitGate = gate
	fakes["pool-a"].mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := m.StopReconcilerAndWait(ctx, "pool-a", "default")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StopReconcilerAndWait() error = %v, want context.DeadlineExceeded", err)
	}
}

func TestManager_StopReconcilerAndWait_UnknownPoolIsNoop(t *testing.T) {
	m := New(context.Background(), nil, nil, nil)

	if err := m.StopReconcilerAndWait(context.Background(), "does-not-exist", "default"); err != nil {
		t.Fatalf("StopReconcilerAndWait() error = %v, want nil", err)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./internal/poolmanager/ -run TestManager_StopReconcilerAndWait`
Expected: build FAILS with `m.StopReconcilerAndWait undefined`.

- [ ] **Step 3: Implement it**

In `internal/poolmanager/manager.go`:

Add `done` to the handle:

```go
type reconcilerHandle struct {
	runner reconcilerRunner
	cancel context.CancelFunc
	// done is closed when the goroutine running runner.Run exits.
	done chan struct{}
}
```

In `StartReconciler`, create the channel and close it when the goroutine exits. Replace the handle assignment and the goroutine:

```go
	childCtx, cancel := context.WithCancel(m.rootCtx)
	done := make(chan struct{})
	m.handles[key] = &reconcilerHandle{runner: runner, cancel: cancel, done: done}

	log := slog.Default().With("pool", key.name, "namespace", key.namespace)
	log.Info("poolmanager: starting reconciler")

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer close(done)
		if err := runner.Run(childCtx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("poolmanager: reconciler exited unexpectedly", "error", err)
			m.metrics.RecordReconcilerUnexpectedExit(key.name, key.namespace)
		}
	}()
	return nil
```

Replace `StopReconciler` with a shared helper plus both stop methods:

```go
// StopReconciler cancels and forgets the reconciler for (name, namespace),
// if one is running. It does not wait for the reconciler's goroutine to
// exit - see StopReconcilerAndWait, and Run for the shutdown path.
func (m *Manager) StopReconciler(name, namespace string) {
	m.cancelAndForget(name, namespace)
}

// StopReconcilerAndWait cancels and forgets the reconciler for (name,
// namespace) like StopReconciler, then waits for its goroutine to exit, so
// that on a nil return nothing is still provisioning for the pool. It
// returns ctx.Err() if ctx is done first; the reconciler stays cancelled and
// forgotten either way. A no-op returning nil if none is running.
func (m *Manager) StopReconcilerAndWait(ctx context.Context, name, namespace string) error {
	h, ok := m.cancelAndForget(name, namespace)
	if !ok {
		return nil
	}

	select {
	case <-h.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// cancelAndForget removes the handle for (name, namespace) and cancels its
// reconciler, returning the handle if there was one.
func (m *Manager) cancelAndForget(name, namespace string) (*reconcilerHandle, bool) {
	key := poolKey{name: name, namespace: namespace}

	m.mu.Lock()
	h, ok := m.handles[key]
	if ok {
		delete(m.handles, key)
	}
	m.mu.Unlock()

	if ok {
		slog.Info("poolmanager: stopping reconciler", "pool", name, "namespace", namespace)
		h.cancel()
	}
	return h, ok
}
```

- [ ] **Step 4: Run the package tests with the race detector**

Run: `go test -race ./internal/poolmanager/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/poolmanager
git commit -m "feat(poolmanager): add StopReconcilerAndWait"
```

---

### Task 4: `DeletePool` drains the pool

**Files:**
- Modify: `internal/api/pooladmin.go`
- Modify: `cmd/poolmgrd/main.go` (the `NewPoolAdminServer` call in `buildGRPCServer`)
- Modify: `cmd/poolmgrctl/e2e_test.go:93`, `cmd/poolmgrd/e2e_test.go:283-289`
- Modify: `internal/poolmgrctl/pool_test.go` (the `NewPoolAdminServer` call in `bufconnPoolAdmin`)
- Test: `internal/api/pooladmin_test.go`, `internal/api/testutil_test.go`

**Interfaces:**
- Consumes:
  - `store.ErrPoolHasLeasedVMs`, `Store.DeletePoolAndMarkVMs(ctx, name, namespace string, force bool) ([]*poolmgrv1alpha1.VMRecord, error)` (Task 2)
  - `(*poolmanager.Manager).StopReconcilerAndWait(ctx context.Context, name, namespace string) error` (Task 3)
  - `DeletePoolRequest.GetForce()`, `EventType_VM_DELETED_ON_POOL_DELETE` (Task 1)
  - Existing: `reconciler.EnsureVMDeleted(ctx, st, flint, vm) error`, `reconciler.EmitEvent(ctx, st, pool, uid, eventType)`, `reconciler.NewSweeper(st, flint, tickInterval, warningWindow, notifier, m)`, `(*Sweeper).Tick(ctx, now)`
- Produces:
  - `api.PoolLifecycle` gains `StopReconcilerAndWait(ctx context.Context, name, namespace string) error`
  - `api.NewPoolAdminServer(st store.Store, flint *flintlockclient.Pool, poolMgr PoolLifecycle) *PoolAdminServer`

- [ ] **Step 1: Change the constructor signature at every call site**

The new parameter goes second, matching `NewLeaseServer(st, flint, ...)`. In `internal/api/pooladmin.go`:

```go
// PoolAdminServer implements poolmgrv1alpha1.PoolAdminServer: the CRUD
// lifecycle of pool definitions.
type PoolAdminServer struct {
	poolmgrv1alpha1.UnimplementedPoolAdminServer

	store   store.Store
	flint   *flintlockclient.Pool
	poolMgr PoolLifecycle

	poolLocksMu sync.Mutex
	poolLocks   map[poolLockKey]*sync.Mutex
}

// NewPoolAdminServer returns a PoolAdminServer backed by st, deleting the
// VMs of a deleted pool through flint. If flint is nil, DeletePool leaves
// those VMs DELETING for the Sweeper. If poolMgr is nil, NoopPoolLifecycle{}
// is used.
func NewPoolAdminServer(st store.Store, flint *flintlockclient.Pool, poolMgr PoolLifecycle) *PoolAdminServer {
	if poolMgr == nil {
		poolMgr = NoopPoolLifecycle{}
	}
	return &PoolAdminServer{store: st, flint: flint, poolMgr: poolMgr, poolLocks: make(map[poolLockKey]*sync.Mutex)}
}
```

Add `"github.com/liquidmetal-dev/battery/internal/flintlockclient"` to the imports.

Update the callers:

```bash
sed -i 's/api\.NewPoolAdminServer(st, nil)/api.NewPoolAdminServer(st, nil, nil)/; s/api\.NewPoolAdminServer(st, lifecycle)/api.NewPoolAdminServer(st, nil, lifecycle)/; s/api\.NewPoolAdminServer(ps, lifecycle)/api.NewPoolAdminServer(ps, nil, lifecycle)/' internal/api/pooladmin_test.go internal/poolmgrctl/pool_test.go
sed -i 's/api\.NewPoolAdminServer(st, poolMgr)/api.NewPoolAdminServer(st, flint, poolMgr)/' cmd/poolmgrd/main.go cmd/poolmgrctl/e2e_test.go
grep -rn "NewPoolAdminServer(" --include="*.go" .
```

Expected from the `grep`: every call has three arguments.

- [ ] **Step 2: Extend `PoolLifecycle` and the test fake**

In `internal/api/pooladmin.go`:

```go
// PoolLifecycle starts and stops a pool's Reconciler in response to
// CreatePool/UpdatePool/DeletePool. Satisfied structurally by
// *poolmanager.Manager; kept narrow here so internal/api doesn't need to
// import internal/poolmanager.
type PoolLifecycle interface {
	StartReconciler(spec *poolmgrv1alpha1.PoolSpec) error
	StopReconciler(name, namespace string)
	// StopReconcilerAndWait stops the pool's reconciler and waits for it to
	// exit, so nothing is still provisioning for the pool on a nil return.
	StopReconcilerAndWait(ctx context.Context, name, namespace string) error
}
```

and after `NoopPoolLifecycle.StopReconciler`:

```go
// StopReconcilerAndWait does nothing and always succeeds.
func (NoopPoolLifecycle) StopReconcilerAndWait(context.Context, string, string) error { return nil }
```

In `internal/api/testutil_test.go`, replace the `fakePoolLifecycle` type and add the method:

```go
// fakePoolLifecycle records StartReconciler/StopReconciler/
// StopReconcilerAndWait calls for tests that assert PoolAdminServer's wiring
// without a real poolmanager.Manager.
type fakePoolLifecycle struct {
	mu          sync.Mutex
	started     []string
	stopped     []string
	startErr    error
	stopWaitErr error // if set, StopReconcilerAndWait returns this
}
```

```go
func (f *fakePoolLifecycle) StopReconcilerAndWait(_ context.Context, name, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, name)
	return f.stopWaitErr
}
```

Run: `go build ./... && go vet -tags e2e ./...`
Expected: no errors.

- [ ] **Step 3: Write the failing tests**

In `internal/api/pooladmin_test.go`, add these imports: `"slices"`, `"google.golang.org/protobuf/proto"`, `"google.golang.org/protobuf/types/known/timestamppb"`, `"github.com/liquidmetal-dev/battery/internal/reconciler"`.

Delete `TestDeletePoolBlockedWithVMs` and `TestDeletePool_VMsStillPresent_DoesNotStopReconciler`. Add:

```go
// newDeletePoolFixture creates pool-a in a fresh store, served by a
// PoolAdminServer wired to a fake flintlock and a recording lifecycle.
func newDeletePoolFixture(t *testing.T) (*api.PoolAdminServer, store.Store, *fakeMicroVM, *fakePoolLifecycle) {
	t.Helper()

	st := openTestStore(t)
	fakeVM := &fakeMicroVM{}
	lifecycle := &fakePoolLifecycle{}
	s := api.NewPoolAdminServer(st, startFakeFlintlock(t, fakeVM, &fakeMicroVMExec{}), lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(context.Background(), &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	return s, st, fakeVM, lifecycle
}

// leasedVMWithLease stores a LEASED VM in pool-a together with its lease row.
func leasedVMWithLease(t *testing.T, st store.Store, uid, leaseID string) {
	t.Helper()
	ctx := context.Background()

	vm := withPhase(sampleAvailableVM(uid, "pool-a"), poolmgrv1alpha1.VMPhase_LEASED)
	vm.LeaseId = proto.String(leaseID)
	if err := st.CreateVM(ctx, vm); err != nil {
		t.Fatalf("CreateVM(%s) error = %v", uid, err)
	}
	now := time.Now()
	if err := st.CreateLease(ctx, &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         leaseID,
		VmUid:           uid,
		PoolName:        "pool-a",
		PoolNamespace:   "default",
		ClaimedAt:       timestamppb.New(now),
		LastHeartbeatAt: timestamppb.New(now),
		ExpiresAt:       timestamppb.New(now.Add(time.Hour)),
	}); err != nil {
		t.Fatalf("CreateLease(%s) error = %v", leaseID, err)
	}
}

var poolARef = &poolmgrv1alpha1.PoolRef{Name: "pool-a", Namespace: "default"}

func TestDeletePool_DrainsUnleasedVMs(t *testing.T) {
	// Every phase other than the two leased ones is drained without force,
	// including DELETING and FAILED, which CountVMs doesn't tally.
	phases := []poolmgrv1alpha1.VMPhase{
		poolmgrv1alpha1.VMPhase_PROVISIONING,
		poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING,
		poolmgrv1alpha1.VMPhase_AVAILABLE,
		poolmgrv1alpha1.VMPhase_DELETING,
		poolmgrv1alpha1.VMPhase_QUARANTINED,
		poolmgrv1alpha1.VMPhase_FAILED,
	}

	ctx := context.Background()
	s, st, fakeVM, lifecycle := newDeletePoolFixture(t)

	var wantUIDs []string
	for _, phase := range phases {
		uid := "vm-" + phase.String()
		wantUIDs = append(wantUIDs, uid)
		if err := st.CreateVM(ctx, withPhase(sampleAvailableVM(uid, "pool-a"), phase)); err != nil {
			t.Fatalf("CreateVM(%s) error = %v", uid, err)
		}
	}
	slices.Sort(wantUIDs)

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}

	if _, err := s.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: poolARef}); status.Code(err) != codes.NotFound {
		t.Errorf("GetPool() after delete error = %v, want NotFound", err)
	}
	remaining, err := st.ListVMsByPool(ctx, "pool-a", "default", nil)
	if err != nil {
		t.Fatalf("ListVMsByPool() error = %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("VM rows left after delete = %+v, want none", remaining)
	}

	gotDeleted := fakeVM.deletedUIDs()
	slices.Sort(gotDeleted)
	if !slices.Equal(gotDeleted, wantUIDs) {
		t.Errorf("flintlock deleted uids = %v, want %v", gotDeleted, wantUIDs)
	}

	events, err := st.ListEventsSince(ctx, "pool-a", "default", 0, 100)
	if err != nil {
		t.Fatalf("ListEventsSince() error = %v", err)
	}
	var gotEventUIDs []string
	for _, e := range events {
		if e.GetType() != poolmgrv1alpha1.EventType_VM_DELETED_ON_POOL_DELETE {
			t.Errorf("event type = %v, want VM_DELETED_ON_POOL_DELETE", e.GetType())
		}
		gotEventUIDs = append(gotEventUIDs, e.GetVmUid())
	}
	slices.Sort(gotEventUIDs)
	if !slices.Equal(gotEventUIDs, wantUIDs) {
		t.Errorf("event vm uids = %v, want %v", gotEventUIDs, wantUIDs)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 1 || lifecycle.stopped[0] != "pool-a" {
		t.Errorf("stopped = %v, want [pool-a]", lifecycle.stopped)
	}
	if len(lifecycle.started) != 1 {
		t.Errorf("started = %v, want only the CreatePool start", lifecycle.started)
	}
}

func TestDeletePool_LeasedVMWithoutForce_Refused(t *testing.T) {
	ctx := context.Background()
	s, st, fakeVM, lifecycle := newDeletePoolFixture(t)

	leasedVMWithLease(t, st, "vm-leased", "lease-1")
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-available", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeletePool() error = %v, want FailedPrecondition", err)
	}

	if _, err := st.GetPool(ctx, "pool-a", "default"); err != nil {
		t.Errorf("GetPool() after refusal error = %v, want the pool still present", err)
	}
	if vm, err := st.GetVM(ctx, "vm-available"); err != nil || vm.GetPhase() != poolmgrv1alpha1.VMPhase_AVAILABLE {
		t.Errorf("vm-available after refusal = %+v, err %v, want AVAILABLE", vm, err)
	}
	if _, err := st.GetLease(ctx, "lease-1"); err != nil {
		t.Errorf("GetLease() after refusal error = %v, want the lease still present", err)
	}
	if got := fakeVM.deletedUIDs(); len(got) != 0 {
		t.Errorf("flintlock deleted uids = %v, want none", got)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 0 {
		t.Errorf("stopped = %v, want none", lifecycle.stopped)
	}
}

func TestDeletePool_PreLeaseHookRunningWithoutForce_Refused(t *testing.T) {
	ctx := context.Background()
	s, st, _, _ := newDeletePoolFixture(t)

	// A ClaimVM that has claimed its VM but not yet created the lease row.
	if err := st.CreateVM(ctx, withPhase(sampleAvailableVM("vm-claiming", "pool-a"), poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING)); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeletePool() error = %v, want FailedPrecondition", err)
	}
}

func TestDeletePool_LeasedVMWithForce_DeletesVMAndLease(t *testing.T) {
	ctx := context.Background()
	s, st, fakeVM, _ := newDeletePoolFixture(t)

	leasedVMWithLease(t, st, "vm-leased", "lease-1")

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef, Force: true}); err != nil {
		t.Fatalf("DeletePool(force) error = %v", err)
	}

	if _, err := st.GetPool(ctx, "pool-a", "default"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetPool() error = %v, want ErrNotFound", err)
	}
	if _, err := st.GetVM(ctx, "vm-leased"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetVM() error = %v, want ErrNotFound", err)
	}
	if _, err := st.GetLease(ctx, "lease-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetLease() error = %v, want ErrNotFound", err)
	}
	if got := fakeVM.deletedUIDs(); len(got) != 1 || got[0] != "vm-leased" {
		t.Errorf("flintlock deleted uids = %v, want [vm-leased]", got)
	}
}

func TestDeletePool_FlintlockDeleteFails_SweeperFinishes(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	fakeVM := &fakeMicroVM{deleteErr: status.Error(codes.Unavailable, "host down")}
	flint := startFakeFlintlock(t, fakeVM, &fakeMicroVMExec{})
	s := api.NewPoolAdminServer(st, flint, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v, want nil despite the flintlock failure", err)
	}
	if _, err := st.GetPool(ctx, "pool-a", "default"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetPool() error = %v, want ErrNotFound", err)
	}
	vm, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v, want the row kept for retry", err)
	}
	if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Fatalf("vm-1 phase = %v, want DELETING", vm.GetPhase())
	}

	// The host comes back: the sweeper's pending-deletion retry finishes the
	// job even though the VM's pool no longer exists.
	fakeVM.mu.Lock()
	fakeVM.deleteErr = nil
	fakeVM.mu.Unlock()

	reconciler.NewSweeper(st, flint, 0, 0, nil, nil).Tick(ctx, time.Now())

	if _, err := st.GetVM(ctx, "vm-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetVM() after sweep error = %v, want ErrNotFound", err)
	}
	if got := fakeVM.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
		t.Errorf("flintlock deleted uids = %v, want [vm-1]", got)
	}
}

func TestDeletePool_NilFlintlockLeavesVMsDeleting(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	s := api.NewPoolAdminServer(st, nil, nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}
	vm, err := st.GetVM(ctx, "vm-1")
	if err != nil {
		t.Fatalf("GetVM() error = %v", err)
	}
	if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		t.Errorf("vm-1 phase = %v, want DELETING", vm.GetPhase())
	}
}

// tombstoneHookStore wraps a store.Store to intercept DeletePoolAndMarkVMs.
// If refuse is set the call returns that error without touching the store;
// otherwise the real call runs and after is invoked once it has succeeded.
type tombstoneHookStore struct {
	store.Store
	refuse error
	after  func()
}

func (h *tombstoneHookStore) DeletePoolAndMarkVMs(ctx context.Context, name, namespace string, force bool) ([]*poolmgrv1alpha1.VMRecord, error) {
	if h.refuse != nil {
		return nil, h.refuse
	}
	vms, err := h.Store.DeletePoolAndMarkVMs(ctx, name, namespace, force)
	if err == nil && h.after != nil {
		h.after()
	}
	return vms, err
}

func TestDeletePool_ClaimRacesIn_RestartsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{}
	// The pre-check sees no leased VM, then the tombstone finds one: a
	// ClaimVM landed in between.
	hooked := &tombstoneHookStore{Store: st, refuse: store.ErrPoolHasLeasedVMs}
	s := api.NewPoolAdminServer(hooked, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeletePool() error = %v, want FailedPrecondition", err)
	}
	if _, err := st.GetPool(ctx, "pool-a", "default"); err != nil {
		t.Errorf("GetPool() error = %v, want the pool still present", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.stopped) != 1 {
		t.Errorf("stopped = %v, want [pool-a]", lifecycle.stopped)
	}
	if len(lifecycle.started) != 2 {
		t.Errorf("started = %v, want 2 entries (create, restart after the refusal)", lifecycle.started)
	}
}

func TestDeletePool_StopWaitFails_RestartsReconciler(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	lifecycle := &fakePoolLifecycle{stopWaitErr: context.DeadlineExceeded}
	s := api.NewPoolAdminServer(st, nil, lifecycle)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}

	_, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("DeletePool() error = %v, want DeadlineExceeded", err)
	}
	if _, err := st.GetPool(ctx, "pool-a", "default"); err != nil {
		t.Errorf("GetPool() error = %v, want the pool still present", err)
	}

	lifecycle.mu.Lock()
	defer lifecycle.mu.Unlock()
	if len(lifecycle.started) != 2 {
		t.Errorf("started = %v, want 2 entries (create, restart after the failed wait)", lifecycle.started)
	}
}

func TestDeletePool_ClientCancelStillDeletesVMs(t *testing.T) {
	st := openTestStore(t)
	fakeVM := &fakeMicroVM{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The client goes away the moment the tombstone has committed.
	hooked := &tombstoneHookStore{Store: st, after: cancel}
	s := api.NewPoolAdminServer(hooked, startFakeFlintlock(t, fakeVM, &fakeMicroVMExec{}), nil)

	spec := samplePool("pool-a", poolmgrv1alpha1.HookFailurePolicy_QUARANTINE, nil)
	if _, err := s.CreatePool(ctx, &poolmgrv1alpha1.CreatePoolRequest{Spec: spec}); err != nil {
		t.Fatalf("CreatePool() error = %v", err)
	}
	if err := st.CreateVM(ctx, sampleAvailableVM("vm-1", "pool-a")); err != nil {
		t.Fatalf("CreateVM() error = %v", err)
	}

	if _, err := s.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: poolARef}); err != nil {
		t.Fatalf("DeletePool() error = %v", err)
	}

	if got := fakeVM.deletedUIDs(); len(got) != 1 || got[0] != "vm-1" {
		t.Errorf("flintlock deleted uids = %v, want [vm-1]", got)
	}
	if _, err := st.GetVM(context.Background(), "vm-1"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("GetVM() error = %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 4: Run them to verify they fail**

Run: `go test ./internal/api/ -run TestDeletePool`
Expected: FAIL. `TestDeletePool_DrainsUnleasedVMs`, `..._WithForce_...`, `..._FlintlockDeleteFails_...`, `..._NilFlintlock...` and `..._ClientCancel...` fail with `FailedPrecondition ... still has VMs`; the two restart tests fail on the `started` count or the status code.

- [ ] **Step 5: Implement the new flow**

In `internal/api/pooladmin.go`, add `"time"` to the imports and, below the imports:

```go
// poolDeleteCleanupTimeout bounds DeletePool's inline flintlock deletes,
// which run detached from the RPC's own cancellation.
const poolDeleteCleanupTimeout = 30 * time.Second
```

Replace `DeletePool` and its doc comment with:

```go
// DeletePool deletes the named pool and drains it: every VM the pool owns is
// deleted with it. A pool with a leased VM (LEASED or
// PRE_LEASE_HOOK_RUNNING) fails with FailedPrecondition unless req.force is
// set, in which case those VMs and their leases are deleted too.
//
// The pool's reconciler is stopped, and waited for, before the pool row is
// removed, so no in-flight Provision can add a VM afterwards. The pool row
// and its VMs' transition to DELETING then commit in one store transaction;
// once that has happened DeletePool returns OK even if a microVM could not
// be deleted from its flintlock host, since the Sweeper's pending-deletion
// retry finishes any VM left DELETING.
func (s *PoolAdminServer) DeletePool(ctx context.Context, req *poolmgrv1alpha1.DeletePoolRequest) (*emptypb.Empty, error) {
	name, ns := req.GetRef().GetName(), req.GetRef().GetNamespace()
	force := req.GetForce()

	log := slog.Default().With("pool", name, "namespace", ns)
	log.InfoContext(ctx, "pooladmin: DeletePool requested", "force", force)

	unlock := s.lockPool(name, ns)
	defer unlock()

	spec, err := s.store.GetPool(ctx, name, ns)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			log.WarnContext(ctx, "pooladmin: DeletePool failed: pool not found")
			return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", ns, name)
		}
		log.ErrorContext(ctx, "pooladmin: DeletePool: get pool failed", "error", err)
		return nil, status.Errorf(codes.Internal, "get pool: %v", err)
	}

	// Refuse early, before disturbing the reconciler, when the answer is
	// already known. DeletePoolAndMarkVMs repeats this check atomically.
	if !force {
		vms, err := s.store.ListVMsByPool(ctx, name, ns, nil)
		if err != nil {
			log.ErrorContext(ctx, "pooladmin: DeletePool: list vms failed", "error", err)
			return nil, status.Errorf(codes.Internal, "list vms: %v", err)
		}
		if n := countLeased(vms); n > 0 {
			log.WarnContext(ctx, "pooladmin: DeletePool failed: pool has leased VMs", "leased_count", n)
			return nil, status.Errorf(codes.FailedPrecondition, "pool %s/%s has %d leased VMs, release them or delete with force", ns, name, n)
		}
	}

	if err := s.poolMgr.StopReconcilerAndWait(ctx, name, ns); err != nil {
		log.ErrorContext(ctx, "pooladmin: DeletePool: reconciler did not stop", "error", err)
		s.restartReconciler(ctx, log, spec)
		return nil, status.FromContextError(err).Err()
	}

	vms, err := s.store.DeletePoolAndMarkVMs(ctx, name, ns, force)
	switch {
	case errors.Is(err, store.ErrPoolHasLeasedVMs):
		// A ClaimVM landed between the check above and the transaction.
		log.WarnContext(ctx, "pooladmin: DeletePool failed: pool has leased VMs")
		s.restartReconciler(ctx, log, spec)
		return nil, status.Errorf(codes.FailedPrecondition, "pool %s/%s has leased VMs, release them or delete with force", ns, name)
	case errors.Is(err, store.ErrNotFound):
		log.WarnContext(ctx, "pooladmin: DeletePool failed: pool not found")
		return nil, status.Errorf(codes.NotFound, "pool %s/%s not found", ns, name)
	case err != nil:
		log.ErrorContext(ctx, "pooladmin: DeletePool: store delete failed", "error", err)
		s.restartReconciler(ctx, log, spec)
		return nil, status.Errorf(codes.Internal, "delete pool: %v", err)
	}

	s.deletePoolVMs(ctx, log, spec, vms)

	log.InfoContext(ctx, "pooladmin: pool deleted", "vm_count", len(vms))
	return &emptypb.Empty{}, nil
}

// countLeased returns how many of vms are held by a consumer, or about to
// be: LEASED, or PRE_LEASE_HOOK_RUNNING (a ClaimVM in flight).
func countLeased(vms []*poolmgrv1alpha1.VMRecord) int {
	n := 0
	for _, vm := range vms {
		switch vm.GetPhase() {
		case poolmgrv1alpha1.VMPhase_LEASED, poolmgrv1alpha1.VMPhase_PRE_LEASE_HOOK_RUNNING:
			n++
		}
	}
	return n
}

// restartReconciler starts spec's reconciler again after a DeletePool that
// stopped it but then left the pool in place. As in CreatePool, a start
// failure is only logged: the RPC is already failing for its own reason.
func (s *PoolAdminServer) restartReconciler(ctx context.Context, log *slog.Logger, spec *poolmgrv1alpha1.PoolSpec) {
	if err := s.poolMgr.StartReconciler(spec); err != nil {
		log.ErrorContext(ctx, "pooladmin: restart reconciler failed", "error", err)
	}
}

// deletePoolVMs emits VM_DELETED_ON_POOL_DELETE for, and deletes, each VM of
// a pool that DeletePoolAndMarkVMs has just removed. It runs on a context
// detached from ctx's cancellation: the pool is already gone, so a client
// that hangs up must not stop its microVMs being deleted. A failed delete is
// only logged; the VM stays DELETING for the Sweeper to retry.
func (s *PoolAdminServer) deletePoolVMs(ctx context.Context, log *slog.Logger, spec *poolmgrv1alpha1.PoolSpec, vms []*poolmgrv1alpha1.VMRecord) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), poolDeleteCleanupTimeout)
	defer cancel()

	for _, vm := range vms {
		reconciler.EmitEvent(cleanupCtx, s.store, spec, vm.GetUid(), poolmgrv1alpha1.EventType_VM_DELETED_ON_POOL_DELETE)
		if s.flint == nil {
			continue
		}
		if err := reconciler.EnsureVMDeleted(cleanupCtx, s.store, s.flint, vm); err != nil {
			log.WarnContext(ctx, "pooladmin: DeletePool: microvm delete failed, leaving it for the sweeper", "microvm_uid", vm.GetUid(), "error", err)
		}
	}
}
```

- [ ] **Step 6: Run the API tests**

Run: `go test -race ./internal/api/`
Expected: PASS, including the unchanged `TestDeletePool`, `TestDeletePool_StopsReconciler` and `TestGetUpdateDeletePoolNotFound`.

- [ ] **Step 7: Update the e2e expectation**

In `cmd/poolmgrd/e2e_test.go`, replace the block at the end of `TestE2E_PoolLifecycle` (the comment starting `// No VM-level cordon/force-delete API exists yet` through the `FailedPrecondition` check) with:

```go
	// DeletePool drains the pool: the replenished VM goes with it.
	if _, err := pm.PoolAdmin.DeletePool(ctx, &poolmgrv1alpha1.DeletePoolRequest{Ref: ref}); err != nil {
		t.Fatalf("DeletePool on a pool with a live VM: %v", err)
	}
	waitForEvent(ctx, t, recorder, poolmgrv1alpha1.EventType_VM_DELETED_ON_POOL_DELETE)

	_, err = pm.PoolAdmin.GetPool(ctx, &poolmgrv1alpha1.GetPoolRequest{Ref: ref})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetPool after DeletePool: got err=%v, want NotFound", err)
	}
```

Run: `go vet -tags e2e ./cmd/... && go test -tags e2e ./cmd/...`
Expected: PASS. If `TestE2E_PoolLifecycle` reports the event recorder never saw `VM_DELETED_ON_POOL_DELETE` because the subscription ends with the pool, drop the `waitForEvent` line and keep the `GetPool` check.

- [ ] **Step 8: Commit**

```bash
git add internal/api internal/poolmgrctl/pool_test.go cmd/poolmgrd cmd/poolmgrctl
git commit -m "fix(api): drain a pool's VMs in DeletePool instead of refusing"
```

---

### Task 5: Runbook and full verification

**Files:**
- Modify: `docs/runbooks/e2e-manual-verification.md` (end of section 8, before `## 9.`)

**Interfaces:**
- Consumes: the behaviour from Tasks 1-4.
- Produces: nothing.

- [ ] **Step 1: Document the cleanup**

In `docs/runbooks/e2e-manual-verification.md`, after the paragraph ending ``plus `POOL_REPLENISHING`/`POOL_SIZE_BELOW_TARGET` around replenishment.`` and before `## 9.`, add:

````markdown
When you're done with `e2e-pool`, delete it. `DeletePool` drains the pool: the replenished
`AVAILABLE` VM is deleted from its flintlock host along with the pool, and a
`VM_DELETED_ON_POOL_DELETE` event is emitted for it.

```sh
grpcurl -d '{"ref": {"name": "e2e-pool", "namespace": "e2e"}}' \
  -plaintext localhost:9091 poolmgr.v1alpha1.PoolAdmin/DeletePool
```

If a VM is still leased the call fails with `FAILED_PRECONDITION`. Release the lease first, or
add `"force": true` to the request (`poolmgrctl pool delete --force`) to delete the leased VM
and end its lease as well.
````

- [ ] **Step 2: Run the whole suite**

Run: `go build ./... && go test -race ./... && go test -tags e2e ./cmd/... && golangci-lint run`
Expected: all PASS, no lint findings.

- [ ] **Step 3: Confirm generated code is current**

Run: `hack/generate-proto.sh && git status --short`
Expected: only `docs/runbooks/e2e-manual-verification.md` is modified.

- [ ] **Step 4: Commit**

```bash
git add docs/runbooks/e2e-manual-verification.md
git commit -m "docs: describe DeletePool draining in the e2e runbook"
```
