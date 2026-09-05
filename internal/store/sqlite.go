package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	_ "modernc.org/sqlite" // registers the "sqlite" database/sql driver
)

//go:embed schema.sql
var schemaSQL string

type sqliteStore struct {
	db *sql.DB
}

// Open opens (creating if necessary) a SQLite database at path and applies
// the pool manager schema.
func Open(path string) (Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("store: open sqlite db: %w", err)
	}

	// A single *sql.DB with a single connection avoids SQLITE_BUSY errors
	// from concurrent writers, since the modernc.org/sqlite driver does not
	// itself serialize access the way a busy-timeout PRAGMA alone would.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: enable foreign keys: %w", err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode = WAL;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: enable WAL mode: %w", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("store: apply schema: %w", err)
	}

	return &sqliteStore{db: db}, nil
}

func (s *sqliteStore) Close() error {
	return s.db.Close()
}

func (s *sqliteStore) CreatePool(ctx context.Context, p *poolmgrv1alpha1.PoolSpec) error {
	row, err := poolToRow(p)
	if err != nil {
		return fmt.Errorf("store: marshal pool: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO pools (
			name, namespace, size, flintlock_hosts, microvm_template,
			replenishment_strategy, create_commands, pre_lease_commands,
			hook_failure_policy, heartbeat_interval_ns, heartbeat_expiry_threshold_ns
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.name, row.namespace, row.size, row.flintlockHosts, row.microvmTemplate,
		row.replenishmentStrategy, row.createCommands, row.preLeaseCommands,
		row.hookFailurePolicy, row.heartbeatIntervalNs, row.heartbeatExpiryThresholdNs,
	)
	if err != nil {
		return fmt.Errorf("store: insert pool: %w", err)
	}
	return nil
}

func (s *sqliteStore) GetPool(ctx context.Context, name, namespace string) (*poolmgrv1alpha1.PoolSpec, error) {
	r := s.db.QueryRowContext(ctx, `
		SELECT name, namespace, size, flintlock_hosts, microvm_template,
			replenishment_strategy, create_commands, pre_lease_commands,
			hook_failure_policy, heartbeat_interval_ns, heartbeat_expiry_threshold_ns
		FROM pools WHERE name = ? AND namespace = ?`, name, namespace)

	var row poolRow
	err := r.Scan(&row.name, &row.namespace, &row.size, &row.flintlockHosts, &row.microvmTemplate,
		&row.replenishmentStrategy, &row.createCommands, &row.preLeaseCommands,
		&row.hookFailurePolicy, &row.heartbeatIntervalNs, &row.heartbeatExpiryThresholdNs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: query pool: %w", err)
	}

	return rowToPool(row)
}

func (s *sqliteStore) ListPools(ctx context.Context) ([]*poolmgrv1alpha1.PoolSpec, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name, namespace, size, flintlock_hosts, microvm_template,
			replenishment_strategy, create_commands, pre_lease_commands,
			hook_failure_policy, heartbeat_interval_ns, heartbeat_expiry_threshold_ns
		FROM pools ORDER BY namespace, name`)
	if err != nil {
		return nil, fmt.Errorf("store: query pools: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var pools []*poolmgrv1alpha1.PoolSpec
	for rows.Next() {
		var row poolRow
		if err := rows.Scan(&row.name, &row.namespace, &row.size, &row.flintlockHosts, &row.microvmTemplate,
			&row.replenishmentStrategy, &row.createCommands, &row.preLeaseCommands,
			&row.hookFailurePolicy, &row.heartbeatIntervalNs, &row.heartbeatExpiryThresholdNs); err != nil {
			return nil, fmt.Errorf("store: scan pool: %w", err)
		}
		p, err := rowToPool(row)
		if err != nil {
			return nil, err
		}
		pools = append(pools, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate pools: %w", err)
	}
	return pools, nil
}

func (s *sqliteStore) UpdatePool(ctx context.Context, p *poolmgrv1alpha1.PoolSpec) error {
	row, err := poolToRow(p)
	if err != nil {
		return fmt.Errorf("store: marshal pool: %w", err)
	}

	res, err := s.db.ExecContext(ctx, `
		UPDATE pools SET
			size = ?, flintlock_hosts = ?, microvm_template = ?,
			replenishment_strategy = ?, create_commands = ?, pre_lease_commands = ?,
			hook_failure_policy = ?, heartbeat_interval_ns = ?, heartbeat_expiry_threshold_ns = ?
		WHERE name = ? AND namespace = ?`,
		row.size, row.flintlockHosts, row.microvmTemplate,
		row.replenishmentStrategy, row.createCommands, row.preLeaseCommands,
		row.hookFailurePolicy, row.heartbeatIntervalNs, row.heartbeatExpiryThresholdNs,
		row.name, row.namespace,
	)
	if err != nil {
		return fmt.Errorf("store: update pool: %w", err)
	}
	return checkRowsAffected(res)
}

func (s *sqliteStore) DeletePool(ctx context.Context, name, namespace string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM pools WHERE name = ? AND namespace = ?`, name, namespace)
	if err != nil {
		return fmt.Errorf("store: delete pool: %w", err)
	}
	return checkRowsAffected(res)
}

func (s *sqliteStore) CreateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error {
	row, err := vmToRow(v)
	if err != nil {
		return fmt.Errorf("store: marshal vm: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO vms (uid, pool_name, pool_namespace, flintlock_host, phase, lease_id, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		row.uid, row.poolName, row.poolNamespace, row.flintlockHost, row.phase, row.leaseID, row.createdAt, row.updatedAt,
	)
	if err != nil {
		return fmt.Errorf("store: insert vm: %w", err)
	}
	return nil
}

func (s *sqliteStore) GetVM(ctx context.Context, uid string) (*poolmgrv1alpha1.VMRecord, error) {
	r := s.db.QueryRowContext(ctx, `
		SELECT uid, pool_name, pool_namespace, flintlock_host, phase, lease_id, created_at, updated_at
		FROM vms WHERE uid = ?`, uid)

	var row vmRow
	err := r.Scan(&row.uid, &row.poolName, &row.poolNamespace, &row.flintlockHost, &row.phase, &row.leaseID, &row.createdAt, &row.updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: query vm: %w", err)
	}
	return rowToVM(row), nil
}

func (s *sqliteStore) ListVMsByPool(ctx context.Context, poolName, poolNamespace string, phase *poolmgrv1alpha1.VMPhase) ([]*poolmgrv1alpha1.VMRecord, error) {
	query := `SELECT uid, pool_name, pool_namespace, flintlock_host, phase, lease_id, created_at, updated_at
		FROM vms WHERE pool_name = ? AND pool_namespace = ?`
	args := []any{poolName, poolNamespace}
	if phase != nil {
		query += ` AND phase = ?`
		args = append(args, int32(*phase))
	}
	query += ` ORDER BY uid`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: query vms: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var vms []*poolmgrv1alpha1.VMRecord
	for rows.Next() {
		var row vmRow
		if err := rows.Scan(&row.uid, &row.poolName, &row.poolNamespace, &row.flintlockHost, &row.phase, &row.leaseID, &row.createdAt, &row.updatedAt); err != nil {
			return nil, fmt.Errorf("store: scan vm: %w", err)
		}
		vms = append(vms, rowToVM(row))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate vms: %w", err)
	}
	return vms, nil
}

func (s *sqliteStore) UpdateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error {
	row, err := vmToRow(v)
	if err != nil {
		return fmt.Errorf("store: marshal vm: %w", err)
	}

	res, err := s.db.ExecContext(ctx, `
		UPDATE vms SET pool_name = ?, pool_namespace = ?, flintlock_host = ?, phase = ?, lease_id = ?, updated_at = ?
		WHERE uid = ?`,
		row.poolName, row.poolNamespace, row.flintlockHost, row.phase, row.leaseID, row.updatedAt, row.uid,
	)
	if err != nil {
		return fmt.Errorf("store: update vm: %w", err)
	}
	return checkRowsAffected(res)
}

func (s *sqliteStore) DeleteVM(ctx context.Context, uid string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM vms WHERE uid = ?`, uid)
	if err != nil {
		return fmt.Errorf("store: delete vm: %w", err)
	}
	return checkRowsAffected(res)
}

func (s *sqliteStore) ClaimAvailableVM(ctx context.Context, poolName, poolNamespace string) (*poolmgrv1alpha1.VMRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin claim tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var uid string
	err = tx.QueryRowContext(ctx, `
		SELECT uid FROM vms WHERE pool_name = ? AND pool_namespace = ? AND phase = ? LIMIT 1`,
		poolName, poolNamespace, int32(poolmgrv1alpha1.VMPhase_AVAILABLE),
	).Scan(&uid)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoAvailableVM
	}
	if err != nil {
		return nil, fmt.Errorf("store: select available vm: %w", err)
	}

	// Guard the UPDATE with "AND phase = AVAILABLE" and check rows affected, rather than
	// trusting the SELECT above: if some other writer claimed this uid between the SELECT
	// and here, this UPDATE must not silently re-claim it too.
	now := time.Now().UnixNano()
	res, err := tx.ExecContext(ctx, `
		UPDATE vms SET phase = ?, updated_at = ? WHERE uid = ? AND phase = ?`,
		int32(poolmgrv1alpha1.VMPhase_LEASED), now, uid, int32(poolmgrv1alpha1.VMPhase_AVAILABLE),
	)
	if err != nil {
		return nil, fmt.Errorf("store: claim vm: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("store: claim vm rows affected: %w", err)
	}
	if n == 0 {
		return nil, ErrNoAvailableVM
	}

	var row vmRow
	err = tx.QueryRowContext(ctx, `
		SELECT uid, pool_name, pool_namespace, flintlock_host, phase, lease_id, created_at, updated_at
		FROM vms WHERE uid = ?`, uid,
	).Scan(&row.uid, &row.poolName, &row.poolNamespace, &row.flintlockHost, &row.phase, &row.leaseID, &row.createdAt, &row.updatedAt)
	if err != nil {
		return nil, fmt.Errorf("store: reload claimed vm: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: commit claim tx: %w", err)
	}

	return rowToVM(row), nil
}

func (s *sqliteStore) CreateLease(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord) error {
	row, err := leaseToRow(l)
	if err != nil {
		return fmt.Errorf("store: marshal lease: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO leases (lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		row.leaseID, row.vmUID, row.poolName, row.poolNamespace, row.claimedAt, row.lastHeartbeatAt, row.expiresAt,
	)
	if err != nil {
		return fmt.Errorf("store: insert lease: %w", err)
	}
	return nil
}

func (s *sqliteStore) GetLease(ctx context.Context, leaseID string) (*poolmgrv1alpha1.LeaseRecord, error) {
	r := s.db.QueryRowContext(ctx, `
		SELECT lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at
		FROM leases WHERE lease_id = ?`, leaseID)

	var row leaseRow
	err := r.Scan(&row.leaseID, &row.vmUID, &row.poolName, &row.poolNamespace, &row.claimedAt, &row.lastHeartbeatAt, &row.expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: query lease: %w", err)
	}
	return rowToLease(row), nil
}

func (s *sqliteStore) UpdateLeaseHeartbeat(ctx context.Context, leaseID string, at time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE leases SET last_heartbeat_at = ? WHERE lease_id = ?`,
		at.UnixNano(), leaseID,
	)
	if err != nil {
		return fmt.Errorf("store: update lease heartbeat: %w", err)
	}
	return checkRowsAffected(res)
}

func (s *sqliteStore) DeleteLease(ctx context.Context, leaseID string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM leases WHERE lease_id = ?`, leaseID)
	if err != nil {
		return fmt.Errorf("store: delete lease: %w", err)
	}
	return checkRowsAffected(res)
}

func (s *sqliteStore) ListExpiredLeases(ctx context.Context, now time.Time) ([]*poolmgrv1alpha1.LeaseRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at
		FROM leases WHERE expires_at <= ? ORDER BY expires_at`, now.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("store: query expired leases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var leases []*poolmgrv1alpha1.LeaseRecord
	for rows.Next() {
		var row leaseRow
		if err := rows.Scan(&row.leaseID, &row.vmUID, &row.poolName, &row.poolNamespace, &row.claimedAt, &row.lastHeartbeatAt, &row.expiresAt); err != nil {
			return nil, fmt.Errorf("store: scan lease: %w", err)
		}
		leases = append(leases, rowToLease(row))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate leases: %w", err)
	}
	return leases, nil
}

func (s *sqliteStore) AppendEvent(ctx context.Context, e *poolmgrv1alpha1.Event) error {
	createdAt, err := requireTimestamp("created_at", e.GetCreatedAt())
	if err != nil {
		return fmt.Errorf("store: marshal event: %w", err)
	}

	res, err := s.db.ExecContext(ctx, `
		INSERT INTO events (pool_name, pool_namespace, vm_uid, type, created_at, payload_json)
		VALUES (?, ?, ?, ?, ?, ?)`,
		e.GetPoolName(), e.GetPoolNamespace(), e.GetVmUid(), int32(e.GetType()), createdAt.UnixNano(), e.GetPayloadJson(),
	)
	if err != nil {
		return fmt.Errorf("store: insert event: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("store: get event id: %w", err)
	}
	e.Id = id
	return nil
}

func (s *sqliteStore) ListEventsSince(ctx context.Context, poolName, poolNamespace string, sinceID int64) ([]*poolmgrv1alpha1.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, pool_name, pool_namespace, vm_uid, type, created_at, payload_json
		FROM events WHERE pool_name = ? AND pool_namespace = ? AND id > ? ORDER BY id`, poolName, poolNamespace, sinceID)
	if err != nil {
		return nil, fmt.Errorf("store: query events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var events []*poolmgrv1alpha1.Event
	for rows.Next() {
		var (
			id            int64
			pn, ns, vmUID string
			typ           int32
			createdAt     int64
			payload       string
		)
		if err := rows.Scan(&id, &pn, &ns, &vmUID, &typ, &createdAt, &payload); err != nil {
			return nil, fmt.Errorf("store: scan event: %w", err)
		}
		events = append(events, &poolmgrv1alpha1.Event{
			Id:            id,
			PoolName:      pn,
			PoolNamespace: ns,
			VmUid:         vmUID,
			Type:          poolmgrv1alpha1.EventType(typ),
			CreatedAt:     timestamppb.New(time.Unix(0, createdAt)),
			PayloadJson:   payload,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate events: %w", err)
	}
	return events, nil
}

func checkRowsAffected(res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: rows affected: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
