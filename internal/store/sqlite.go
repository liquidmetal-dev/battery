package store

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"modernc.org/sqlite" // also registers the "sqlite" database/sql driver
	sqlite3 "modernc.org/sqlite/lib"
)

//go:embed schema.sql
var schemaSQL string

type sqliteStore struct {
	db *sql.DB
}

// Open opens (creating if necessary) a SQLite database at path and applies
// the pool manager schema and any pending migrations.
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
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &sqliteStore{db: db}, nil
}

// migrations are the schema changes applied on top of schema.sql, which is
// the version-0 baseline. migrations[i] takes a database from
// PRAGMA user_version i to i+1. Append new migrations to the end; never edit,
// remove, or reorder one that has shipped.
var migrations = []string{
	// 1: request_id on leases, for idempotent ClaimVM. The index is partial
	// so that the many leases claimed without a request ID (stored as NULL)
	// never conflict with each other.
	`ALTER TABLE leases ADD COLUMN request_id TEXT;
	CREATE UNIQUE INDEX IF NOT EXISTS idx_leases_request_id
		ON leases (request_id) WHERE request_id IS NOT NULL;`,
	// 2: per-host TLS settings, so a host's connection details live in the
	// store rather than only in the config file. The defaults describe
	// "no TLS settings", which is what a pre-existing row had.
	`ALTER TABLE hosts ADD COLUMN tls_insecure INTEGER NOT NULL DEFAULT 0;
	ALTER TABLE hosts ADD COLUMN ca_file TEXT NOT NULL DEFAULT '';
	ALTER TABLE hosts ADD COLUMN cert_file TEXT NOT NULL DEFAULT '';
	ALTER TABLE hosts ADD COLUMN key_file TEXT NOT NULL DEFAULT '';`,
}

// migrate brings db from its current PRAGMA user_version up to
// len(migrations), applying each pending migration in its own transaction
// together with the user_version bump, so a failed migration leaves the
// database at the last version that fully applied.
func migrate(db *sql.DB) error {
	var version int
	if err := db.QueryRow(`PRAGMA user_version;`).Scan(&version); err != nil {
		return fmt.Errorf("store: read schema version: %w", err)
	}
	if version > len(migrations) {
		return fmt.Errorf("store: database schema version %d is newer than this binary supports (%d)", version, len(migrations))
	}

	for v := version; v < len(migrations); v++ {
		if err := applyMigration(db, v+1, migrations[v]); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(db *sql.DB, version int, stmts string) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("store: begin migration %d: %w", version, err)
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.Exec(stmts); err != nil {
		return fmt.Errorf("store: apply migration %d: %w", version, err)
	}
	// PRAGMA does not accept bound parameters; version is an int we control.
	if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d;`, version)); err != nil {
		return fmt.Errorf("store: set schema version %d: %w", version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit migration %d: %w", version, err)
	}
	return nil
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

func (s *sqliteStore) ListVMsByPhase(ctx context.Context, phase poolmgrv1alpha1.VMPhase) ([]*poolmgrv1alpha1.VMRecord, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT uid, pool_name, pool_namespace, flintlock_host, phase, lease_id, created_at, updated_at
		FROM vms WHERE phase = ? ORDER BY uid`, int32(phase))
	if err != nil {
		return nil, fmt.Errorf("store: query vms by phase: %w", err)
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
		INSERT INTO leases (lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at, request_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		row.leaseID, row.vmUID, row.poolName, row.poolNamespace, row.claimedAt, row.lastHeartbeatAt, row.expiresAt, row.requestID,
	)
	if isRequestIDConflict(err) {
		return ErrDuplicateRequestID
	}
	if err != nil {
		return fmt.Errorf("store: insert lease: %w", err)
	}
	return nil
}

// isRequestIDConflict reports whether err is idx_leases_request_id rejecting
// an INSERT, as opposed to any other constraint (such as a duplicate
// lease_id) that SQLite also reports as a UNIQUE violation.
func isRequestIDConflict(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) || sqliteErr.Code() != sqlite3.SQLITE_CONSTRAINT_UNIQUE {
		return false
	}
	return strings.Contains(sqliteErr.Error(), "leases.request_id")
}

func (s *sqliteStore) GetLease(ctx context.Context, leaseID string) (*poolmgrv1alpha1.LeaseRecord, error) {
	r := s.db.QueryRowContext(ctx, `
		SELECT lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at, request_id
		FROM leases WHERE lease_id = ?`, leaseID)

	var row leaseRow
	err := r.Scan(&row.leaseID, &row.vmUID, &row.poolName, &row.poolNamespace, &row.claimedAt, &row.lastHeartbeatAt, &row.expiresAt, &row.requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: query lease: %w", err)
	}
	return rowToLease(row), nil
}

func (s *sqliteStore) GetLeaseByRequestID(ctx context.Context, requestID string) (*poolmgrv1alpha1.LeaseRecord, error) {
	// An empty request ID is stored as NULL, which never matches, so this
	// returns ErrNotFound for "" without a special case.
	r := s.db.QueryRowContext(ctx, `
		SELECT lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at, request_id
		FROM leases WHERE request_id = ?`, requestID)

	var row leaseRow
	err := r.Scan(&row.leaseID, &row.vmUID, &row.poolName, &row.poolNamespace, &row.claimedAt, &row.lastHeartbeatAt, &row.expiresAt, &row.requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: query lease by request id: %w", err)
	}
	return rowToLease(row), nil
}

func (s *sqliteStore) UpdateLeaseHeartbeat(ctx context.Context, leaseID string, at time.Time, expiresAt time.Time) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE leases SET last_heartbeat_at = ?, expires_at = ? WHERE lease_id = ?`,
		at.UnixNano(), expiresAt.UnixNano(), leaseID,
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
		SELECT lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at, request_id
		FROM leases WHERE expires_at <= ? ORDER BY expires_at`, now.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("store: query expired leases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var leases []*poolmgrv1alpha1.LeaseRecord
	for rows.Next() {
		var row leaseRow
		if err := rows.Scan(&row.leaseID, &row.vmUID, &row.poolName, &row.poolNamespace, &row.claimedAt, &row.lastHeartbeatAt, &row.expiresAt, &row.requestID); err != nil {
			return nil, fmt.Errorf("store: scan lease: %w", err)
		}
		leases = append(leases, rowToLease(row))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate leases: %w", err)
	}
	return leases, nil
}

func (s *sqliteStore) ListLeases(ctx context.Context, poolRef *poolmgrv1alpha1.PoolRef) ([]*poolmgrv1alpha1.LeaseRecord, error) {
	query := `SELECT lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at, request_id FROM leases`
	var args []any
	if poolRef != nil {
		query += ` WHERE pool_name = ? AND pool_namespace = ?`
		args = append(args, poolRef.GetName(), poolRef.GetNamespace())
	}
	query += ` ORDER BY lease_id`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: query leases: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var leases []*poolmgrv1alpha1.LeaseRecord
	for rows.Next() {
		var row leaseRow
		if err := rows.Scan(&row.leaseID, &row.vmUID, &row.poolName, &row.poolNamespace, &row.claimedAt, &row.lastHeartbeatAt, &row.expiresAt, &row.requestID); err != nil {
			return nil, fmt.Errorf("store: scan lease: %w", err)
		}
		leases = append(leases, rowToLease(row))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate leases: %w", err)
	}
	return leases, nil
}

func (s *sqliteStore) DeleteLeaseIfExpired(ctx context.Context, leaseID string, now time.Time) (*poolmgrv1alpha1.LeaseRecord, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("store: begin claim tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var row leaseRow
	err = tx.QueryRowContext(ctx, `
		SELECT lease_id, vm_uid, pool_name, pool_namespace, claimed_at, last_heartbeat_at, expires_at, request_id
		FROM leases WHERE lease_id = ?`, leaseID,
	).Scan(&row.leaseID, &row.vmUID, &row.poolName, &row.poolNamespace, &row.claimedAt, &row.lastHeartbeatAt, &row.expiresAt, &row.requestID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: select lease: %w", err)
	}
	if row.expiresAt > now.UnixNano() {
		return nil, ErrLeaseNotExpired
	}

	// Guard the DELETE with "AND expires_at <= ?" and check rows affected, rather than trusting
	// the SELECT above: if a Heartbeat renewed this lease between the SELECT and here, this
	// DELETE must not remove it.
	res, err := tx.ExecContext(ctx, `DELETE FROM leases WHERE lease_id = ? AND expires_at <= ?`, leaseID, now.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("store: delete expired lease: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("store: delete expired lease rows affected: %w", err)
	}
	if n == 0 {
		return nil, ErrLeaseNotExpired
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("store: commit claim tx: %w", err)
	}
	return rowToLease(row), nil
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

func (s *sqliteStore) ListEventsSince(ctx context.Context, poolName, poolNamespace string, sinceID int64, limit int) ([]*poolmgrv1alpha1.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, pool_name, pool_namespace, vm_uid, type, created_at, payload_json
		FROM events WHERE pool_name = ? AND pool_namespace = ? AND id > ? ORDER BY id LIMIT ?`,
		poolName, poolNamespace, sinceID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: query events: %w", err)
	}
	return scanEvents(rows)
}

func (s *sqliteStore) ListAllEventsSince(ctx context.Context, sinceID int64, limit int) ([]*poolmgrv1alpha1.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, pool_name, pool_namespace, vm_uid, type, created_at, payload_json
		FROM events WHERE id > ? ORDER BY id LIMIT ?`, sinceID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: query events: %w", err)
	}
	return scanEvents(rows)
}

// scanEvents reads and closes rows produced by a `SELECT id, pool_name, pool_namespace, vm_uid,
// type, created_at, payload_json FROM events ...` query, in column order.
func scanEvents(rows *sql.Rows) ([]*poolmgrv1alpha1.Event, error) {
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

// hostColumns lists the hosts table's columns in the order hostRow.scanDest
// expects them.
const hostColumns = `name, address, cordoned, cordoned_reason, cordoned_at, updated_at,
	tls_insecure, ca_file, cert_file, key_file`

// scanDest returns pointers to row's fields in hostColumns order.
func (row *hostRow) scanDest() []any {
	return []any{
		&row.name, &row.address, &row.cordoned, &row.cordonedReason, &row.cordonedAt, &row.updatedAt,
		&row.tlsInsecure, &row.caFile, &row.certFile, &row.keyFile,
	}
}

func (s *sqliteStore) CreateHost(ctx context.Context, host *poolmgrv1alpha1.Host) error {
	row, err := hostToRow(host)
	if err != nil {
		return fmt.Errorf("store: marshal host: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO hosts (`+hostColumns+`)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.name, row.address, row.cordoned, row.cordonedReason, row.cordonedAt, row.updatedAt,
		row.tlsInsecure, row.caFile, row.certFile, row.keyFile,
	)
	if isHostNameConflict(err) {
		return ErrHostExists
	}
	if err != nil {
		return fmt.Errorf("store: insert host: %w", err)
	}
	return nil
}

// isHostNameConflict reports whether err is the hosts primary key rejecting
// an INSERT.
func isHostNameConflict(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	switch sqliteErr.Code() {
	case sqlite3.SQLITE_CONSTRAINT_PRIMARYKEY, sqlite3.SQLITE_CONSTRAINT_UNIQUE:
		return strings.Contains(sqliteErr.Error(), "hosts.name")
	}
	return false
}

func (s *sqliteStore) UpdateHost(ctx context.Context, host *poolmgrv1alpha1.Host) (*poolmgrv1alpha1.Host, error) {
	tls := host.GetTls()
	res, err := s.db.ExecContext(ctx, `
		UPDATE hosts SET address = ?, tls_insecure = ?, ca_file = ?, cert_file = ?, key_file = ?, updated_at = ?
		WHERE name = ?`,
		host.GetAddress(), tls.GetInsecure(), tls.GetCaFile(), tls.GetCertFile(), tls.GetKeyFile(),
		time.Now().UnixNano(), host.GetName(),
	)
	if err != nil {
		return nil, fmt.Errorf("store: update host: %w", err)
	}
	if err := checkRowsAffected(res); err != nil {
		return nil, err
	}

	return s.GetHost(ctx, host.GetName())
}

func (s *sqliteStore) DeleteHost(ctx context.Context, name string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin delete host tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// The checks and the delete share one transaction on the store's single
	// connection (see Open), so neither a pool write nor a ReservePlacement
	// can land between them.
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM hosts WHERE name = ?)`, name).Scan(&exists); err != nil {
		return fmt.Errorf("store: select host: %w", err)
	}
	if !exists {
		return ErrNotFound
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT pools.namespace, pools.name
		FROM pools, json_each(pools.flintlock_hosts)
		WHERE json_each.value = ?
		ORDER BY pools.namespace, pools.name`, name)
	if err != nil {
		return fmt.Errorf("store: query pools naming host: %w", err)
	}
	var pools []string
	for rows.Next() {
		var ns, pool string
		if err := rows.Scan(&ns, &pool); err != nil {
			_ = rows.Close()
			return fmt.Errorf("store: scan pool naming host: %w", err)
		}
		pools = append(pools, ns+"/"+pool)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("store: iterate pools naming host: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: iterate pools naming host: %w", err)
	}

	var count int32
	if err := tx.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM vms WHERE flintlock_host = ?)
		     + (SELECT COUNT(*) FROM placements WHERE host = ?)`,
		name, name,
	).Scan(&count); err != nil {
		return fmt.Errorf("store: count vms by host: %w", err)
	}

	if len(pools) > 0 || count > 0 {
		return &HostInUseError{Pools: pools, VMCount: count}
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM hosts WHERE name = ?`, name); err != nil {
		return fmt.Errorf("store: delete host: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit delete host tx: %w", err)
	}
	return nil
}

func (s *sqliteStore) GetHost(ctx context.Context, name string) (*poolmgrv1alpha1.Host, error) {
	r := s.db.QueryRowContext(ctx, `SELECT `+hostColumns+` FROM hosts WHERE name = ?`, name)

	var row hostRow
	err := r.Scan(row.scanDest()...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: query host: %w", err)
	}
	return rowToHost(row), nil
}

func (s *sqliteStore) ListHosts(ctx context.Context) ([]*poolmgrv1alpha1.Host, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+hostColumns+` FROM hosts ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("store: query hosts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var hosts []*poolmgrv1alpha1.Host
	for rows.Next() {
		var row hostRow
		if err := rows.Scan(row.scanDest()...); err != nil {
			return nil, fmt.Errorf("store: scan host: %w", err)
		}
		hosts = append(hosts, rowToHost(row))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate hosts: %w", err)
	}
	return hosts, nil
}

func (s *sqliteStore) SetHostCordoned(ctx context.Context, name string, cordoned bool, reason string) (*poolmgrv1alpha1.Host, error) {
	now := time.Now()

	var cordonedReason sql.NullString
	var cordonedAt sql.NullInt64
	if cordoned {
		if reason != "" {
			cordonedReason = sql.NullString{String: reason, Valid: true}
		}
		cordonedAt = sql.NullInt64{Int64: now.UnixNano(), Valid: true}
	}

	res, err := s.db.ExecContext(ctx, `
		UPDATE hosts SET cordoned = ?, cordoned_reason = ?, cordoned_at = ?, updated_at = ?
		WHERE name = ?`,
		cordoned, cordonedReason, cordonedAt, now.UnixNano(), name,
	)
	if err != nil {
		return nil, fmt.Errorf("store: set host cordoned: %w", err)
	}
	if err := checkRowsAffected(res); err != nil {
		return nil, err
	}

	return s.GetHost(ctx, name)
}

func (s *sqliteStore) ReservePlacement(ctx context.Context, id, host, poolName, poolNamespace string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin reserve placement tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	// The cordon check and the insert share one transaction on the store's
	// single connection (see Open), so SetHostCordoned's UPDATE can't land
	// between them: either it committed first and this returns
	// ErrHostCordoned, or it waits and then sees the reservation counted.
	// DeleteHost is serialized the same way: either it committed first and
	// this returns ErrHostNotRegistered, or it sees the reservation and
	// refuses.
	var cordoned bool
	err = tx.QueryRowContext(ctx, `SELECT cordoned FROM hosts WHERE name = ?`, host).Scan(&cordoned)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ErrHostNotRegistered
	case err != nil:
		return fmt.Errorf("store: select host cordoned: %w", err)
	case cordoned:
		return ErrHostCordoned
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO placements (id, host, pool_name, pool_namespace, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		id, host, poolName, poolNamespace, time.Now().UnixNano(),
	); err != nil {
		return fmt.Errorf("store: insert placement: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit reserve placement tx: %w", err)
	}
	return nil
}

func (s *sqliteStore) ReleasePlacement(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM placements WHERE id = ?`, id); err != nil {
		return fmt.Errorf("store: release placement: %w", err)
	}
	return nil
}

func (s *sqliteStore) ClearPlacements(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM placements`); err != nil {
		return fmt.Errorf("store: clear placements: %w", err)
	}
	return nil
}

func (s *sqliteStore) CountVMsByHost(ctx context.Context, name string) (int32, error) {
	var count int32
	err := s.db.QueryRowContext(ctx, `
		SELECT (SELECT COUNT(*) FROM vms WHERE flintlock_host = ?)
		     + (SELECT COUNT(*) FROM placements WHERE host = ?)`,
		name, name,
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("store: count vms by host: %w", err)
	}
	return count, nil
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
