// Package store provides the SQLite-backed persistence layer for pool
// manager data: pools, VMs, leases, and the events outbox. Callers depend
// only on the Store interface so the backing database can change without
// touching reconciler or API code.
package store

import (
	"context"
	"errors"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// ErrNotFound is returned when a lookup by key finds no matching row.
var ErrNotFound = errors.New("store: not found")

// ErrNoAvailableVM is returned by ClaimAvailableVM when no VM in the pool
// is currently in the AVAILABLE phase.
var ErrNoAvailableVM = errors.New("store: no available vm in pool")

// ErrLeaseNotExpired is returned by DeleteLeaseIfExpired when the lease's
// expiry was extended (by a Heartbeat) since the caller last observed it.
var ErrLeaseNotExpired = errors.New("store: lease not expired")

// Store is the repository interface for pool manager persistence.
type Store interface {
	CreatePool(ctx context.Context, p *poolmgrv1alpha1.PoolSpec) error
	GetPool(ctx context.Context, name, namespace string) (*poolmgrv1alpha1.PoolSpec, error)
	ListPools(ctx context.Context) ([]*poolmgrv1alpha1.PoolSpec, error)
	UpdatePool(ctx context.Context, p *poolmgrv1alpha1.PoolSpec) error
	DeletePool(ctx context.Context, name, namespace string) error

	CreateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error
	GetVM(ctx context.Context, uid string) (*poolmgrv1alpha1.VMRecord, error)
	ListVMsByPool(ctx context.Context, poolName, poolNamespace string, phase *poolmgrv1alpha1.VMPhase) ([]*poolmgrv1alpha1.VMRecord, error)
	UpdateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error
	DeleteVM(ctx context.Context, uid string) error
	// ClaimAvailableVM atomically selects one AVAILABLE VM in the pool identified by
	// (poolName, poolNamespace) and marks it LEASED, returning the updated record.
	// Returns ErrNoAvailableVM if no VM in the pool is currently AVAILABLE.
	ClaimAvailableVM(ctx context.Context, poolName, poolNamespace string) (*poolmgrv1alpha1.VMRecord, error)

	CreateLease(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord) error
	GetLease(ctx context.Context, leaseID string) (*poolmgrv1alpha1.LeaseRecord, error)
	// UpdateLeaseHeartbeat records a heartbeat at `at` and extends the lease's expiry to
	// expiresAt (computed by the caller from the lease's pool's heartbeat_expiry_threshold).
	UpdateLeaseHeartbeat(ctx context.Context, leaseID string, at time.Time, expiresAt time.Time) error
	DeleteLease(ctx context.Context, leaseID string) error
	ListExpiredLeases(ctx context.Context, now time.Time) ([]*poolmgrv1alpha1.LeaseRecord, error)
	// ListLeases returns every lease, optionally filtered to one pool
	// (poolRef == nil means unfiltered), ordered by lease_id.
	ListLeases(ctx context.Context, poolRef *poolmgrv1alpha1.PoolRef) ([]*poolmgrv1alpha1.LeaseRecord, error)
	// DeleteLeaseIfExpired re-checks leaseID's expiry against now and, only if still expired,
	// deletes the lease row and returns the record as it was just before deletion. Returns
	// ErrLeaseNotExpired if a heartbeat renewed the lease's expiry since the caller last observed
	// it (the caller should treat the lease as alive and skip it), or ErrNotFound if no such
	// lease exists. Used to atomically "claim" an expired lease for deletion without racing a
	// concurrent Heartbeat call.
	DeleteLeaseIfExpired(ctx context.Context, leaseID string, now time.Time) (*poolmgrv1alpha1.LeaseRecord, error)

	AppendEvent(ctx context.Context, e *poolmgrv1alpha1.Event) error
	// ListEventsSince returns up to limit events for (poolName, poolNamespace) with id >
	// sinceID, ordered by id. Callers paging through a large outbox should re-call with
	// sinceID advanced to the last returned event's id until fewer than limit rows come back.
	ListEventsSince(ctx context.Context, poolName, poolNamespace string, sinceID int64, limit int) ([]*poolmgrv1alpha1.Event, error)
	// ListAllEventsSince returns up to limit events for every pool with id > sinceID, ordered
	// by id, for subscribers with no pool filter. See ListEventsSince re: paging.
	ListAllEventsSince(ctx context.Context, sinceID int64, limit int) ([]*poolmgrv1alpha1.Event, error)

	// ListVMsByPhase returns all VMs (across all pools) currently in phase.
	ListVMsByPhase(ctx context.Context, phase poolmgrv1alpha1.VMPhase) ([]*poolmgrv1alpha1.VMRecord, error)

	Close() error
}
