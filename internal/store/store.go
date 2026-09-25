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

// ErrHostDrained is returned by ReservePlacement when the host was drained
// at the moment the reservation was attempted.
var ErrHostDrained = errors.New("store: host is drained")

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

	// UpsertHostIfMissing inserts a row for host if none exists for its name; otherwise it
	// refreshes the stored address to host.Address (so a host's address in the static config
	// file is kept current across restarts) while leaving its drain state untouched. Used to
	// seed the host registry from static config at startup without clobbering drain state.
	UpsertHostIfMissing(ctx context.Context, host *poolmgrv1alpha1.Host) error
	GetHost(ctx context.Context, name string) (*poolmgrv1alpha1.Host, error)
	ListHosts(ctx context.Context) ([]*poolmgrv1alpha1.Host, error)
	// SetHostDrained sets host name's drained state and reason, returning the updated host.
	// Returns ErrNotFound if no such host is registered.
	SetHostDrained(ctx context.Context, name string, drained bool, reason string) (*poolmgrv1alpha1.Host, error)
	// ListDrainedHostNames returns the set of currently-drained host names, for PickHost's
	// placement filter.
	ListDrainedHostNames(ctx context.Context) (map[string]bool, error)
	// ReservePlacement records that a VM with the given id is about to be created on host
	// for pool (poolName, poolNamespace), in the same transaction as a check that host is
	// not drained. Returns ErrHostDrained if it is, in which case nothing is recorded and
	// the caller must not create the VM there. A host with no registry row is treated as
	// not drained (DrainHost refuses unregistered hosts, so it can never be). The
	// reservation counts toward CountVMsByHost until ReleasePlacement(id).
	ReservePlacement(ctx context.Context, id, host, poolName, poolNamespace string) error
	// ReleasePlacement removes the reservation for id. Idempotent: releasing an id that
	// doesn't exist is not an error.
	ReleasePlacement(ctx context.Context, id string) error
	// ClearPlacements removes every reservation. Called once at poolmgrd startup: nothing
	// can be in flight then, so any surviving row was left behind by a crash.
	ClearPlacements(ctx context.Context) error
	// CountVMsByHost returns the number of VM records placed on host name in any phase
	// (including DELETING, QUARANTINED and FAILED, all of which may still exist on the host)
	// plus in-flight placement reservations for it, across all pools. A drained host whose
	// count is 0 has nothing left on it and nothing on the way, so it is safe to take down.
	CountVMsByHost(ctx context.Context, name string) (int32, error)

	Close() error
}
