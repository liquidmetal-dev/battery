// Package store provides the SQLite-backed persistence layer for pool
// manager data: pools, VMs, leases, and the events outbox. Callers depend
// only on the Store interface so the backing database can change without
// touching reconciler or API code.
package store

import (
	"context"
	"errors"
	"fmt"
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

// ErrDuplicateRequestID is returned by CreateLease when another lease
// already carries the same non-empty request ID.
var ErrDuplicateRequestID = errors.New("store: duplicate lease request id")

// ErrHostCordoned is returned by ReservePlacement when the host was cordoned
// at the moment the reservation was attempted.
var ErrHostCordoned = errors.New("store: host is cordoned")

// ErrHostExists is returned by CreateHost when a host with the same name is
// already registered.
var ErrHostExists = errors.New("store: host already exists")

// HostInUseError is returned by DeleteHost when something still depends on
// the host: pool specs that name it in flintlock_hosts, or VM records and
// placement reservations counted against it.
type HostInUseError struct {
	// Pools lists each pool that names the host, as "namespace/name".
	Pools []string
	// VMCount is CountVMsByHost for the host.
	VMCount int32
}

func (e *HostInUseError) Error() string {
	return fmt.Sprintf("store: host in use: named by pools %v, %d vms", e.Pools, e.VMCount)
}

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

	// CreateLease stores l. An empty l.RequestId is stored as no request ID.
	// Returns ErrDuplicateRequestID if another lease already has l's non-empty
	// RequestId.
	CreateLease(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord) error
	GetLease(ctx context.Context, leaseID string) (*poolmgrv1alpha1.LeaseRecord, error)
	// GetLeaseByRequestID returns the lease created with requestID. Returns
	// ErrNotFound if there is none, including when requestID is empty.
	GetLeaseByRequestID(ctx context.Context, requestID string) (*poolmgrv1alpha1.LeaseRecord, error)
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
	// refreshes the stored address and TLS settings to host's (so a host's connection details
	// in the static config file are kept current across restarts) while leaving its cordon
	// state untouched. Used to seed the host registry from static config at startup without
	// clobbering cordon state.
	UpsertHostIfMissing(ctx context.Context, host *poolmgrv1alpha1.Host) error
	// CreateHost stores host as given, including its cordon fields. Returns ErrHostExists if
	// a host with the same name is already registered.
	CreateHost(ctx context.Context, host *poolmgrv1alpha1.Host) error
	// UpdateHost replaces the address and TLS settings of the host named host.Name, stamps
	// its updated_at with the current time, and returns the updated host. Cordon fields and
	// host.UpdatedAt are ignored: SetHostCordoned owns cordon state. Returns ErrNotFound if
	// no such host is registered.
	UpdateHost(ctx context.Context, host *poolmgrv1alpha1.Host) (*poolmgrv1alpha1.Host, error)
	// DeleteHost removes host name's registry row, but only if nothing depends on it: in the
	// same transaction it checks that no pool spec names the host in flintlock_hosts and
	// that CountVMsByHost is 0, returning a *HostInUseError (and deleting nothing) if
	// either fails. A ReservePlacement can therefore never land between the check and the
	// delete. Returns ErrNotFound if no such host is registered.
	DeleteHost(ctx context.Context, name string) error
	GetHost(ctx context.Context, name string) (*poolmgrv1alpha1.Host, error)
	ListHosts(ctx context.Context) ([]*poolmgrv1alpha1.Host, error)
	// SetHostCordoned sets host name's cordoned state and reason, returning the updated host.
	// Returns ErrNotFound if no such host is registered.
	SetHostCordoned(ctx context.Context, name string, cordoned bool, reason string) (*poolmgrv1alpha1.Host, error)
	// ListCordonedHostNames returns the set of currently-cordoned host names, for PickHost's
	// placement filter.
	ListCordonedHostNames(ctx context.Context) (map[string]bool, error)
	// ReservePlacement records that a VM with the given id is about to be created on host
	// for pool (poolName, poolNamespace), in the same transaction as a check that host is
	// not cordoned. Returns ErrHostCordoned if it is, in which case nothing is recorded and
	// the caller must not create the VM there. A host with no registry row is treated as
	// not cordoned (CordonHost refuses unregistered hosts, so it can never be). The
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
	// plus in-flight placement reservations for it, across all pools. A cordoned host whose
	// count is 0 has nothing left on it and nothing on the way, so it is safe to take down.
	CountVMsByHost(ctx context.Context, name string) (int32, error)

	Close() error
}
