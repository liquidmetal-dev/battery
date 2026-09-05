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

// Store is the repository interface for pool manager persistence.
type Store interface {
	CreatePool(ctx context.Context, p *poolmgrv1alpha1.PoolSpec) error
	GetPool(ctx context.Context, name, namespace string) (*poolmgrv1alpha1.PoolSpec, error)
	ListPools(ctx context.Context) ([]*poolmgrv1alpha1.PoolSpec, error)
	UpdatePool(ctx context.Context, p *poolmgrv1alpha1.PoolSpec) error
	DeletePool(ctx context.Context, name, namespace string) error

	CreateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error
	GetVM(ctx context.Context, uid string) (*poolmgrv1alpha1.VMRecord, error)
	ListVMsByPool(ctx context.Context, poolName string, phase *poolmgrv1alpha1.VMPhase) ([]*poolmgrv1alpha1.VMRecord, error)
	UpdateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error
	DeleteVM(ctx context.Context, uid string) error
	// ClaimAvailableVM atomically selects one AVAILABLE VM in poolName and
	// marks it LEASED, returning the updated record. Returns ErrNoAvailableVM
	// if no VM in the pool is currently AVAILABLE.
	ClaimAvailableVM(ctx context.Context, poolName string) (*poolmgrv1alpha1.VMRecord, error)

	CreateLease(ctx context.Context, l *poolmgrv1alpha1.LeaseRecord) error
	GetLease(ctx context.Context, leaseID string) (*poolmgrv1alpha1.LeaseRecord, error)
	UpdateLeaseHeartbeat(ctx context.Context, leaseID string, at time.Time) error
	DeleteLease(ctx context.Context, leaseID string) error
	ListExpiredLeases(ctx context.Context, now time.Time) ([]*poolmgrv1alpha1.LeaseRecord, error)

	AppendEvent(ctx context.Context, e *poolmgrv1alpha1.Event) error
	ListEventsSince(ctx context.Context, poolName string, sinceID int64) ([]*poolmgrv1alpha1.Event, error)

	Close() error
}
