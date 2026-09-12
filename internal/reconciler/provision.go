package reconciler

import (
	"context"
	"errors"
	"fmt"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/metrics"
	"github.com/liquidmetal-dev/battery/internal/store"
)

// hookCreate/hookPreLease name the two hooks metrics are labeled by,
// matching the design doc's poolmgr_hook_duration_seconds{hook} and
// poolmgr_hook_failures_total{hook} label values.
const (
	hookCreate   = "create"
	hookPreLease = "pre_lease"
)

// ErrCreateTimedOut is returned when a newly created microvm doesn't reach
// the CREATED state within ProvisionConfig.CreatePollTimeout.
var ErrCreateTimedOut = errors.New("reconciler: timed out waiting for microvm to be created")

// ErrCreateFailed is returned when flintlock reports the microvm's state as
// FAILED while polling for creation.
var ErrCreateFailed = errors.New("reconciler: microvm create failed")

// ErrHookFailed is returned when a create_command errors or exits non-zero,
// or the guest-agent never becomes reachable.
var ErrHookFailed = errors.New("reconciler: create hook failed")

// hookFailureCleanupTimeout bounds ApplyHookFailurePolicy's own cleanup
// work, run on a context detached from cancellation of its caller's ctx -
// see ApplyHookFailurePolicy's comment for why.
const hookFailureCleanupTimeout = 30 * time.Second

// ProvisionConfig bounds the timing of a single Provision call. Zero-valued
// fields are replaced with DefaultProvisionConfig's values by
// NewProvisioner.
type ProvisionConfig struct {
	// CreatePollInterval/CreatePollTimeout bound polling GetMicroVM while the
	// microvm is not yet CREATED.
	CreatePollInterval time.Duration
	CreatePollTimeout  time.Duration
	// GuestAgentInterval/GuestAgentTimeout bound flintlockclient.WaitReady.
	GuestAgentInterval time.Duration
	GuestAgentTimeout  time.Duration
	// ExecTimeoutSeconds bounds each create_command's server-side run time.
	// 0 means no server-side timeout.
	ExecTimeoutSeconds int32
}

// DefaultProvisionConfig returns reasonable defaults for production use.
func DefaultProvisionConfig() ProvisionConfig {
	return ProvisionConfig{
		CreatePollInterval: 2 * time.Second,
		CreatePollTimeout:  60 * time.Second,
		GuestAgentInterval: 1 * time.Second,
		GuestAgentTimeout:  30 * time.Second,
	}
}

// Provisioner runs the provisioning pipeline for a single pool: create the
// microvm in flintlock, wait for it to boot, wait for the guest-agent, run
// the pool's create_commands, and apply hook_failure_policy on any failure.
type Provisioner struct {
	store   store.Store
	flint   *flintlockclient.Pool
	cfg     ProvisionConfig
	metrics *metrics.Registry
}

// NewProvisioner returns a Provisioner backed by st and flint. Zero-valued
// fields of cfg are replaced with DefaultProvisionConfig's values. If m is
// nil, a fresh unshared Registry is used (metrics recorded but never
// scraped) - most tests use this since they don't assert on metrics.
func NewProvisioner(st store.Store, flint *flintlockclient.Pool, cfg ProvisionConfig, m *metrics.Registry) *Provisioner {
	def := DefaultProvisionConfig()
	if cfg.CreatePollInterval <= 0 {
		cfg.CreatePollInterval = def.CreatePollInterval
	}
	if cfg.CreatePollTimeout <= 0 {
		cfg.CreatePollTimeout = def.CreatePollTimeout
	}
	if cfg.GuestAgentInterval <= 0 {
		cfg.GuestAgentInterval = def.GuestAgentInterval
	}
	if cfg.GuestAgentTimeout <= 0 {
		cfg.GuestAgentTimeout = def.GuestAgentTimeout
	}
	if m == nil {
		m = metrics.NewRegistry()
	}
	return &Provisioner{store: st, flint: flint, cfg: cfg, metrics: m}
}

// Provision runs the full pipeline for one new VM in pool, placing it on
// the least-loaded eligible host. It returns nil only once the VM is
// persisted as AVAILABLE; any failure along the way is handled per
// pool.HookFailurePolicy (the VM is deleted or quarantined and a
// VM_HOOK_FAILED event is emitted) and also returned as an error for the
// caller to log.
func (p *Provisioner) Provision(ctx context.Context, pool *poolmgrv1alpha1.PoolSpec) error {
	start := time.Now()
	defer func() { p.metrics.ObserveProvisionDuration(pool.GetName(), pool.GetNamespace(), time.Since(start)) }()

	drained, err := p.store.ListDrainedHostNames(ctx)
	if err != nil {
		return fmt.Errorf("reconciler: provision: %w", err)
	}

	host, err := PickHost(ctx, p.store, pool, drained)
	if err != nil {
		return err
	}

	client, err := p.flint.Client(host)
	if err != nil {
		return fmt.Errorf("reconciler: provision: %w", err)
	}

	spec, ok := proto.Clone(pool.GetMicrovmTemplate()).(*flintlocktypes.MicroVMSpec)
	if !ok || spec == nil {
		spec = &flintlocktypes.MicroVMSpec{}
	}
	spec.AllowGuestAgent = true

	createResp, err := client.CreateMicroVM(ctx, &microvmv1alpha1.CreateMicroVMRequest{Microvm: spec})
	if err != nil {
		return fmt.Errorf("reconciler: provision: CreateMicroVM: %w", err)
	}
	uid := createResp.GetMicrovm().GetSpec().GetUid()
	if uid == "" {
		return fmt.Errorf("reconciler: provision: CreateMicroVM returned no uid")
	}

	now := timestamppb.Now()
	vm := &poolmgrv1alpha1.VMRecord{
		Uid:           uid,
		PoolName:      pool.GetName(),
		PoolNamespace: pool.GetNamespace(),
		FlintlockHost: host,
		Phase:         poolmgrv1alpha1.VMPhase_PROVISIONING,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := p.store.CreateVM(ctx, vm); err != nil {
		// No VMRecord was persisted, so there's nothing to quarantine and
		// hook_failure_policy doesn't apply: best-effort delete the
		// now-orphaned microvm before returning, regardless of policy.
		_, _ = client.DeleteMicroVM(ctx, &microvmv1alpha1.DeleteMicroVMRequest{Uid: uid})
		return fmt.Errorf("reconciler: provision: CreateVM: %w", err)
	}
	EmitEvent(ctx, p.store, pool, uid, poolmgrv1alpha1.EventType_VM_PROVISIONED)

	if err := p.waitCreated(ctx, client, uid); err != nil {
		ApplyHookFailurePolicy(ctx, p.store, p.flint, pool, vm, hookCreate, p.metrics)
		return err
	}

	if err := p.updatePhase(ctx, pool, vm, poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING); err != nil {
		return err
	}

	execClient, err := p.flint.ExecClient(host)
	if err != nil {
		err = fmt.Errorf("reconciler: provision: %w", err)
		ApplyHookFailurePolicy(ctx, p.store, p.flint, pool, vm, hookCreate, p.metrics)
		return err
	}

	readyCtx, cancel := context.WithTimeout(ctx, p.cfg.GuestAgentTimeout)
	err = flintlockclient.WaitReady(readyCtx, execClient, uid, p.cfg.GuestAgentInterval)
	cancel()
	if err != nil {
		err = fmt.Errorf("%w: guest-agent not ready: %w", ErrHookFailed, err)
		ApplyHookFailurePolicy(ctx, p.store, p.flint, pool, vm, hookCreate, p.metrics)
		return err
	}

	hookStart := time.Now()
	for _, cmd := range pool.GetCreateCommands() {
		result, err := flintlockclient.Exec(ctx, execClient, uid, cmd, flintlockclient.ExecOptions{TimeoutSeconds: p.cfg.ExecTimeoutSeconds})
		if err != nil {
			err = fmt.Errorf("%w: %q: %w", ErrHookFailed, cmd, err)
			ApplyHookFailurePolicy(ctx, p.store, p.flint, pool, vm, hookCreate, p.metrics)
			return err
		}
		if result.ExitCode != 0 {
			err = fmt.Errorf("%w: %q: exit code %d", ErrHookFailed, cmd, result.ExitCode)
			ApplyHookFailurePolicy(ctx, p.store, p.flint, pool, vm, hookCreate, p.metrics)
			return err
		}
	}
	p.metrics.ObserveHookDuration(hookCreate, pool.GetName(), pool.GetNamespace(), time.Since(hookStart))

	if err := p.updatePhase(ctx, pool, vm, poolmgrv1alpha1.VMPhase_AVAILABLE); err != nil {
		return err
	}
	EmitEvent(ctx, p.store, pool, uid, poolmgrv1alpha1.EventType_VM_AVAILABLE)
	return nil
}

// updatePhase persists vm's new phase in both the store and the in-memory
// record passed by callers, so subsequent steps (and fail's quarantine
// path) see the current phase. If the persist itself fails, that's still a
// provisioning failure: apply pool.HookFailurePolicy rather than leaving
// the microvm running and the VMRecord stuck in its previous phase.
func (p *Provisioner) updatePhase(ctx context.Context, pool *poolmgrv1alpha1.PoolSpec, vm *poolmgrv1alpha1.VMRecord, phase poolmgrv1alpha1.VMPhase) error {
	vm.Phase = phase
	vm.UpdatedAt = timestamppb.Now()
	if err := p.store.UpdateVM(ctx, vm); err != nil {
		err = fmt.Errorf("reconciler: provision: UpdateVM: %w", err)
		ApplyHookFailurePolicy(ctx, p.store, p.flint, pool, vm, hookCreate, p.metrics)
		return err
	}
	return nil
}

// waitCreated polls GetMicroVM until the microvm's state is CREATED, or
// returns ErrCreateFailed/ErrCreateTimedOut.
func (p *Provisioner) waitCreated(ctx context.Context, client microvmv1alpha1.MicroVMClient, uid string) error {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.CreatePollTimeout)
	defer cancel()

	ticker := time.NewTicker(p.cfg.CreatePollInterval)
	defer ticker.Stop()

	for {
		resp, err := client.GetMicroVM(ctx, &microvmv1alpha1.GetMicroVMRequest{Uid: uid})
		if err != nil {
			if status.Code(err) == codes.DeadlineExceeded {
				// The bounded ctx expired mid-RPC, racing our own ctx.Done()
				// check below: classify it the same way regardless of which
				// one noticed first, so callers get a consistent error.
				// (ctx.Err() isn't reliable here: its deadline-timer
				// callback runs asynchronously and can still read nil for
				// a few scheduler ticks after grpc has already produced
				// this status from the same expired deadline.)
				return fmt.Errorf("%w: %s", ErrCreateTimedOut, uid)
			}
			return fmt.Errorf("reconciler: GetMicroVM: %w", err)
		}
		switch resp.GetMicrovm().GetStatus().GetState() {
		case flintlocktypes.MicroVMStatus_CREATED:
			return nil
		case flintlocktypes.MicroVMStatus_FAILED:
			return fmt.Errorf("%w: %s", ErrCreateFailed, uid)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s", ErrCreateTimedOut, uid)
		case <-ticker.C:
		}
	}
}

// ApplyHookFailurePolicy applies pool.HookFailurePolicy to vm after a hook
// failure (a create-hook failure during provisioning, or a pre-lease-hook
// failure during ClaimVM), and emits VM_HOOK_FAILED. Store/flintlock errors
// here are best-effort: the original failure cause is what the caller
// should return/log. hook ("create" or "pre_lease") and m label/record
// poolmgr_hook_failures_total; every failure path in Provision and
// runPreLeaseHooks funnels through here, so this is the single place that
// metric is recorded rather than duplicating it at each call site.
//
// Cleanup runs on a context detached from ctx's cancellation, bounded by
// hookFailureCleanupTimeout, rather than on ctx itself: ctx is very often
// already cancelled or about to be by the time this runs - e.g. the owning
// Reconciler was stopped mid-Provision because its pool was updated or
// deleted, or poolmgrd is shutting down. Without detaching, every store/
// flint call below would fail immediately on the cancelled ctx, leaving vm
// stranded in a non-terminal phase (PROVISIONING/CREATE_HOOK_RUNNING)
// forever instead of being quarantined or deleted. Mirrors
// api.LeaseServer.applyHookFailurePolicy's identical reasoning for the
// pre-lease-hook path.
func ApplyHookFailurePolicy(ctx context.Context, st store.Store, flint *flintlockclient.Pool, pool *poolmgrv1alpha1.PoolSpec, vm *poolmgrv1alpha1.VMRecord, hook string, m *metrics.Registry) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), hookFailureCleanupTimeout)
	defer cancel()

	switch pool.GetHookFailurePolicy() {
	case poolmgrv1alpha1.HookFailurePolicy_QUARANTINE:
		vm.Phase = poolmgrv1alpha1.VMPhase_QUARANTINED
		vm.LeaseId = nil // no lease exists for a hook failure; don't leave a dangling reference
		vm.UpdatedAt = timestamppb.Now()
		_ = st.UpdateVM(cleanupCtx, vm)
	default: // DELETE_AND_REPLACE, and the unspecified zero value: fail safe by deleting.
		if client, err := flint.Client(vm.GetFlintlockHost()); err == nil {
			_, _ = client.DeleteMicroVM(cleanupCtx, &microvmv1alpha1.DeleteMicroVMRequest{Uid: vm.GetUid()})
		}
		_ = st.DeleteVM(cleanupCtx, vm.GetUid())
	}
	if m != nil {
		m.RecordHookFailure(hook, pool.GetName(), pool.GetNamespace())
	}
	EmitEvent(cleanupCtx, st, pool, vm.GetUid(), poolmgrv1alpha1.EventType_VM_HOOK_FAILED)
}

// EmitEvent appends an event for pool/uid to the store's outbox. It never
// returns an error so it can be called from failure paths without
// complicating control flow; failures are dropped (best-effort).
func EmitEvent(ctx context.Context, st store.Store, pool *poolmgrv1alpha1.PoolSpec, uid string, t poolmgrv1alpha1.EventType) {
	_ = st.AppendEvent(ctx, &poolmgrv1alpha1.Event{
		PoolName:      pool.GetName(),
		PoolNamespace: pool.GetNamespace(),
		VmUid:         uid,
		Type:          t,
		CreatedAt:     timestamppb.Now(),
	})
}

// DeletionNotifier lets EnsureVMDeleted/FinishVMDeletion callers nudge
// whichever component owns a pool's Reconciler after a VM is fully deleted,
// so REPLACE_ON_DELETE pools can replenish.
type DeletionNotifier interface {
	NotifyVMDeleted(poolName, poolNamespace string)
}

// EnsureVMDeleted marks vm DELETING (durably, if it isn't already) and
// attempts to delete it via flintlock. Success, or flintlock reporting
// NotFound (already gone), removes vm's store row and returns nil. Any
// other flintlock/host error leaves vm in the DELETING phase for a later
// retry (e.g. Sweeper's pending-deletion scan) and returns that error.
func EnsureVMDeleted(ctx context.Context, st store.Store, flint *flintlockclient.Pool, vm *poolmgrv1alpha1.VMRecord) error {
	if vm.GetPhase() != poolmgrv1alpha1.VMPhase_DELETING {
		vm.Phase = poolmgrv1alpha1.VMPhase_DELETING
		vm.UpdatedAt = timestamppb.Now()
		if err := st.UpdateVM(ctx, vm); err != nil {
			return fmt.Errorf("reconciler: mark vm deleting: %w", err)
		}
	}

	client, err := flint.Client(vm.GetFlintlockHost())
	if err != nil {
		return fmt.Errorf("reconciler: ensure vm deleted: %w", err)
	}
	if _, err := client.DeleteMicroVM(ctx, &microvmv1alpha1.DeleteMicroVMRequest{Uid: vm.GetUid()}); err != nil && status.Code(err) != codes.NotFound {
		return fmt.Errorf("reconciler: DeleteMicroVM: %w", err)
	}

	if err := st.DeleteVM(ctx, vm.GetUid()); err != nil {
		return fmt.Errorf("reconciler: delete vm record: %w", err)
	}
	return nil
}

// FinishVMDeletion completes the bookkeeping after EnsureVMDeleted has
// succeeded for vm: it deletes any lease row still referencing vm (a
// release whose flintlock call initially failed keeps its lease row in
// place until deletion is confirmed), emits the appropriate VM_DELETED_*
// event, and notifies notifier (nil-safe). The event type is inferred from
// durable state rather than tracked separately: an expiry-triggered
// deletion has already deleted its lease row up front (via
// store.DeleteLeaseIfExpired) by the time this runs, while a
// release-triggered one keeps its lease row until here - so a lease row
// still being present for vm.GetLeaseId() means this was a release.
//
// poolmgr_vm_releases_total/poolmgr_lease_duration_seconds (m, nil-safe)
// are only recorded here for that release case: an expiry-triggered
// deletion's lease row (and its ClaimedAt) is already gone by this point,
// so Sweeper.beginExpiry records those metrics itself, right when
// DeleteLeaseIfExpired durably ends the lease - independent of how long
// this function's caller took to actually finish deleting the VM.
func FinishVMDeletion(ctx context.Context, st store.Store, pool *poolmgrv1alpha1.PoolSpec, vm *poolmgrv1alpha1.VMRecord, notifier DeletionNotifier, m *metrics.Registry) {
	eventType := poolmgrv1alpha1.EventType_VM_DELETED_DUE_TO_EXPIRY
	if leaseID := vm.GetLeaseId(); leaseID != "" {
		if lease, err := st.GetLease(ctx, leaseID); err == nil {
			eventType = poolmgrv1alpha1.EventType_VM_DELETED_ON_RELEASE
			if m != nil {
				m.RecordVMRelease(pool.GetName(), pool.GetNamespace(), "api")
				m.ObserveLeaseDuration(pool.GetName(), pool.GetNamespace(), time.Since(lease.GetClaimedAt().AsTime()))
			}
			_ = st.DeleteLease(ctx, leaseID)
		}
	}
	EmitEvent(ctx, st, pool, vm.GetUid(), eventType)
	if notifier != nil {
		notifier.NotifyVMDeleted(pool.GetName(), pool.GetNamespace())
	}
}
