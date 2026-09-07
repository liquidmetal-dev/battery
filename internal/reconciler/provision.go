package reconciler

import (
	"context"
	"errors"
	"fmt"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	microvmv1alpha1 "github.com/liquidmetal-dev/flintlock/api/services/microvm/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/liquidmetal-dev/battery/internal/flintlockclient"
	"github.com/liquidmetal-dev/battery/internal/store"
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
	store store.Store
	flint *flintlockclient.Pool
	cfg   ProvisionConfig
}

// NewProvisioner returns a Provisioner backed by st and flint. Zero-valued
// fields of cfg are replaced with DefaultProvisionConfig's values.
func NewProvisioner(st store.Store, flint *flintlockclient.Pool, cfg ProvisionConfig) *Provisioner {
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
	return &Provisioner{store: st, flint: flint, cfg: cfg}
}

// Provision runs the full pipeline for one new VM in pool, placing it on
// the least-loaded eligible host. It returns nil only once the VM is
// persisted as AVAILABLE; any failure along the way is handled per
// pool.HookFailurePolicy (the VM is deleted or quarantined and a
// VM_HOOK_FAILED event is emitted) and also returned as an error for the
// caller to log.
func (p *Provisioner) Provision(ctx context.Context, pool *poolmgrv1alpha1.PoolSpec) error {
	host, err := PickHost(ctx, p.store, pool)
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
		return fmt.Errorf("reconciler: provision: CreateVM: %w", err)
	}
	p.emit(ctx, pool, uid, poolmgrv1alpha1.EventType_VM_PROVISIONED)

	if err := p.waitCreated(ctx, client, uid); err != nil {
		p.fail(ctx, pool, vm, err)
		return err
	}

	if err := p.updatePhase(ctx, vm, poolmgrv1alpha1.VMPhase_CREATE_HOOK_RUNNING); err != nil {
		return err
	}

	execClient, err := p.flint.ExecClient(host)
	if err != nil {
		err = fmt.Errorf("reconciler: provision: %w", err)
		p.fail(ctx, pool, vm, err)
		return err
	}

	readyCtx, cancel := context.WithTimeout(ctx, p.cfg.GuestAgentTimeout)
	err = flintlockclient.WaitReady(readyCtx, execClient, uid, p.cfg.GuestAgentInterval)
	cancel()
	if err != nil {
		err = fmt.Errorf("%w: guest-agent not ready: %w", ErrHookFailed, err)
		p.fail(ctx, pool, vm, err)
		return err
	}

	for _, cmd := range pool.GetCreateCommands() {
		result, err := flintlockclient.Exec(ctx, execClient, uid, cmd, flintlockclient.ExecOptions{TimeoutSeconds: p.cfg.ExecTimeoutSeconds})
		if err != nil {
			err = fmt.Errorf("%w: %q: %w", ErrHookFailed, cmd, err)
			p.fail(ctx, pool, vm, err)
			return err
		}
		if result.ExitCode != 0 {
			err = fmt.Errorf("%w: %q: exit code %d", ErrHookFailed, cmd, result.ExitCode)
			p.fail(ctx, pool, vm, err)
			return err
		}
	}

	if err := p.updatePhase(ctx, vm, poolmgrv1alpha1.VMPhase_AVAILABLE); err != nil {
		return err
	}
	p.emit(ctx, pool, uid, poolmgrv1alpha1.EventType_VM_AVAILABLE)
	return nil
}

// updatePhase persists vm's new phase in both the store and the in-memory
// record passed by callers, so subsequent steps (and fail's quarantine
// path) see the current phase.
func (p *Provisioner) updatePhase(ctx context.Context, vm *poolmgrv1alpha1.VMRecord, phase poolmgrv1alpha1.VMPhase) error {
	vm.Phase = phase
	vm.UpdatedAt = timestamppb.Now()
	if err := p.store.UpdateVM(ctx, vm); err != nil {
		return fmt.Errorf("reconciler: provision: UpdateVM: %w", err)
	}
	return nil
}

// waitCreated polls GetMicroVM until the microvm's state is CREATED, or
// returns ErrCreateFailed/ErrCreateTimedOut.
func (p *Provisioner) waitCreated(ctx context.Context, client microvmv1alpha1.MicroVMClient, uid string) error {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.CreatePollTimeout)
	defer cancel()

	for {
		resp, err := client.GetMicroVM(ctx, &microvmv1alpha1.GetMicroVMRequest{Uid: uid})
		if err != nil {
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
		case <-time.After(p.cfg.CreatePollInterval):
		}
	}
}

// fail applies pool.HookFailurePolicy to vm after a provisioning failure,
// and emits VM_HOOK_FAILED. Store/flintlock errors here are best-effort:
// the original cause is what the caller returns and logs.
func (p *Provisioner) fail(ctx context.Context, pool *poolmgrv1alpha1.PoolSpec, vm *poolmgrv1alpha1.VMRecord, _ error) {
	switch pool.GetHookFailurePolicy() {
	case poolmgrv1alpha1.HookFailurePolicy_QUARANTINE:
		vm.Phase = poolmgrv1alpha1.VMPhase_QUARANTINED
		vm.UpdatedAt = timestamppb.Now()
		_ = p.store.UpdateVM(ctx, vm)
	default: // DELETE_AND_REPLACE, and the unspecified zero value: fail safe by deleting.
		if client, err := p.flint.Client(vm.GetFlintlockHost()); err == nil {
			_, _ = client.DeleteMicroVM(ctx, &microvmv1alpha1.DeleteMicroVMRequest{Uid: vm.GetUid()})
		}
		_ = p.store.DeleteVM(ctx, vm.GetUid())
	}
	p.emit(ctx, pool, vm.GetUid(), poolmgrv1alpha1.EventType_VM_HOOK_FAILED)
}

func (p *Provisioner) emit(ctx context.Context, pool *poolmgrv1alpha1.PoolSpec, uid string, t poolmgrv1alpha1.EventType) {
	_ = p.store.AppendEvent(ctx, &poolmgrv1alpha1.Event{
		PoolName:      pool.GetName(),
		PoolNamespace: pool.GetNamespace(),
		VmUid:         uid,
		Type:          t,
		CreatedAt:     timestamppb.Now(),
	})
}
