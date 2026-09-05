package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// poolRow is the flat column representation of a pools table row.
type poolRow struct {
	name                       string
	namespace                  string
	size                       int32
	flintlockHosts             string
	microvmTemplate            string
	replenishmentStrategy      string
	createCommands             string
	preLeaseCommands           string
	hookFailurePolicy          int32
	heartbeatIntervalNs        int64
	heartbeatExpiryThresholdNs int64
}

func poolToRow(p *poolmgrv1alpha1.PoolSpec) (poolRow, error) {
	hosts, err := json.Marshal(p.GetFlintlockHosts())
	if err != nil {
		return poolRow{}, fmt.Errorf("marshal flintlock_hosts: %w", err)
	}

	template, err := marshalProtoJSON(p.GetMicrovmTemplate())
	if err != nil {
		return poolRow{}, fmt.Errorf("marshal microvm_template: %w", err)
	}

	strategy, err := marshalProtoJSON(p.GetReplenishmentStrategy())
	if err != nil {
		return poolRow{}, fmt.Errorf("marshal replenishment_strategy: %w", err)
	}

	createCommands, err := json.Marshal(p.GetCreateCommands())
	if err != nil {
		return poolRow{}, fmt.Errorf("marshal create_commands: %w", err)
	}

	preLeaseCommands, err := json.Marshal(p.GetPreLeaseCommands())
	if err != nil {
		return poolRow{}, fmt.Errorf("marshal pre_lease_commands: %w", err)
	}

	return poolRow{
		name:                       p.GetName(),
		namespace:                  p.GetNamespace(),
		size:                       p.GetSize(),
		flintlockHosts:             string(hosts),
		microvmTemplate:            template,
		replenishmentStrategy:      strategy,
		createCommands:             string(createCommands),
		preLeaseCommands:           string(preLeaseCommands),
		hookFailurePolicy:          int32(p.GetHookFailurePolicy()),
		heartbeatIntervalNs:        p.GetHeartbeatInterval().AsDuration().Nanoseconds(),
		heartbeatExpiryThresholdNs: p.GetHeartbeatExpiryThreshold().AsDuration().Nanoseconds(),
	}, nil
}

func rowToPool(row poolRow) (*poolmgrv1alpha1.PoolSpec, error) {
	var hosts []string
	if err := json.Unmarshal([]byte(row.flintlockHosts), &hosts); err != nil {
		return nil, fmt.Errorf("store: unmarshal flintlock_hosts: %w", err)
	}

	template := &flintlocktypes.MicroVMSpec{}
	if err := unmarshalProtoJSON(row.microvmTemplate, template); err != nil {
		return nil, fmt.Errorf("store: unmarshal microvm_template: %w", err)
	}

	strategy := &poolmgrv1alpha1.ReplenishmentStrategy{}
	if err := unmarshalProtoJSON(row.replenishmentStrategy, strategy); err != nil {
		return nil, fmt.Errorf("store: unmarshal replenishment_strategy: %w", err)
	}

	var createCommands, preLeaseCommands []string
	if err := json.Unmarshal([]byte(row.createCommands), &createCommands); err != nil {
		return nil, fmt.Errorf("store: unmarshal create_commands: %w", err)
	}
	if err := json.Unmarshal([]byte(row.preLeaseCommands), &preLeaseCommands); err != nil {
		return nil, fmt.Errorf("store: unmarshal pre_lease_commands: %w", err)
	}

	return &poolmgrv1alpha1.PoolSpec{
		Name:                     row.name,
		Namespace:                row.namespace,
		Size:                     row.size,
		FlintlockHosts:           hosts,
		MicrovmTemplate:          template,
		ReplenishmentStrategy:    strategy,
		CreateCommands:           createCommands,
		PreLeaseCommands:         preLeaseCommands,
		HookFailurePolicy:        poolmgrv1alpha1.HookFailurePolicy(row.hookFailurePolicy),
		HeartbeatInterval:        durationpb.New(nanoseconds(row.heartbeatIntervalNs)),
		HeartbeatExpiryThreshold: durationpb.New(nanoseconds(row.heartbeatExpiryThresholdNs)),
	}, nil
}

// vmRow is the flat column representation of a vms table row.
type vmRow struct {
	uid           string
	poolName      string
	flintlockHost string
	phase         int32
	leaseID       sql.NullString
	createdAt     int64
	updatedAt     int64
}

func vmToRow(v *poolmgrv1alpha1.VMRecord) vmRow {
	row := vmRow{
		uid:           v.GetUid(),
		poolName:      v.GetPoolName(),
		flintlockHost: v.GetFlintlockHost(),
		phase:         int32(v.GetPhase()),
		createdAt:     v.GetCreatedAt().AsTime().UnixNano(),
		updatedAt:     v.GetUpdatedAt().AsTime().UnixNano(),
	}
	if v.LeaseId != nil {
		row.leaseID = sql.NullString{String: *v.LeaseId, Valid: true}
	}
	return row
}

func rowToVM(row vmRow) *poolmgrv1alpha1.VMRecord {
	v := &poolmgrv1alpha1.VMRecord{
		Uid:           row.uid,
		PoolName:      row.poolName,
		FlintlockHost: row.flintlockHost,
		Phase:         poolmgrv1alpha1.VMPhase(row.phase),
		CreatedAt:     timestamppb.New(time.Unix(0, row.createdAt)),
		UpdatedAt:     timestamppb.New(time.Unix(0, row.updatedAt)),
	}
	if row.leaseID.Valid {
		leaseID := row.leaseID.String
		v.LeaseId = &leaseID
	}
	return v
}

// leaseRow is the flat column representation of a leases table row.
type leaseRow struct {
	leaseID         string
	vmUID           string
	poolName        string
	claimedAt       int64
	lastHeartbeatAt int64
	expiresAt       int64
}

func leaseToRow(l *poolmgrv1alpha1.LeaseRecord) leaseRow {
	return leaseRow{
		leaseID:         l.GetLeaseId(),
		vmUID:           l.GetVmUid(),
		poolName:        l.GetPoolName(),
		claimedAt:       l.GetClaimedAt().AsTime().UnixNano(),
		lastHeartbeatAt: l.GetLastHeartbeatAt().AsTime().UnixNano(),
		expiresAt:       l.GetExpiresAt().AsTime().UnixNano(),
	}
}

func rowToLease(row leaseRow) *poolmgrv1alpha1.LeaseRecord {
	return &poolmgrv1alpha1.LeaseRecord{
		LeaseId:         row.leaseID,
		VmUid:           row.vmUID,
		PoolName:        row.poolName,
		ClaimedAt:       timestamppb.New(time.Unix(0, row.claimedAt)),
		LastHeartbeatAt: timestamppb.New(time.Unix(0, row.lastHeartbeatAt)),
		ExpiresAt:       timestamppb.New(time.Unix(0, row.expiresAt)),
	}
}

func marshalProtoJSON(m proto.Message) (string, error) {
	b, err := protojson.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func unmarshalProtoJSON(data string, m proto.Message) error {
	return protojson.Unmarshal([]byte(data), m)
}

func nanoseconds(ns int64) time.Duration {
	return time.Duration(ns)
}
