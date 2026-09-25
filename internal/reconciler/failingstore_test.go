package reconciler_test

import (
	"context"
	"errors"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/store"
)

// errInjected is returned by failingStore's injected failures.
var errInjected = errors.New("injected store failure")

// failingStore wraps a real Store and lets tests inject a failure into
// CreateVM, or into UpdateVM when the incoming record's phase matches
// failUpdateVMPhase, to exercise the provisioning pipeline's failure paths
// without a special-purpose fake for each one. cordonHostBeforeReserve, if
// set, cordons that host immediately before delegating ReservePlacement:
// the narrowest reproduction of CordonHost landing after PickHost chose it.
// removeHostBeforeReserve does the same with DeleteHost, for RemoveHost.
type failingStore struct {
	store.Store

	failCreateVM            bool
	failUpdateVMPhase       *poolmgrv1alpha1.VMPhase
	cordonHostBeforeReserve string
	removeHostBeforeReserve string
}

func (f *failingStore) ReservePlacement(ctx context.Context, id, host, poolName, poolNamespace string) error {
	if f.cordonHostBeforeReserve != "" {
		if _, err := f.SetHostCordoned(ctx, f.cordonHostBeforeReserve, true, "cordoned mid-provision"); err != nil {
			return err
		}
	}
	if f.removeHostBeforeReserve != "" {
		if err := f.DeleteHost(ctx, f.removeHostBeforeReserve); err != nil {
			return err
		}
	}
	return f.Store.ReservePlacement(ctx, id, host, poolName, poolNamespace)
}

func (f *failingStore) CreateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error {
	if f.failCreateVM {
		return errInjected
	}
	return f.Store.CreateVM(ctx, v)
}

func (f *failingStore) UpdateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error {
	if f.failUpdateVMPhase != nil && v.GetPhase() == *f.failUpdateVMPhase {
		return errInjected
	}
	return f.Store.UpdateVM(ctx, v)
}
