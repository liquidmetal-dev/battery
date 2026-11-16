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
// without a special-purpose fake for each one. cordonHostBeforePlace, if
// set, cordons that host immediately before delegating PlaceVM: the
// latest a CordonHost can land and still stop the placement.
// removeHostBeforePlace does the same with DeleteHost, for RemoveHost.
type failingStore struct {
	store.Store

	failCreateVM          bool
	failCreateVMPhase     *poolmgrv1alpha1.VMPhase // if set, CreateVM fails only for records in this phase
	onCreateVM            func()                   // if set, called at the start of CreateVM
	failUpdateVMPhase     *poolmgrv1alpha1.VMPhase
	cordonHostBeforePlace string
	removeHostBeforePlace string
}

func (f *failingStore) PlaceVM(ctx context.Context, id, poolName, poolNamespace string, hosts []string) (string, error) {
	if f.cordonHostBeforePlace != "" {
		if _, err := f.SetHostCordoned(ctx, f.cordonHostBeforePlace, true, "cordoned mid-provision"); err != nil {
			return "", err
		}
	}
	if f.removeHostBeforePlace != "" {
		if err := f.DeleteHost(ctx, f.removeHostBeforePlace); err != nil {
			return "", err
		}
	}
	return f.Store.PlaceVM(ctx, id, poolName, poolNamespace, hosts)
}

func (f *failingStore) CreateVM(ctx context.Context, v *poolmgrv1alpha1.VMRecord) error {
	if f.onCreateVM != nil {
		f.onCreateVM()
	}
	if f.failCreateVM {
		return errInjected
	}
	if f.failCreateVMPhase != nil && v.GetPhase() == *f.failCreateVMPhase {
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
