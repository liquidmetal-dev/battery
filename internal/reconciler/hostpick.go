package reconciler

import (
	"errors"
)

// ErrNoEligibleHost is returned by Provision when a pool has no flintlock
// hosts configured, or every host configured for it is currently cordoned
// or not registered. It wraps store.ErrNoEligibleHost from PlaceVM. The
// reconciler treats it as an expected maintenance-time condition rather
// than a provision failure.
var ErrNoEligibleHost = errors.New("reconciler: pool has no eligible flintlock hosts")
