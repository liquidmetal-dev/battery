package reconciler

import (
	"testing"
	"time"
)

// SetCleanupTimeoutsForTest shortens hookFailureCleanupTimeout and
// flintlockDeleteTimeout to d for the rest of t, so a test can make a
// flintlock delete hang past them without waiting the production 30 s.
func SetCleanupTimeoutsForTest(t testing.TB, d time.Duration) {
	t.Helper()
	oldCleanup, oldDelete := hookFailureCleanupTimeout, flintlockDeleteTimeout
	hookFailureCleanupTimeout, flintlockDeleteTimeout = d, d
	t.Cleanup(func() {
		hookFailureCleanupTimeout, flintlockDeleteTimeout = oldCleanup, oldDelete
	})
}
