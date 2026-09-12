package reconciler_test

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"github.com/liquidmetal-dev/battery/internal/reconciler"
)

func testPolicy() *poolmgrv1alpha1.AutoscalingPolicy {
	return &poolmgrv1alpha1.AutoscalingPolicy{
		Enabled:               true,
		MinSize:               2,
		MaxSize:               10,
		ScaleStep:             1,
		ScaleUpClaimsPerSec:   1.0,
		ScaleDownClaimsPerSec: 0.1,
		ClaimRateWindow:       durationpb.New(time.Minute),
		Cooldown:              durationpb.New(2 * time.Minute),
	}
}

func TestAutoscaler_ClaimsPerSec_PrunesOutsideWindow(t *testing.T) {
	a := reconciler.NewAutoscaler()
	now := time.Now()

	a.RecordClaim(now.Add(-2 * time.Minute)) // outside a 1-minute window
	a.RecordClaim(now.Add(-30 * time.Second))
	a.RecordClaim(now.Add(-10 * time.Second))

	got := a.ClaimsPerSec(now, time.Minute)
	want := 2.0 / 60.0
	if diff := got - want; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("ClaimsPerSec() = %v, want %v", got, want)
	}
}

func TestAutoscaler_Evaluate_Disabled_NoChange(t *testing.T) {
	a := reconciler.NewAutoscaler()
	policy := testPolicy()
	policy.Enabled = false

	newSize, scaled := a.Evaluate(policy, 5, time.Now())
	if scaled {
		t.Fatalf("Evaluate() scaled = true, want false for disabled policy")
	}
	if newSize != 5 {
		t.Fatalf("Evaluate() newSize = %d, want unchanged 5", newSize)
	}
}

func TestAutoscaler_Evaluate_ScalesUp_WhenAboveThreshold(t *testing.T) {
	a := reconciler.NewAutoscaler()
	policy := testPolicy()
	now := time.Now()

	// 5 claims in the last minute => 5/60 claims/sec, above the 1.0/60 threshold... wait, need
	// rate directly comparable: threshold is claims/sec, so use enough claims to exceed 1.0/sec.
	for i := 0; i < 90; i++ {
		a.RecordClaim(now.Add(-time.Duration(i) * time.Second / 2))
	}

	newSize, scaled := a.Evaluate(policy, 5, now)
	if !scaled {
		t.Fatalf("Evaluate() scaled = false, want true")
	}
	if want := int32(5 + policy.ScaleStep); newSize != want {
		t.Fatalf("Evaluate() newSize = %d, want %d", newSize, want)
	}
}

func TestAutoscaler_Evaluate_ScaleUp_ClampsToMax(t *testing.T) {
	a := reconciler.NewAutoscaler()
	policy := testPolicy()
	now := time.Now()
	for i := 0; i < 90; i++ {
		a.RecordClaim(now.Add(-time.Duration(i) * time.Second / 2))
	}

	newSize, scaled := a.Evaluate(policy, policy.MaxSize, now)
	if scaled {
		t.Fatalf("Evaluate() scaled = true, want false: already at max_size")
	}
	if newSize != policy.MaxSize {
		t.Fatalf("Evaluate() newSize = %d, want max_size %d", newSize, policy.MaxSize)
	}
}

func TestAutoscaler_Evaluate_ScalesDown_WhenBelowThreshold(t *testing.T) {
	a := reconciler.NewAutoscaler()
	policy := testPolicy()
	now := time.Now()
	// No claims recorded => rate is 0, below scale_down_claims_per_sec.

	newSize, scaled := a.Evaluate(policy, 5, now)
	if !scaled {
		t.Fatalf("Evaluate() scaled = false, want true")
	}
	if want := int32(5 - policy.ScaleStep); newSize != want {
		t.Fatalf("Evaluate() newSize = %d, want %d", newSize, want)
	}
}

func TestAutoscaler_Evaluate_ScaleDown_ClampsToMin(t *testing.T) {
	a := reconciler.NewAutoscaler()
	policy := testPolicy()
	now := time.Now()

	newSize, scaled := a.Evaluate(policy, policy.MinSize, now)
	if scaled {
		t.Fatalf("Evaluate() scaled = true, want false: already at min_size")
	}
	if newSize != policy.MinSize {
		t.Fatalf("Evaluate() newSize = %d, want min_size %d", newSize, policy.MinSize)
	}
}

func TestAutoscaler_Evaluate_DeadZone_NoChange(t *testing.T) {
	a := reconciler.NewAutoscaler()
	policy := testPolicy()
	now := time.Now()

	// 18 claims in the last minute => 0.3 claims/sec, between scale_down (0.1) and scale_up
	// (1.0) thresholds.
	for i := 0; i < 18; i++ {
		a.RecordClaim(now.Add(-time.Duration(i) * 3 * time.Second))
	}

	newSize, scaled := a.Evaluate(policy, 5, now)
	if scaled {
		t.Fatalf("Evaluate() scaled = true, want false in dead zone")
	}
	if newSize != 5 {
		t.Fatalf("Evaluate() newSize = %d, want unchanged 5", newSize)
	}
}

func TestAutoscaler_Evaluate_RespectsCooldown(t *testing.T) {
	a := reconciler.NewAutoscaler()
	policy := testPolicy()
	now := time.Now()

	for i := 0; i < 90; i++ {
		a.RecordClaim(now.Add(-time.Duration(i) * time.Second / 2))
	}

	if _, scaled := a.Evaluate(policy, 5, now); !scaled {
		t.Fatalf("first Evaluate() scaled = false, want true")
	}

	// Immediately re-evaluate: still within cooldown, should not scale again even though the
	// rate still crosses the threshold.
	newSize, scaled := a.Evaluate(policy, 6, now.Add(time.Second))
	if scaled {
		t.Fatalf("second Evaluate() scaled = true, want false within cooldown")
	}
	if newSize != 6 {
		t.Fatalf("second Evaluate() newSize = %d, want unchanged 6", newSize)
	}

	// After the cooldown elapses, scaling resumes, given fresh claims within the window.
	later := now.Add(policy.Cooldown.AsDuration() + time.Second)
	for i := 0; i < 90; i++ {
		a.RecordClaim(later.Add(-time.Duration(i) * time.Second / 2))
	}
	newSize, scaled = a.Evaluate(policy, 6, later)
	if !scaled {
		t.Fatalf("post-cooldown Evaluate() scaled = false, want true")
	}
	if want := int32(6 + policy.ScaleStep); newSize != want {
		t.Fatalf("post-cooldown Evaluate() newSize = %d, want %d", newSize, want)
	}
}
