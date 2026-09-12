package reconciler

import (
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

func TestResolveBatchSize(t *testing.T) {
	tests := []struct {
		name   string
		policy *poolmgrv1alpha1.RolloutPolicy
		size   int32
		want   int32
	}{
		{
			name:   "nil policy defaults to 1",
			policy: nil,
			size:   10,
			want:   1,
		},
		{
			name:   "policy with unset oneof defaults to 1",
			policy: &poolmgrv1alpha1.RolloutPolicy{},
			size:   10,
			want:   1,
		},
		{
			name:   "explicit count is used directly",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: 3}},
			size:   10,
			want:   3,
		},
		{
			name:   "count 0 is an intentional pause, not a default",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: 0}},
			size:   10,
			want:   0,
		},
		{
			name:   "negative count falls back to the default of 1",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: -3}},
			size:   10,
			want:   1,
		},
		{
			name:   "count is clamped to pool size",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Count{Count: 100}},
			size:   10,
			want:   10,
		},
		{
			name:   "percent over 100 is clamped to pool size",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Percent{Percent: 500}},
			size:   10,
			want:   10,
		},
		{
			name:   "percent rounds up",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Percent{Percent: 25}},
			size:   10,
			want:   3, // ceil(2.5) = 3
		},
		{
			name:   "percent exact division",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Percent{Percent: 50}},
			size:   10,
			want:   5,
		},
		{
			name:   "percent rounds up to minimum 1",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Percent{Percent: 1}},
			size:   10,
			want:   1,
		},
		{
			name:   "percent against zero size still minimum 1",
			policy: &poolmgrv1alpha1.RolloutPolicy{MaxUnavailable: &poolmgrv1alpha1.RolloutPolicy_Percent{Percent: 50}},
			size:   0,
			want:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveBatchSize(tt.policy, tt.size); got != tt.want {
				t.Errorf("resolveBatchSize() = %d, want %d", got, tt.want)
			}
		})
	}
}
