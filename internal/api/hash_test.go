package api

import (
	"testing"

	flintlocktypes "github.com/liquidmetal-dev/flintlock/api/types"
)

func TestComputeTemplateHash_Deterministic(t *testing.T) {
	tmpl := &flintlocktypes.MicroVMSpec{
		Vcpu:       2,
		MemoryInMb: 1024,
		Labels:     map[string]string{"a": "1", "b": "2", "c": "3"},
		Metadata:   map[string]string{"z": "9", "y": "8"},
	}

	first := computeTemplateHash(tmpl)
	second := computeTemplateHash(tmpl)
	if first != second {
		t.Fatalf("computeTemplateHash() not deterministic: %q != %q", first, second)
	}

	// Cloning with map fields rebuilt in a different insertion order must
	// still hash identically: this is the whole reason protojson (which
	// sorts map keys) is used instead of proto.Marshal.
	reordered := &flintlocktypes.MicroVMSpec{
		Vcpu:       2,
		MemoryInMb: 1024,
		Labels:     map[string]string{"c": "3", "a": "1", "b": "2"},
		Metadata:   map[string]string{"y": "8", "z": "9"},
	}
	if got := computeTemplateHash(reordered); got != first {
		t.Fatalf("computeTemplateHash() with reordered map entries = %q, want %q", got, first)
	}
}

func TestComputeTemplateHash_ChangesWithTemplate(t *testing.T) {
	tmpl := &flintlocktypes.MicroVMSpec{Vcpu: 2, MemoryInMb: 1024}
	base := computeTemplateHash(tmpl)

	tmpl.Vcpu = 4
	if got := computeTemplateHash(tmpl); got == base {
		t.Fatalf("computeTemplateHash() = %q after changing vcpu, want different from %q", got, base)
	}
}
