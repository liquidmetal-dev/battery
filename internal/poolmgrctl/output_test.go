package poolmgrctl

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func samplePoolForOutput(name, namespace string) *poolmgrv1alpha1.Pool {
	return &poolmgrv1alpha1.Pool{
		Spec: &poolmgrv1alpha1.PoolSpec{
			Name:      name,
			Namespace: namespace,
			Size:      3,
		},
		Status: &poolmgrv1alpha1.PoolStatus{
			AvailableCount:    1,
			LeasedCount:       2,
			ProvisioningCount: 3,
			QuarantinedCount:  4,
		},
	}
}

func TestPrintPoolsTable(t *testing.T) {
	pools := []*poolmgrv1alpha1.Pool{
		samplePoolForOutput("pool-a", "default"),
		samplePoolForOutput("pool-b", "other"),
	}

	var buf bytes.Buffer
	if err := printPools(&buf, pools, OutputTable); err != nil {
		t.Fatalf("printPools() error = %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"NAME", "NAMESPACE", "SIZE", "AVAILABLE", "LEASED", "PROVISIONING", "QUARANTINED",
		"pool-a", "default", "pool-b", "other",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("printPools() table output missing %q, got:\n%s", want, out)
		}
	}
}

func TestPrintPoolsTable_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := printPools(&buf, nil, OutputTable); err != nil {
		t.Fatalf("printPools() error = %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "NAME") {
		t.Errorf("printPools() empty table missing header, got:\n%s", out)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 1 {
		t.Errorf("printPools() empty table = %d lines, want 1 (header only), got:\n%s", len(lines), out)
	}
}

func TestPrintPoolTable(t *testing.T) {
	pool := samplePoolForOutput("pool-a", "default")

	var buf bytes.Buffer
	if err := printPool(&buf, pool, OutputTable); err != nil {
		t.Fatalf("printPool() error = %v", err)
	}

	out := buf.String()
	for _, want := range []string{"NAME", "pool-a", "default", "3", "1", "2", "4"} {
		if !strings.Contains(out, want) {
			t.Errorf("printPool() table output missing %q, got:\n%s", want, out)
		}
	}
}

func TestPrintPoolJSON_RoundTrip(t *testing.T) {
	pool := samplePoolForOutput("pool-a", "default")

	var buf bytes.Buffer
	if err := printPool(&buf, pool, OutputJSON); err != nil {
		t.Fatalf("printPool() error = %v", err)
	}

	got := &poolmgrv1alpha1.Pool{}
	if err := protojson.Unmarshal(buf.Bytes(), got); err != nil {
		t.Fatalf("protojson.Unmarshal() error = %v, output:\n%s", err, buf.String())
	}

	if !proto.Equal(got, pool) {
		t.Errorf("round-tripped pool = %+v, want %+v", got, pool)
	}
}

func TestPrintPoolsJSON_RoundTrip(t *testing.T) {
	pools := []*poolmgrv1alpha1.Pool{
		samplePoolForOutput("pool-a", "default"),
		samplePoolForOutput("pool-b", "other"),
	}

	var buf bytes.Buffer
	if err := printPools(&buf, pools, OutputJSON); err != nil {
		t.Fatalf("printPools() error = %v", err)
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("json.Unmarshal() error = %v, output:\n%s", err, buf.String())
	}
	if len(raw) != len(pools) {
		t.Fatalf("got %d array elements, want %d", len(raw), len(pools))
	}

	for i, r := range raw {
		got := &poolmgrv1alpha1.Pool{}
		if err := protojson.Unmarshal(r, got); err != nil {
			t.Fatalf("protojson.Unmarshal(element %d) error = %v", i, err)
		}
		if !proto.Equal(got, pools[i]) {
			t.Errorf("element %d = %+v, want %+v", i, got, pools[i])
		}
	}
}

func TestPrintPoolsJSON_Empty(t *testing.T) {
	var buf bytes.Buffer
	if err := printPools(&buf, nil, OutputJSON); err != nil {
		t.Fatalf("printPools() error = %v", err)
	}

	var raw []json.RawMessage
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatalf("json.Unmarshal() error = %v, output:\n%s", err, buf.String())
	}
	if len(raw) != 0 {
		t.Errorf("got %d array elements, want 0", len(raw))
	}
}

func TestParseOutputFormat(t *testing.T) {
	if _, err := parseOutputFormat("table"); err != nil {
		t.Errorf("parseOutputFormat(table) error = %v", err)
	}
	if _, err := parseOutputFormat("json"); err != nil {
		t.Errorf("parseOutputFormat(json) error = %v", err)
	}
	if _, err := parseOutputFormat("yaml"); err == nil {
		t.Error("parseOutputFormat(yaml) expected error, got nil")
	}
}
