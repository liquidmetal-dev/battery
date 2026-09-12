package poolmgrctl

import (
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// OutputFormat selects how pool.go's commands render their results.
type OutputFormat string

const (
	// OutputTable renders results as a human-readable text/tabwriter table.
	OutputTable OutputFormat = "table"
	// OutputJSON renders results as protojson.
	OutputJSON OutputFormat = "json"
)

// parseOutputFormat validates s as an OutputFormat, rejecting anything that
// isn't "table" or "json".
func parseOutputFormat(s string) (OutputFormat, error) {
	switch OutputFormat(s) {
	case OutputTable, OutputJSON:
		return OutputFormat(s), nil
	default:
		return "", fmt.Errorf("invalid output format %q: must be %q or %q", s, OutputTable, OutputJSON)
	}
}

// printPools renders pools to w in the given format.
//
// JSON mode marshals pools as a single JSON array of protojson objects
// (rather than one object per line) since this is "list" output and an
// array is the most natural shape for a caller to consume as a whole; each
// element is built via protojson and stitched together by hand since
// protojson output can't be fed directly into encoding/json's array
// marshaling.
//
// Table mode prints a header-only table when pools is empty, matching
// kubectl's "no resources found" convention loosely without a special-cased
// message.
func printPools(w io.Writer, pools []*poolmgrv1alpha1.Pool, format OutputFormat) error {
	switch format {
	case OutputJSON:
		return printPoolsJSON(w, pools)
	case OutputTable, "":
		return printPoolsTable(w, pools)
	default:
		return fmt.Errorf("invalid output format %q", format)
	}
}

// printPool renders a single pool to w in the given format.
func printPool(w io.Writer, pool *poolmgrv1alpha1.Pool, format OutputFormat) error {
	switch format {
	case OutputJSON:
		return printPoolJSON(w, pool)
	case OutputTable, "":
		return printPoolTable(w, pool)
	default:
		return fmt.Errorf("invalid output format %q", format)
	}
}

func printPoolsJSON(w io.Writer, pools []*poolmgrv1alpha1.Pool) error {
	marshalOpts := protojson.MarshalOptions{Multiline: true}

	buf := []byte("[")
	for i, pool := range pools {
		if i > 0 {
			buf = append(buf, ',')
		}
		b, err := marshalOpts.Marshal(pool)
		if err != nil {
			return fmt.Errorf("marshal pool: %w", err)
		}
		buf = append(buf, b...)
	}
	buf = append(buf, ']')

	if _, err := w.Write(buf); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

func printPoolJSON(w io.Writer, pool *poolmgrv1alpha1.Pool) error {
	marshalOpts := protojson.MarshalOptions{Multiline: true}
	b, err := marshalOpts.Marshal(pool)
	if err != nil {
		return fmt.Errorf("marshal pool: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	_, err = fmt.Fprintln(w)
	return err
}

func printPoolsTable(w io.Writer, pools []*poolmgrv1alpha1.Pool) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tNAMESPACE\tSIZE\tAVAILABLE\tLEASED\tPROVISIONING\tQUARANTINED"); err != nil {
		return err
	}
	for _, pool := range pools {
		spec := pool.GetSpec()
		status := pool.GetStatus()
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%d\t%d\n",
			spec.GetName(), spec.GetNamespace(), spec.GetSize(),
			status.GetAvailableCount(), status.GetLeasedCount(),
			status.GetProvisioningCount(), status.GetQuarantinedCount()); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func printPoolTable(w io.Writer, pool *poolmgrv1alpha1.Pool) error {
	return printPoolsTable(w, []*poolmgrv1alpha1.Pool{pool})
}

// printLeases renders leases to w in the given format, following the same
// conventions as printPools: JSON mode marshals a single JSON array of
// protojson objects; table mode prints a header-only table when leases is
// empty.
func printLeases(w io.Writer, leases []*poolmgrv1alpha1.LeaseRecord, format OutputFormat) error {
	switch format {
	case OutputJSON:
		return printLeasesJSON(w, leases)
	case OutputTable, "":
		return printLeasesTable(w, leases)
	default:
		return fmt.Errorf("invalid output format %q", format)
	}
}

func printLeasesJSON(w io.Writer, leases []*poolmgrv1alpha1.LeaseRecord) error {
	marshalOpts := protojson.MarshalOptions{Multiline: true}

	buf := []byte("[")
	for i, lease := range leases {
		if i > 0 {
			buf = append(buf, ',')
		}
		b, err := marshalOpts.Marshal(lease)
		if err != nil {
			return fmt.Errorf("marshal lease: %w", err)
		}
		buf = append(buf, b...)
	}
	buf = append(buf, ']')

	if _, err := w.Write(buf); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

func printLeasesTable(w io.Writer, leases []*poolmgrv1alpha1.LeaseRecord) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "LEASE_ID\tPOOL\tNAMESPACE\tVM_UID\tCLAIMED_AT\tEXPIRES_AT"); err != nil {
		return err
	}
	for _, lease := range leases {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			lease.GetLeaseId(), lease.GetPoolName(), lease.GetPoolNamespace(), lease.GetVmUid(),
			formatTimestamp(lease.GetClaimedAt()), formatTimestamp(lease.GetExpiresAt())); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// printClaim renders a ClaimVM response to w in the given format, following
// the same table/JSON convention as printPool/printPools/printLeases.
func printClaim(w io.Writer, resp *poolmgrv1alpha1.ClaimVMResponse, format OutputFormat) error {
	switch format {
	case OutputJSON:
		return printClaimJSON(w, resp)
	case OutputTable, "":
		return printClaimTable(w, resp)
	default:
		return fmt.Errorf("invalid output format %q", format)
	}
}

func printClaimJSON(w io.Writer, resp *poolmgrv1alpha1.ClaimVMResponse) error {
	marshalOpts := protojson.MarshalOptions{Multiline: true}
	b, err := marshalOpts.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal claim response: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	_, err = fmt.Fprintln(w)
	return err
}

func printClaimTable(w io.Writer, resp *poolmgrv1alpha1.ClaimVMResponse) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintf(tw, "LEASE_ID\t%s\n", resp.GetLeaseId()); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(tw, "VM_UID\t%s\n", resp.GetVmUid()); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(tw, "HOST\t%s (%s)\n", resp.GetHost().GetName(), resp.GetHost().GetAddress()); err != nil {
		return err
	}
	if err := tw.Flush(); err != nil {
		return err
	}

	ifaces := resp.GetNetworkInterfaces()
	if len(ifaces) == 0 {
		return nil
	}

	names := make([]string, 0, len(ifaces))
	for name := range ifaces {
		names = append(names, name)
	}
	sort.Strings(names)

	if _, err := fmt.Fprintln(w, "NETWORK_INTERFACES:"); err != nil {
		return err
	}
	for _, name := range names {
		iface := ifaces[name]
		if _, err := fmt.Fprintf(w, "  %s: host_device=%s index=%d mac=%s\n",
			name, iface.GetHostDeviceName(), iface.GetIndex(), iface.GetMacAddress()); err != nil {
			return err
		}
	}
	return nil
}

// printHost renders a single Host to w in the given format.
func printHost(w io.Writer, host *poolmgrv1alpha1.Host, format OutputFormat) error {
	switch format {
	case OutputJSON:
		return printHostJSON(w, host)
	case OutputTable, "":
		return printHostStatusesTable(w, []*poolmgrv1alpha1.HostStatus{{Host: host}})
	default:
		return fmt.Errorf("invalid output format %q", format)
	}
}

func printHostJSON(w io.Writer, host *poolmgrv1alpha1.Host) error {
	marshalOpts := protojson.MarshalOptions{Multiline: true}
	b, err := marshalOpts.Marshal(host)
	if err != nil {
		return fmt.Errorf("marshal host: %w", err)
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	_, err = fmt.Fprintln(w)
	return err
}

// printHostStatuses renders hosts to w in the given format, following the
// same table/JSON convention as printPools/printLeases.
func printHostStatuses(w io.Writer, hosts []*poolmgrv1alpha1.HostStatus, format OutputFormat) error {
	switch format {
	case OutputJSON:
		return printHostStatusesJSON(w, hosts)
	case OutputTable, "":
		return printHostStatusesTable(w, hosts)
	default:
		return fmt.Errorf("invalid output format %q", format)
	}
}

func printHostStatusesJSON(w io.Writer, hosts []*poolmgrv1alpha1.HostStatus) error {
	marshalOpts := protojson.MarshalOptions{Multiline: true}

	buf := []byte("[")
	for i, h := range hosts {
		if i > 0 {
			buf = append(buf, ',')
		}
		b, err := marshalOpts.Marshal(h)
		if err != nil {
			return fmt.Errorf("marshal host: %w", err)
		}
		buf = append(buf, b...)
	}
	buf = append(buf, ']')

	if _, err := w.Write(buf); err != nil {
		return err
	}
	_, err := fmt.Fprintln(w)
	return err
}

func printHostStatusesTable(w io.Writer, hosts []*poolmgrv1alpha1.HostStatus) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "NAME\tADDRESS\tDRAINED\tACTIVE_VMS"); err != nil {
		return err
	}
	for _, hs := range hosts {
		host := hs.GetHost()
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%t\t%d\n",
			host.GetName(), host.GetAddress(), host.GetDrained(), hs.GetActiveVmCount()); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// formatTimestamp renders a *timestamppb.Timestamp as RFC3339, or "" if ts
// is nil/unset.
func formatTimestamp(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().Format(time.RFC3339)
}
