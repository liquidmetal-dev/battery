package poolmgrctl

import (
	"fmt"
	"io"
	"text/tabwriter"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
	"google.golang.org/protobuf/encoding/protojson"
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
