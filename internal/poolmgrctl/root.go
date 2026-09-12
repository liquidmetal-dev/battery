// Package poolmgrctl implements the poolmgrctl CLI client for the pool
// manager's gRPC API.
package poolmgrctl

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// defaultAddr is the default address poolmgrctl dials, matching poolmgrd's
// typical gRPC API listen address (there is no hardcoded default in
// poolmgrd itself - its api_server.addr is a required config field - so
// this is just a sensible default for local/dev use).
const defaultAddr = "127.0.0.1:9090"

// apiClients bundles the typed gRPC clients dialed once per invocation and
// threaded through subcommands via the command's context.
type apiClients struct {
	conn      *grpc.ClientConn
	poolAdmin poolmgrv1alpha1.PoolAdminClient
	lease     poolmgrv1alpha1.LeaseClient
	events    poolmgrv1alpha1.EventsClient
}

type clientsKey struct{}

// clientsFromContext returns the apiClients stored on ctx by
// PersistentPreRunE, or nil if none is present.
func clientsFromContext(ctx context.Context) *apiClients {
	c, _ := ctx.Value(clientsKey{}).(*apiClients)
	return c
}

// NewRootCmd builds the poolmgrctl root command, its persistent connection
// flags, and its command tree.
func NewRootCmd() *cobra.Command {
	cf := connFlags{}

	root := &cobra.Command{
		Use:          "poolmgrctl",
		Short:        "CLI client for the pool manager gRPC API",
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			conn, err := dial(cf)
			if err != nil {
				return fmt.Errorf("dial %s: %w", cf.addr, err)
			}

			clients := &apiClients{
				conn:      conn,
				poolAdmin: poolmgrv1alpha1.NewPoolAdminClient(conn),
				lease:     poolmgrv1alpha1.NewLeaseClient(conn),
				events:    poolmgrv1alpha1.NewEventsClient(conn),
			}

			cmd.SetContext(context.WithValue(cmd.Context(), clientsKey{}, clients))
			return nil
		},
		PersistentPostRunE: func(cmd *cobra.Command, _ []string) error {
			clients := clientsFromContext(cmd.Context())
			if clients == nil || clients.conn == nil {
				return nil
			}
			return clients.conn.Close()
		},
	}

	root.PersistentFlags().StringVar(&cf.addr, "addr", defaultAddr, "address of the pool manager's gRPC API")
	root.PersistentFlags().BoolVar(&cf.insecure, "insecure", false, "disable TLS when dialing the pool manager")
	root.PersistentFlags().StringVar(&cf.caFile, "ca-file", "", "path to a CA certificate to verify the pool manager's server certificate")
	root.PersistentFlags().StringVar(&cf.certFile, "cert-file", "", "path to a client certificate for mTLS")
	root.PersistentFlags().StringVar(&cf.keyFile, "key-file", "", "path to the client certificate's private key for mTLS")

	root.AddCommand(newPoolCmd(), newLeaseCmd(), newEventsCmd())

	return root
}

// wrapGRPCErr turns "rpc error: code = ResourceExhausted desc = ..." into
// "resource_exhausted: ..." for CLI display, leaving non-gRPC errors
// untouched.
func wrapGRPCErr(err error) error {
	if err == nil {
		return nil
	}

	st, ok := status.FromError(err)
	if !ok {
		return err
	}

	return fmt.Errorf("%s: %s", snakeCaseCode(st.Code()), st.Message())
}

// snakeCaseCode converts a gRPC code's PascalCase String() (e.g.
// "ResourceExhausted") into the lower snake_case form used by the gRPC
// status proto and most CLI tooling (e.g. "resource_exhausted").
func snakeCaseCode(c codes.Code) string {
	pascal := c.String()

	var b strings.Builder
	for i, r := range pascal {
		if i > 0 && unicode.IsUpper(r) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

// ExitCode maps an error to a process exit code: 2 for cobra's own
// usage/flag/arg errors, 1 otherwise.
//
// cobra doesn't tag usage errors distinctly from RunE errors by default;
// pflag.ErrHelp is the one case that's cleanly identifiable without extra
// plumbing (a FlagErrorFunc/custom error type), so that's what this checks.
func ExitCode(err error) int {
	if err == nil {
		return 0
	}
	if errors.Is(err, pflag.ErrHelp) {
		return 2
	}
	return 1
}
