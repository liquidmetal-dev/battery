package poolmgrctl

import (
	"fmt"

	"github.com/spf13/cobra"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// newLeaseCmd returns the "lease" parent command, with claim/release/list
// wired up as subcommands.
func newLeaseCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "lease",
		Short: "Manage VM leases",
	}

	cmd.AddCommand(newLeaseClaimCmd())
	cmd.AddCommand(newLeaseReleaseCmd())
	cmd.AddCommand(newLeaseListCmd())

	return cmd
}

func newLeaseClaimCmd() *cobra.Command {
	var pool, namespace, output string

	cmd := &cobra.Command{
		Use:   "claim",
		Short: "Claim an available VM from a pool",
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			lease := clientsFromContext(cmd.Context()).lease

			resp, err := lease.ClaimVM(cmd.Context(), &poolmgrv1alpha1.ClaimVMRequest{
				Pool: &poolmgrv1alpha1.PoolRef{Name: pool, Namespace: namespace},
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printClaim(cmd.OutOrStdout(), resp, format)
		},
	}

	cmd.Flags().StringVar(&pool, "pool", "", "pool name")
	cmd.Flags().StringVar(&namespace, "namespace", "", "pool namespace")
	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")
	_ = cmd.MarkFlagRequired("pool")
	_ = cmd.MarkFlagRequired("namespace")

	return cmd
}

func newLeaseReleaseCmd() *cobra.Command {
	var leaseID string

	cmd := &cobra.Command{
		Use:   "release",
		Short: "Release a leased VM",
		RunE: func(cmd *cobra.Command, _ []string) error {
			lease := clientsFromContext(cmd.Context()).lease

			_, err := lease.ReleaseVM(cmd.Context(), &poolmgrv1alpha1.ReleaseVMRequest{LeaseId: leaseID})
			if err != nil {
				return wrapGRPCErr(err)
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "lease %s released\n", leaseID)
			return err
		},
	}

	cmd.Flags().StringVar(&leaseID, "lease-id", "", "lease id")
	_ = cmd.MarkFlagRequired("lease-id")

	return cmd
}

func newLeaseListCmd() *cobra.Command {
	var pool, namespace, output string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List leases",
		// Args runs before the root command's PersistentPreRunE (which
		// dials the gRPC connection), so a --pool/--namespace mismatch is
		// reported without ever attempting to connect.
		Args: func(cmd *cobra.Command, _ []string) error {
			if cmd.Flags().Changed("pool") != cmd.Flags().Changed("namespace") {
				return fmt.Errorf("--pool and --namespace must be given together, or not at all")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			lease := clientsFromContext(cmd.Context()).lease

			req := &poolmgrv1alpha1.ListLeasesRequest{}
			if cmd.Flags().Changed("pool") {
				req.PoolRef = &poolmgrv1alpha1.PoolRef{Name: pool, Namespace: namespace}
			}

			resp, err := lease.ListLeases(cmd.Context(), req)
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printLeases(cmd.OutOrStdout(), resp.GetLeases(), format)
		},
	}

	cmd.Flags().StringVar(&pool, "pool", "", "filter leases by pool name (requires --namespace)")
	cmd.Flags().StringVar(&namespace, "namespace", "", "filter leases by pool namespace (requires --pool)")
	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}
