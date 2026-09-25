package poolmgrctl

import (
	"github.com/spf13/cobra"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// newHostCmd returns the "host" parent command, with cordon/uncordon/list
// wired up as subcommands.
func newHostCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "host",
		Short: "Manage flintlock host cordon state",
	}

	cmd.AddCommand(newHostCordonCmd())
	cmd.AddCommand(newHostUncordonCmd())
	cmd.AddCommand(newHostListCmd())

	return cmd
}

func newHostCordonCmd() *cobra.Command {
	var reason, output string

	cmd := &cobra.Command{
		Use:   "cordon <name>",
		Short: "Stop new VM placement on a host, letting existing leases finish naturally",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			host, err := hostAdmin.CordonHost(cmd.Context(), &poolmgrv1alpha1.CordonHostRequest{
				Name:   args[0],
				Reason: reason,
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printHost(cmd.OutOrStdout(), host, format)
		},
	}

	cmd.Flags().StringVar(&reason, "reason", "", "optional free-text reason recorded alongside the cordon")
	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}

func newHostUncordonCmd() *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "uncordon <name>",
		Short: "Resume new VM placement on a host",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			host, err := hostAdmin.UncordonHost(cmd.Context(), &poolmgrv1alpha1.UncordonHostRequest{Name: args[0]})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printHost(cmd.OutOrStdout(), host, format)
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}

func newHostListCmd() *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List registered hosts, their cordon state, and how many VMs are still counted against each",
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			resp, err := hostAdmin.ListHosts(cmd.Context(), &poolmgrv1alpha1.ListHostsRequest{})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printHostStatuses(cmd.OutOrStdout(), resp.GetHosts(), format)
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}
