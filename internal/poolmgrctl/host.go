package poolmgrctl

import (
	"github.com/spf13/cobra"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// newHostCmd returns the "host" parent command, with drain/undrain/list
// wired up as subcommands.
func newHostCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "host",
		Short: "Manage flintlock host drain state",
	}

	cmd.AddCommand(newHostDrainCmd())
	cmd.AddCommand(newHostUndrainCmd())
	cmd.AddCommand(newHostListCmd())

	return cmd
}

func newHostDrainCmd() *cobra.Command {
	var reason, output string

	cmd := &cobra.Command{
		Use:   "drain <name>",
		Short: "Stop new VM placement on a host, letting existing leases finish naturally",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			host, err := hostAdmin.DrainHost(cmd.Context(), &poolmgrv1alpha1.DrainHostRequest{
				Name:   args[0],
				Reason: reason,
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printHost(cmd.OutOrStdout(), host, format)
		},
	}

	cmd.Flags().StringVar(&reason, "reason", "", "optional free-text reason recorded alongside the drain")
	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}

func newHostUndrainCmd() *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "undrain <name>",
		Short: "Resume new VM placement on a host",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			host, err := hostAdmin.UndrainHost(cmd.Context(), &poolmgrv1alpha1.UndrainHostRequest{Name: args[0]})
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
		Short: "List registered hosts, their drain state, and active VM counts",
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
