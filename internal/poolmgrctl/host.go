package poolmgrctl

import (
	"github.com/spf13/cobra"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// newHostCmd returns the "host" parent command, with add/update/remove/get/
// list/cordon/uncordon wired up as subcommands.
func newHostCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "host",
		Short: "Manage flintlock hosts and their cordon state",
	}

	cmd.AddCommand(newHostAddCmd())
	cmd.AddCommand(newHostUpdateCmd())
	cmd.AddCommand(newHostRemoveCmd())
	cmd.AddCommand(newHostGetCmd())
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

// hostSpecFlags holds the flags host add and host update share: the
// connection settings sent in the request's Host, plus --skip-validation.
// The TLS flags carry a flintlock- prefix because the root command's
// persistent --insecure, --ca-file, --cert-file, and --key-file already
// configure poolmgrctl's own connection to poolmgrd, and a local flag of
// the same name would shadow them.
type hostSpecFlags struct {
	address        string
	insecure       bool
	caFile         string
	certFile       string
	keyFile        string
	skipValidation bool
}

func (f *hostSpecFlags) register(cmd *cobra.Command) {
	cmd.Flags().StringVar(&f.address, "address", "", "flintlock gRPC address, host:port (required)")
	cmd.Flags().BoolVar(&f.insecure, "flintlock-insecure", false, "poolmgrd connects to the host without TLS; excludes the --flintlock-*-file flags")
	cmd.Flags().StringVar(&f.caFile, "flintlock-ca-file", "", "CA certificate poolmgrd verifies the host with, a path on the poolmgrd machine")
	cmd.Flags().StringVar(&f.certFile, "flintlock-cert-file", "", "client certificate poolmgrd presents to the host for mTLS, a path on the poolmgrd machine")
	cmd.Flags().StringVar(&f.keyFile, "flintlock-key-file", "", "key for --flintlock-cert-file, a path on the poolmgrd machine")
	cmd.Flags().BoolVar(&f.skipValidation, "skip-validation", false, "don't dial the host or check its flintlock version first")
	_ = cmd.MarkFlagRequired("address")
}

// host returns the Host these flags describe. The server validates the
// combination, so the CLI and the API can't disagree about the rules.
func (f *hostSpecFlags) host(name string) *poolmgrv1alpha1.Host {
	return &poolmgrv1alpha1.Host{
		Name:    name,
		Address: f.address,
		Tls: &poolmgrv1alpha1.HostTLS{
			Insecure: f.insecure,
			CaFile:   f.caFile,
			CertFile: f.certFile,
			KeyFile:  f.keyFile,
		},
	}
}

func newHostAddCmd() *cobra.Command {
	var spec hostSpecFlags
	var output string

	cmd := &cobra.Command{
		Use:   "add <name>",
		Short: "Register a flintlock host, checking it is reachable and new enough unless --skip-validation is set",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			host, err := hostAdmin.AddHost(cmd.Context(), &poolmgrv1alpha1.AddHostRequest{
				Host:           spec.host(args[0]),
				SkipValidation: spec.skipValidation,
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printHost(cmd.OutOrStdout(), host, format)
		},
	}

	spec.register(cmd)
	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}

func newHostUpdateCmd() *cobra.Command {
	var spec hostSpecFlags
	var output string

	cmd := &cobra.Command{
		Use:   "update <name>",
		Short: "Replace a host's address and TLS settings and redial it; cordon state is kept",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			host, err := hostAdmin.UpdateHost(cmd.Context(), &poolmgrv1alpha1.UpdateHostRequest{
				Host:           spec.host(args[0]),
				SkipValidation: spec.skipValidation,
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printHost(cmd.OutOrStdout(), host, format)
		},
	}

	spec.register(cmd)
	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}

func newHostRemoveCmd() *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "remove <name>",
		Short: "Unregister a host that no pool names and that has no VMs left",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			if _, err := hostAdmin.RemoveHost(cmd.Context(), &poolmgrv1alpha1.RemoveHostRequest{Name: args[0]}); err != nil {
				return wrapGRPCErr(err)
			}

			return printHostRemoved(cmd.OutOrStdout(), args[0], format)
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}

func newHostGetCmd() *cobra.Command {
	var output string

	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Show a host with its VM count and last reported flintlock version",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			hostAdmin := clientsFromContext(cmd.Context()).hostAdmin

			hs, err := hostAdmin.GetHost(cmd.Context(), &poolmgrv1alpha1.GetHostRequest{Name: args[0]})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printHostStatus(cmd.OutOrStdout(), hs, format)
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}
