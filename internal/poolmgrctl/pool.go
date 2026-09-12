package poolmgrctl

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/encoding/protojson"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// newPoolCmd returns the "pool" parent command, with create/update/delete/
// get/list wired up as subcommands.
func newPoolCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pool",
		Short: "Manage pools",
	}

	cmd.AddCommand(newPoolCreateCmd())
	cmd.AddCommand(newPoolUpdateCmd())
	cmd.AddCommand(newPoolDeleteCmd())
	cmd.AddCommand(newPoolGetCmd())
	cmd.AddCommand(newPoolListCmd())

	return cmd
}

// loadPoolSpec reads path and unmarshals it as protojson into a PoolSpec.
// It returns a clear error if the file doesn't exist or isn't valid
// protojson - callers use this to validate --spec-file before any gRPC
// dial is attempted.
func loadPoolSpec(path string) (*poolmgrv1alpha1.PoolSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec file %s: %w", path, err)
	}

	spec := &poolmgrv1alpha1.PoolSpec{}
	if err := protojson.Unmarshal(data, spec); err != nil {
		return nil, fmt.Errorf("parse spec file %s: %w", path, err)
	}

	return spec, nil
}

func newPoolCreateCmd() *cobra.Command {
	var specFile string
	var spec *poolmgrv1alpha1.PoolSpec

	cmd := &cobra.Command{
		Use:   "create",
		Short: "Create a pool from a spec file",
		// Args runs before the root command's PersistentPreRunE (which
		// dials the gRPC connection), so a bad --spec-file is reported
		// without ever attempting to connect.
		Args: func(_ *cobra.Command, _ []string) error {
			loaded, err := loadPoolSpec(specFile)
			if err != nil {
				return err
			}
			spec = loaded
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			poolAdmin := clientsFromContext(cmd.Context()).poolAdmin

			pool, err := poolAdmin.CreatePool(cmd.Context(), &poolmgrv1alpha1.CreatePoolRequest{Spec: spec})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printPool(cmd.OutOrStdout(), pool, OutputTable)
		},
	}

	cmd.Flags().StringVar(&specFile, "spec-file", "", "path to a JSON file containing the pool spec")
	_ = cmd.MarkFlagRequired("spec-file")

	return cmd
}

func newPoolUpdateCmd() *cobra.Command {
	var specFile string
	var spec *poolmgrv1alpha1.PoolSpec

	cmd := &cobra.Command{
		Use:   "update",
		Short: "Update a pool from a spec file",
		Args: func(_ *cobra.Command, _ []string) error {
			loaded, err := loadPoolSpec(specFile)
			if err != nil {
				return err
			}
			spec = loaded
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			poolAdmin := clientsFromContext(cmd.Context()).poolAdmin

			pool, err := poolAdmin.UpdatePool(cmd.Context(), &poolmgrv1alpha1.UpdatePoolRequest{Spec: spec})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printPool(cmd.OutOrStdout(), pool, OutputTable)
		},
	}

	cmd.Flags().StringVar(&specFile, "spec-file", "", "path to a JSON file containing the pool spec")
	_ = cmd.MarkFlagRequired("spec-file")

	return cmd
}

func newPoolDeleteCmd() *cobra.Command {
	var name, namespace string

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a pool",
		RunE: func(cmd *cobra.Command, _ []string) error {
			poolAdmin := clientsFromContext(cmd.Context()).poolAdmin

			_, err := poolAdmin.DeletePool(cmd.Context(), &poolmgrv1alpha1.DeletePoolRequest{
				Ref: &poolmgrv1alpha1.PoolRef{Name: name, Namespace: namespace},
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			_, err = fmt.Fprintf(cmd.OutOrStdout(), "pool %s/%s deleted\n", namespace, name)
			return err
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "pool name")
	cmd.Flags().StringVar(&namespace, "namespace", "", "pool namespace")
	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("namespace")

	return cmd
}

func newPoolGetCmd() *cobra.Command {
	var name, namespace, output string

	cmd := &cobra.Command{
		Use:   "get",
		Short: "Get a pool",
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			poolAdmin := clientsFromContext(cmd.Context()).poolAdmin

			pool, err := poolAdmin.GetPool(cmd.Context(), &poolmgrv1alpha1.GetPoolRequest{
				Ref: &poolmgrv1alpha1.PoolRef{Name: name, Namespace: namespace},
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printPool(cmd.OutOrStdout(), pool, format)
		},
	}

	cmd.Flags().StringVar(&name, "name", "", "pool name")
	cmd.Flags().StringVar(&namespace, "namespace", "", "pool namespace")
	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")
	_ = cmd.MarkFlagRequired("name")
	_ = cmd.MarkFlagRequired("namespace")

	return cmd
}

func newPoolListCmd() *cobra.Command {
	var namespace, output string

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List pools",
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := parseOutputFormat(output)
			if err != nil {
				return err
			}

			poolAdmin := clientsFromContext(cmd.Context()).poolAdmin

			req := &poolmgrv1alpha1.ListPoolsRequest{}
			if cmd.Flags().Changed("namespace") {
				req.Namespace = &namespace
			}

			resp, err := poolAdmin.ListPools(cmd.Context(), req)
			if err != nil {
				return wrapGRPCErr(err)
			}

			return printPools(cmd.OutOrStdout(), resp.GetPools(), format)
		},
	}

	cmd.Flags().StringVar(&namespace, "namespace", "", "filter pools by namespace")
	cmd.Flags().StringVarP(&output, "output", "o", string(OutputTable), "output format: table|json")

	return cmd
}
