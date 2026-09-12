package poolmgrctl

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"
)

// newEventsCmd returns the "events" parent command, with tail wired up as a
// subcommand.
func newEventsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "events",
		Short: "Observe pool/VM/lease lifecycle events",
	}

	cmd.AddCommand(newEventsTailCmd())

	return cmd
}

func newEventsTailCmd() *cobra.Command {
	var pool, namespace string

	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Stream events for a pool until interrupted",
		RunE: func(cmd *cobra.Command, _ []string) error {
			events := clientsFromContext(cmd.Context()).events

			stream, err := events.Subscribe(cmd.Context(), &poolmgrv1alpha1.SubscribeRequest{
				Pool: &poolmgrv1alpha1.PoolRef{Name: pool, Namespace: namespace},
			})
			if err != nil {
				return wrapGRPCErr(err)
			}

			return tailEvents(cmd.Context(), cmd.OutOrStdout(), stream)
		},
	}

	cmd.Flags().StringVar(&pool, "pool", "", "pool to tail events for")
	cmd.Flags().StringVar(&namespace, "namespace", "", "namespace of the pool to tail events for")
	_ = cmd.MarkFlagRequired("pool")
	_ = cmd.MarkFlagRequired("namespace")

	return cmd
}

// eventStream is the subset of poolmgrv1alpha1.Events_SubscribeClient that
// tailEvents needs, so it can be exercised in tests without a real gRPC
// stream.
type eventStream interface {
	Recv() (*poolmgrv1alpha1.Event, error)
}

// tailEvents reads events from stream and prints one line per event to w
// until the stream ends (io.EOF), an error occurs, or ctx is cancelled - a
// cancelled context (a normal Ctrl-C/SIGTERM exit) is not treated as an
// error.
func tailEvents(ctx context.Context, w io.Writer, stream eventStream) error {
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if ctx.Err() != nil || status.Code(err) == codes.Canceled {
				return nil
			}
			return wrapGRPCErr(err)
		}

		if _, err := fmt.Fprintf(w, "%s %s %s %s\n",
			formatTimestamp(event.GetCreatedAt()), event.GetType(), event.GetVmUid(), event.GetPayloadJson()); err != nil {
			return err
		}
	}
}
