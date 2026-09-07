// Package api implements the pool manager's gRPC service handlers.
package api

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	poolmgrv1alpha1 "github.com/liquidmetal-dev/battery/api/proto/poolmgr/v1alpha1"

	"github.com/liquidmetal-dev/battery/internal/store"
)

// DefaultEventsPollInterval is how often Subscribe polls the outbox for new
// events, matching the DefaultTickInterval/DefaultSweepInterval convention
// in internal/reconciler.
const DefaultEventsPollInterval = time.Second

// DefaultEventsBatchSize bounds how many outbox rows Subscribe fetches per
// store query, so replaying a long-lived outbox (or serving many concurrent
// subscribers) can't hold the store's single database connection or
// allocate the full event history at once.
const DefaultEventsBatchSize = 100

// EventsServer implements poolmgrv1alpha1.EventsServer: Subscribe.
type EventsServer struct {
	poolmgrv1alpha1.UnimplementedEventsServer

	store        store.Store
	pollInterval time.Duration
	batchSize    int
}

// NewEventsServer returns an EventsServer backed by st. pollInterval <= 0
// uses DefaultEventsPollInterval; batchSize <= 0 uses DefaultEventsBatchSize.
func NewEventsServer(st store.Store, pollInterval time.Duration, batchSize int) *EventsServer {
	if pollInterval <= 0 {
		pollInterval = DefaultEventsPollInterval
	}
	if batchSize <= 0 {
		batchSize = DefaultEventsBatchSize
	}
	return &EventsServer{store: st, pollInterval: pollInterval, batchSize: batchSize}
}

// Subscribe streams events from the outbox to stream: on connect it replays
// every matching event currently in the outbox (there's no ack/pruning yet,
// so "recent" means everything present), then polls for and streams new
// ones as they're appended, until stream's context is done. Events are
// fetched s.batchSize at a time - and a full batch is immediately followed
// by another fetch rather than a wait on the poll ticker - so a long outbox
// backlog is never allocated or read in one shot, and never holds the
// store's single database connection for longer than one batch.
func (s *EventsServer) Subscribe(req *poolmgrv1alpha1.SubscribeRequest, stream grpc.ServerStreamingServer[poolmgrv1alpha1.Event]) error {
	ref := req.GetPool()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	var sinceID int64
	for {
		if stream.Context().Err() != nil {
			return nil
		}

		events, err := s.listEventsSince(stream.Context(), ref, sinceID, s.batchSize)
		if err != nil {
			return status.Errorf(codes.Internal, "list events: %v", err)
		}
		for _, e := range events {
			if err := stream.Send(e); err != nil {
				return err
			}
			sinceID = e.GetId()
		}
		if len(events) == s.batchSize {
			continue // more may be pending; drain before waiting on the ticker
		}

		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

// listEventsSince dispatches to the store's filtered or all-pools query
// depending on whether ref is set.
func (s *EventsServer) listEventsSince(ctx context.Context, ref *poolmgrv1alpha1.PoolRef, sinceID int64, limit int) ([]*poolmgrv1alpha1.Event, error) {
	if ref == nil {
		return s.store.ListAllEventsSince(ctx, sinceID, limit)
	}
	return s.store.ListEventsSince(ctx, ref.GetName(), ref.GetNamespace(), sinceID, limit)
}
