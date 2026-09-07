package metrics_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"github.com/liquidmetal-dev/battery/internal/metrics"
)

// TestServerOptions_RecordsGRPCMetrics spins up an in-process gRPC server
// using Registry.ServerOptions, makes one RPC, and asserts the standard
// grpc_prometheus request-count metric incremented for it.
func TestServerOptions_RecordsGRPCMetrics(t *testing.T) {
	reg := metrics.NewRegistry()

	srv := grpc.NewServer(reg.ServerOptions()...)
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(srv, healthSrv)
	reg.RegisterGRPCServer(srv)

	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { srv.Stop() })
	go func() {
		_ = srv.Serve(lis)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := healthpb.NewHealthClient(conn)
	if _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Check: %v", err)
	}

	body := scrape(t, reg)
	assertContains(t, body, `grpc_server_handled_total{grpc_code="OK",grpc_method="Check",grpc_service="grpc.health.v1.Health",grpc_type="unary"} 1`)
}
