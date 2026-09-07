// Package metrics defines the pool manager's Prometheus instrumentation:
// metric definitions, small recording helpers used by the reconciler and
// API packages, a pull-based collector for per-pool gauges, and gRPC server
// interceptor options. See the "Metrics (Prometheus, /metrics)" section of
// docs/design/2026-09-05-microvm-warm-pool-manager-design.md.
package metrics

import (
	"net/http"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/liquidmetal-dev/battery/internal/store"
)

// Registry is a Prometheus metric registry: metrics are always registered
// on and recorded against an explicit *Registry (never the global default
// registry), so tests can construct isolated instances instead of leaking
// state across the package's test cases.
type Registry struct {
	registry    *prometheus.Registry
	metrics     *metricSet
	grpcMetrics *grpc_prometheus.ServerMetrics
}

// NewRegistry returns a Registry with all of the pool manager's push-based
// metrics (see metrics.go) registered and ready to record against.
func NewRegistry() *Registry {
	reg := prometheus.NewRegistry()
	grpcMetrics := grpc_prometheus.NewServerMetrics()
	// Off by default in grpc_prometheus; without this,
	// grpc_server_handling_seconds never appears in a scrape.
	grpcMetrics.EnableHandlingTimeHistogram()
	reg.MustRegister(grpcMetrics)

	return &Registry{
		registry:    reg,
		metrics:     newMetricSet(reg),
		grpcMetrics: grpcMetrics,
	}
}

// RegisterPoolCollector registers a PoolCollector backed by st on the
// registry, so scrapes include the per-pool gauges alongside the push-based
// metrics.
func (r *Registry) RegisterPoolCollector(st store.Store) {
	r.registry.MustRegister(NewPoolCollector(st))
}

// Handler returns the HTTP handler that serves this registry's metrics in
// the Prometheus text exposition format.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{})
}
