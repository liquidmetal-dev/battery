package metrics_test

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liquidmetal-dev/battery/internal/metrics"
)

// scrape renders reg's metrics through its HTTP handler and returns the
// exposition-format body, exercising the same code path a real Prometheus
// scrape would.
func scrape(t *testing.T, reg *metrics.Registry) string {
	t.Helper()

	req := httptest.NewRequest("GET", "/metrics", nil)
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, req)

	body, err := io.ReadAll(w.Result().Body)
	if err != nil {
		t.Fatalf("read scrape body: %v", err)
	}
	return string(body)
}

func assertContains(t *testing.T, body, want string) {
	t.Helper()
	if !strings.Contains(body, want) {
		t.Fatalf("scrape output missing %q\ngot:\n%s", want, body)
	}
}

func assertNotContains(t *testing.T, body, unwanted string) {
	t.Helper()
	if strings.Contains(body, unwanted) {
		t.Fatalf("scrape output unexpectedly contains %q\ngot:\n%s", unwanted, body)
	}
}
