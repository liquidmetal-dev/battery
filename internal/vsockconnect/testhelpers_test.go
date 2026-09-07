package vsockconnect_test

import (
	"os"
	"path/filepath"
	"testing"
)

// writeFakeVsockConnect writes a fake vsock-connect executable that records the arguments it was
// invoked with (to the file at $ARGS_FILE) and then behaves according to environment variables:
//   - ping: exits with code $FAKE_PING_EXIT (default 0)
//   - exec: writes $FAKE_STDOUT to stdout and $FAKE_STDERR to stderr, then exits with
//     code $FAKE_EXEC_EXIT (default 0)
func writeFakeVsockConnect(t *testing.T, dir string) string {
	t.Helper()

	script := `#!/bin/sh
echo "$@" > "$ARGS_FILE"
case "$1" in
  ping)
    exit "${FAKE_PING_EXIT:-0}"
    ;;
  exec)
    printf '%s' "$FAKE_STDOUT"
    printf '%s' "$FAKE_STDERR" >&2
    exit "${FAKE_EXEC_EXIT:-0}"
    ;;
  *)
    echo "unknown subcommand: $1" >&2
    exit 2
    ;;
esac
`

	path := filepath.Join(dir, "vsock-connect")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("writing fake vsock-connect: %v", err)
	}
	return path
}
