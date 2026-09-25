package flintlockclient

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/mod/semver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// MinFlintlockVersion is the oldest flintlockd the pool manager provisions
// on. Before v0.15.2 flintlockd put each VM's guest-agent socket under
// <state-dir>/<namespace>/<name>/<uid>/, so a long namespace or pool name
// pushed it past the 107-byte Unix socket path limit and every VM failed
// with "connect: invalid argument" well after CreateMicroVM succeeded
// (https://github.com/liquidmetal-dev/battery/issues/94). v0.15.2 keys the
// socket path by uid alone.
const MinFlintlockVersion = "v0.15.2"

// ErrUnsupportedVersion is returned by CheckVersion when a host's
// flintlockd is older than MinFlintlockVersion, or its version can't be
// determined.
var ErrUnsupportedVersion = errors.New("flintlockclient: unsupported flintlock version")

// CheckVersion reports whether the named host runs a flintlockd at least as
// new as MinFlintlockVersion; see Conn.CheckVersion. It returns
// ErrUnknownHost if no such host is in the pool.
func (p *Pool) CheckVersion(ctx context.Context, hostName string) error {
	c, err := p.conn(hostName)
	if err != nil {
		return err
	}
	return c.CheckVersion(ctx)
}

// FlintlockVersion returns the flintlock version the named host last
// reported to CheckVersion, and false if the host isn't in the pool or
// hasn't reported one since it was added or last updated. A version too old
// to pass the check is still returned.
func (p *Pool) FlintlockVersion(hostName string) (string, bool) {
	c, err := p.conn(hostName)
	if err != nil {
		return "", false
	}
	return c.FlintlockVersion()
}

// CheckVersion reports whether c's host runs a flintlockd at least as new
// as MinFlintlockVersion, returning an ErrUnsupportedVersion-wrapping error
// if not. Any other RPC failure (e.g. the host being unreachable) is
// returned as-is. A host that passes is remembered and not asked again; one
// that fails is asked again next time, so upgrading it takes effect without
// restarting the pool manager. Either way the reported version is kept for
// FlintlockVersion.
func (c *Conn) CheckVersion(ctx context.Context) error {
	c.versionMu.Lock()
	ok := c.versionOK
	c.versionMu.Unlock()
	if ok {
		return nil
	}

	resp, err := c.microVM.ServerInfo(ctx, &emptypb.Empty{})
	if status.Code(err) == codes.Unimplemented {
		// ServerInfo was added in v0.15.0.
		return fmt.Errorf("%w: host %q is older than v0.15.0, need %s or newer", ErrUnsupportedVersion, c.name, MinFlintlockVersion)
	}
	if err != nil {
		return fmt.Errorf("flintlockclient: host %q: ServerInfo: %w", c.name, err)
	}

	version := resp.GetVersion().GetVersion()
	core, valid := versionCore(version)
	ok = valid && semver.Compare(core, MinFlintlockVersion) >= 0

	c.versionMu.Lock()
	c.version, c.versionOK = version, ok
	c.versionMu.Unlock()

	if !valid {
		return fmt.Errorf("%w: host %q reports version %q, which can't be compared with the minimum %s (is flintlockd built without version information?)",
			ErrUnsupportedVersion, c.name, version, MinFlintlockVersion)
	}
	if !ok {
		return fmt.Errorf("%w: host %q runs flintlock %s, need %s or newer", ErrUnsupportedVersion, c.name, version, MinFlintlockVersion)
	}
	return nil
}

// FlintlockVersion returns the flintlock version c's host last reported to
// CheckVersion, and false if it hasn't reported one.
func (c *Conn) FlintlockVersion() (string, bool) {
	c.versionMu.Lock()
	defer c.versionMu.Unlock()
	return c.version, c.version != ""
}

// versionCore reduces a flintlock version string to its vMAJOR.MINOR.PATCH
// core, so that builds between releases (git describe output such as
// "v0.15.2-3-gabc1234") compare as the release they're built on rather than
// as a semver prerelease of it.
func versionCore(version string) (string, bool) {
	v := version
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	if i := strings.IndexAny(v, "-+"); i >= 0 {
		v = v[:i]
	}
	if !semver.IsValid(v) {
		return "", false
	}
	return v, true
}
