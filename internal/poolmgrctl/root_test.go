package poolmgrctl

import (
	"bytes"
	"errors"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// TestExitCode_Nil proves ExitCode(nil) == 0, the success case.
func TestExitCode_Nil(t *testing.T) {
	if got := ExitCode(nil); got != 0 {
		t.Errorf("ExitCode(nil) = %d, want 0", got)
	}
}

// TestExitCode_AnyError proves ExitCode maps every non-nil error to exit 1
// - there's no real exit-2 distinction for cobra usage errors (cobra's
// ExecuteC intercepts pflag.ErrHelp internally before it ever reaches
// ExitCode, and a plain usage error like an unknown flag comes back as an
// ordinary error), so the honest, simple behavior is "always exit 1".
func TestExitCode_AnyError(t *testing.T) {
	for _, err := range []error{
		errors.New("boom"),
		errors.New(`required flag(s) "spec-file" not set`),
	} {
		if got := ExitCode(err); got != 1 {
			t.Errorf("ExitCode(%v) = %d, want 1", err, got)
		}
	}
}

// TestRootCmd_UnknownFlag_ReturnsPlainError proves that an actual cobra
// usage error (an unrecognized flag) surfaces as a plain error from
// Execute() - not as pflag.ErrHelp - confirming ExitCode's exit-1-for-
// everything behavior is what a real usage error actually gets.
func TestRootCmd_UnknownFlag_ReturnsPlainError(t *testing.T) {
	root := NewRootCmd()
	root.SetOut(&bytes.Buffer{})
	root.SetErr(&bytes.Buffer{})
	root.SetArgs([]string{"--bogus-flag", "pool", "list"})

	err := root.Execute()
	if err == nil {
		t.Fatal("expected an error for an unknown flag, got nil")
	}
	if errors.Is(err, pflag.ErrHelp) {
		t.Errorf("error = %v, want a plain usage error, not pflag.ErrHelp", err)
	}
	if got := ExitCode(err); got != 1 {
		t.Errorf("ExitCode(%v) = %d, want 1", err, got)
	}
}

// TestRootCmd_NoSubcommandShadowsConnectionFlags: a subcommand's local flag
// with the name of a root persistent flag (--insecure, --ca-file, ...)
// silently takes its value, leaving poolmgrctl's own connection to poolmgrd
// unconfigured. host add/update's TLS flags are prefixed for this reason.
func TestRootCmd_NoSubcommandShadowsConnectionFlags(t *testing.T) {
	root := NewRootCmd()

	var walk func(cmd *cobra.Command)
	walk = func(cmd *cobra.Command) {
		cmd.LocalNonPersistentFlags().VisitAll(func(f *pflag.Flag) {
			if root.PersistentFlags().Lookup(f.Name) != nil {
				t.Errorf("%q defines --%s, which shadows the root persistent flag", cmd.CommandPath(), f.Name)
			}
		})
		for _, sub := range cmd.Commands() {
			walk(sub)
		}
	}
	for _, sub := range root.Commands() {
		walk(sub)
	}
}
