# Agent Instructions

## Commit messages

- All commits must follow [Conventional Commits](https://www.conventionalcommits.org/) format (e.g. `feat: add x`, `fix: correct y`, `chore: update z`).
- Do not add agent footers (e.g. "Co-Authored-By", "Generated with", session links) to commit messages or pull request descriptions.

## Pull requests

- Do not add agent footers to PR descriptions.

## API changes

- If a change modifies `api/proto/poolmgr/v1alpha1/*.proto` (new or changed RPCs or fields on `PoolAdmin`, `Lease`, or `Events`), update `poolmgrctl` (`internal/poolmgrctl/`) in the same change so the CLI doesn't drift from the API it wraps.
