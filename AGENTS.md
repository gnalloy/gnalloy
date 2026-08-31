# Gnalloy Agent Notes

## Module

- Name: `gnalloy`
- Module path: `gnalloy.org/gnalloy`
- Description: Go-native, Netty-inspired networking core for buffers, channels, event loops, pollers, bootstrap, and generic codec primitives.

## Engineering Rules

- Keep this repository focused on its module boundary; do not move protocol, transport, handler, resolver, benchmark, or example responsibilities across repositories without an ADR.
- New Go code must use UTF-8, LF line endings, `gofmt`, explicit errors, and table-driven tests for behavior changes.
- Code comments added by agents should be concise Simplified Chinese and explain intent or non-obvious ownership/concurrency rules.
- Run `GOWORK=off GOTOOLCHAIN=local go test ./... -count=1` and `go vet ./...` before committing standalone module changes.
- Use local `G:\opensource\gnalloy\go.work` only for cross-repository development; do not commit relative `replace` directives for normal library dependencies.
