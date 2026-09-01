# Gnalloy Agent Notes

## Module

- Name: `gnalloy`
- Module path: `gnalloy.org/gnalloy`
- Description: Go-native, Netty-inspired networking core for buffers, channels, event loops, pollers, bootstrap, and generic codec primitives.

## Project Skills

Use the project-local skills in `.claude/skills/` when the task matches their scope.
This directory is the canonical cross-tool skill copy for Claude Code, OpenCode, Codex, and other agents that can read project instructions.
A mirror exists in `.codex/skills/` for Codex-specific surfaces.

- Go core runtime, generics, buffers, channels, event loops, and framework APIs: `.claude/skills/golang-pro/SKILL.md`
- Context cancellation, lifecycle ordering, event flow, and concurrency review: `.claude/skills/go-concurrency-patterns/SKILL.md`
- Framework boundaries, modular contracts, transport/protocol separation, DI/container design, and ADRs: `.claude/skills/architecture-patterns/SKILL.md` and `.claude/skills/architecture-designer/SKILL.md`
- Public APIs, package contracts, and module surfaces: `.claude/skills/api-and-interface-design/SKILL.md`
- Significant decisions and durable project context: `.claude/skills/documentation-and-adrs/SKILL.md`
- Multi-file features and staged refactors: `.claude/skills/incremental-implementation/SKILL.md`
- New features or unclear requirements: `.claude/skills/spec-driven-development/SKILL.md`
- Framework error codes, wrapping, and failure contracts: `.claude/skills/error-handling-patterns/SKILL.md`
- Configuration contracts and validation behavior: `.claude/skills/config-validate/SKILL.md`
- Bugs, test failures, and unexpected runtime behavior: `.claude/skills/systematic-debugging/SKILL.md`
- Refactoring for clarity without behavior changes: `.claude/skills/code-simplification/SKILL.md`
- Behavior changes and bug fixes: `.claude/skills/test-driven-development/SKILL.md`
- Reviews and quality checks: `.claude/skills/code-review-and-quality/SKILL.md`
- Security reviews and threat modeling when explicitly requested: `.claude/skills/security-best-practices/SKILL.md` and `.claude/skills/security-threat-model/SKILL.md`
- Future web/API modules and protocol-facing APIs: `.claude/skills/openapi-spec-generation/SKILL.md` and `.claude/skills/rest-api-conventions/SKILL.md`
- Observability, tracing, performance, benchmarks, and pressure tests: `.claude/skills/observability-and-instrumentation/SKILL.md`, `.claude/skills/performance-engineer/SKILL.md`, and `.claude/skills/distributed-tracing/SKILL.md`
- GitHub Actions setup or failed PR checks: `.claude/skills/github-actions-templates/SKILL.md` and `.claude/skills/gh-fix-ci/SKILL.md`
- Netty, Java, and Spring semantics are reference material only: `.claude/skills/java-architect/SKILL.md` and `.claude/skills/spring-boot-engineer/SKILL.md`

Keep Gnalloy Go-native: prefer explicit APIs, deterministic registration, small runtime contracts, bounded dependencies, and low-allocation hot paths over Java-style runtime scanning or reflection-heavy behavior.

## Engineering Rules

- Keep this repository focused on its module boundary; do not move protocol, transport, handler, resolver, benchmark, or example responsibilities across repositories without an ADR.
- New Go code must use UTF-8, LF line endings, `gofmt`, explicit errors, and table-driven tests for behavior changes.
- Code comments added by agents should be concise Simplified Chinese and explain intent or non-obvious ownership/concurrency rules.
- Run `GOWORK=off GOTOOLCHAIN=local go test ./... -count=1` and `go vet ./...` before committing standalone module changes.
- Use a local `go.work` file at your chosen workspace root only for cross-repository development; do not commit relative `replace` directives for normal library dependencies.
