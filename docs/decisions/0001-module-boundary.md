# ADR-001: Keep gnalloy as an Independent Gnalloy Module

## Status
Accepted

## Date
2026-08-31

## Context
Gnalloy is split from the previous single `goark.dev/gnalloy` module into focused repositories under `github.com/gnalloy`. The public import domain is `gnalloy.org`.

## Decision
`gnalloy` is published as `gnalloy.org/gnalloy` and owns this responsibility: Go-native, Netty-inspired networking core for buffers, channels, event loops, pollers, bootstrap, and generic codec primitives.

The core module remains `gnalloy.org/gnalloy`. Protocol codecs, concrete transports, handlers, resolvers, examples, and benchmark tooling depend on the core contracts instead of living inside the core repository.

## Consequences
- Consumers can depend on only the protocol, transport, or handler they actually need.
- Hot-path core code avoids optional protocol, compression, QUIC, OpenTelemetry, and benchmark dependencies.
- Cross-module changes require workspace verification plus standalone module verification before push.
