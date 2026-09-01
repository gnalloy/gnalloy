# gnalloy

[简体中文](README.zh-CN.md) | [Documentation](docs/README.md)

Go-native, Netty-inspired networking core for buffers, channels, event loops, pollers, bootstrap, and generic codec primitives.

This is the core contract module. It owns ByteBuf, Channel, Pipeline, EventLoop, timer, queue, transport interfaces, bootstrap contracts, and generic codec primitives. Protocol implementations and concrete transports stay outside the core.

## Status

- Import path: `gnalloy.org/gnalloy`
- Repository: `github.com/gnalloy/gnalloy`
- Default branch: `dev`
- Preview install: `go get gnalloy.org/gnalloy@dev`
- License: Apache-2.0

## Install
```bash
go get gnalloy.org/gnalloy@dev
go doc gnalloy.org/gnalloy
GOWORK=off GOTOOLCHAIN=local go test ./... -count=1
```

## Documentation
- [Overview](docs/overview.md) ([中文](docs/overview.zh-CN.md))
- [Usage](docs/usage.md) ([中文](docs/usage.zh-CN.md))
- [Examples](docs/examples.md) ([中文](docs/examples.zh-CN.md))
- [Configuration](docs/configuration.md) ([中文](docs/configuration.zh-CN.md))
- [Testing and Performance](docs/testing.md) ([中文](docs/testing.zh-CN.md))
- [API Reference](docs/api.md) ([中文](docs/api.zh-CN.md))
- [Notes and Caveats](docs/notes.md) ([中文](docs/notes.zh-CN.md))
- [ADR-001 Module Boundary](docs/decisions/0001-module-boundary.md) ([中文](docs/decisions/0001-module-boundary.zh-CN.md))

## Module Boundary

This repository owns: Go-native, Netty-inspired networking core for buffers, channels, event loops, pollers, bootstrap, and generic codec primitives.

It does not absorb neighboring module responsibilities. Core primitives stay in `gnalloy.org/gnalloy`; protocol codecs, transports, handlers, resolvers, examples, and benchmarks stay in their own repositories.

## Packages
- `gnalloy.org/gnalloy` (`gnalloy`)
- `gnalloy.org/gnalloy/bootstrap` (`bootstrap`)
- `gnalloy.org/gnalloy/buffer` (`buffer`)
- `gnalloy.org/gnalloy/channel` (`channel`)
- `gnalloy.org/gnalloy/channel/embedded` (`embedded`)
- `gnalloy.org/gnalloy/channel/pool` (`pool`)
- `gnalloy.org/gnalloy/codec` (`codec`)
- `gnalloy.org/gnalloy/message` (`message`)
- `gnalloy.org/gnalloy/queue` (`queue`)
- `gnalloy.org/gnalloy/timer` (`timer`)
- `gnalloy.org/gnalloy/transport` (`transport`)
- `gnalloy.org/gnalloy/transport/poller` (`poller`)
- `gnalloy.org/gnalloy/transport/poller/epoll` (`epoll`)
- `gnalloy.org/gnalloy/transport/poller/iocp` (`iocp`)
- `gnalloy.org/gnalloy/transport/poller/iouring` (`iouring`)
- `gnalloy.org/gnalloy/transport/poller/kqueue` (`kqueue`)
- `gnalloy.org/gnalloy/transport/poller/memory` (`memory`)
- `gnalloy.org/gnalloy/transport/poller/std` (`std`)
- `gnalloy.org/gnalloy/validation/platformmatrix` (`platformmatrix`)

## Gnalloy Dependencies

- No direct Gnalloy module dependency is required by the current `go.mod`.

## Common Integration Pattern
- Configuration is passed through explicit constructors and option structs rather than package-level mutable state.
- Keep hot-path defaults conservative, bounded, and allocation-aware.

## Current Public Entry Points

The generated API reference lists the full public surface. Common constructors or option types currently include:
- `var ErrMissingGroup = errors.New("gnalloy/bootstrap: boss group and worker group are required") ...`
- `type ChannelFactory func(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error)`
- `type ClientConfig struct{ ... }`
- `type ServerConfig struct{ ... }`
- `var ErrReleasedBuffer = errors.New("gnalloy/buffer: buffer already released") ...`
- `type MmapAllocatorConfig struct{ ... }`
- `type PooledAllocatorConfig struct{ ... }`
- `var ErrDuplicateHandler = errors.New("gnalloy/channel: duplicate handler name") ...`
- `var OptionAutoRead = NewChannelOption("AUTO_READ", true) ...`
- `func NewUnsafeChannel(cfg UnsafeConfig) (*LocalChannel, *Unsafe)`
- `type ChannelOptions struct{ ... }`
- `type UnsafeConfig struct{ ... }`
- `var ErrClosed = errors.New("gnalloy/channel/embedded: channel closed")`
- `type Config struct{ ... }`
- `var ErrInvalidConfig = errors.New("gnalloy/channel/pool: invalid config") ...`
- `type Config struct{ ... }`

## Verification

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -count=1
GOWORK=off GOTOOLCHAIN=local go vet ./...
GOWORK=off GOTOOLCHAIN=local go test ./... -run '^$' -bench . -benchmem -count=1
```

For pressure tests, assemble this module with the relevant transport, codec, and handler stack and run the scenario from `gnalloy.org/benchmarks` or `gnalloy.org/examples`. Keep host, operating system, payload, concurrency, warmup, and repetitions in the report.

## Caveats
- This repository is intentionally narrow. Cross-module behavior should be assembled in applications, recipes, examples, or benchmark harnesses.
- Public APIs should remain Go-native and explicit; avoid runtime scanning, hidden global registries, and reflection-heavy behavior in hot paths.
- Treat network input as untrusted. Configure parser limits and return typed errors instead of panics.
- Keep benchmark claims tied to a concrete host, operating system, protocol, payload, concurrency, warmup, and repetition count.
