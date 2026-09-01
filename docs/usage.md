# Usage

[简体中文](usage.zh-CN.md) | [Docs Index](README.md)

## Requirements

- Go 1.25 or newer, matching the module `go` directive.
- A Gnalloy application, recipe, example, or benchmark harness that owns lifecycle and deployment configuration.
- Standalone module verification should set `GOWORK=off` so the module is tested through its published dependency graph.

## Install
```bash
go get gnalloy.org/gnalloy@dev
```

## Import
```go
import "gnalloy.org/gnalloy"
```

## Integration Pattern
- Configuration is passed through explicit constructors and option structs rather than package-level mutable state.
- Keep hot-path defaults conservative, bounded, and allocation-aware.

## API Selection

Use the API inventory to choose the exact constructor or option type for your protocol path:

```bash
go doc gnalloy.org/gnalloy
```

Common current entry points:
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

## Cross-Module Assembly

When multiple Gnalloy repositories are developed together, create a local `go.work` file in your chosen workspace. Keep application-local `replace` directives out of published library modules unless the change is intentionally temporary and never committed.

## Error Handling

Network input, peer behavior, platform capability, and timeout failures must be handled as normal errors. Do not recover protocol correctness by panicking. Return or propagate the module error and close the affected Channel when ownership requires it.
