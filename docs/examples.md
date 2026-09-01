# Examples

[简体中文](examples.zh-CN.md) | [Docs Index](README.md)

## Example 1: Add the Module to an Application

```bash
mkdir gnalloy-app && cd gnalloy-app
go mod init example.com/gnalloy-app
go get gnalloy.org/gnalloy@dev
go doc gnalloy.org/gnalloy
```

## Example 2: Inspect Current Packages

The current source tree exposes these package import paths:
- `gnalloy.org/gnalloy`
- `gnalloy.org/gnalloy/bootstrap`
- `gnalloy.org/gnalloy/buffer`
- `gnalloy.org/gnalloy/channel`
- `gnalloy.org/gnalloy/channel/embedded`
- `gnalloy.org/gnalloy/channel/pool`
- `gnalloy.org/gnalloy/codec`
- `gnalloy.org/gnalloy/message`
- `gnalloy.org/gnalloy/queue`
- `gnalloy.org/gnalloy/timer`
- `gnalloy.org/gnalloy/transport`
- `gnalloy.org/gnalloy/transport/poller`
- `gnalloy.org/gnalloy/transport/poller/epoll`
- `gnalloy.org/gnalloy/transport/poller/iocp`
- `gnalloy.org/gnalloy/transport/poller/iouring`
- `gnalloy.org/gnalloy/transport/poller/kqueue`
- `gnalloy.org/gnalloy/transport/poller/memory`
- `gnalloy.org/gnalloy/transport/poller/std`
- `gnalloy.org/gnalloy/validation/platformmatrix`

Use `go doc` against the package that matches the behavior you need:

```bash
go doc gnalloy.org/gnalloy
go doc gnalloy.org/gnalloy/bootstrap
go doc gnalloy.org/gnalloy/buffer
go doc gnalloy.org/gnalloy/channel
go doc gnalloy.org/gnalloy/channel/embedded
go doc gnalloy.org/gnalloy/channel/pool
go doc gnalloy.org/gnalloy/codec
go doc gnalloy.org/gnalloy/message
```

Selected current exported entry points:
- `var ErrMissingGroup = errors.New("gnalloy/bootstrap: boss group and worker group are required") ...`
- `func DefaultChannelFactory(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error)`
- `type ChannelFactory func(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error)`
- `type ChannelInitializer = ChildInitializer`
- `type ChildInitializer func(ch channel.Channel) error`
- `type ClientConfig struct{ ... }`
- `var ErrReleasedBuffer = errors.New("gnalloy/buffer: buffer already released") ...`
- `func ActiveLeakCount() int`
- `func ByteBufCompare(a ByteBuf, b ByteBuf) int`
- `func ByteBufEqual(a ByteBuf, b ByteBuf) bool`
- `func ByteBufHashCode(buf ByteBuf) uint32`
- `func BytesBefore(buf ByteBuf, fromIndex int, toIndex int, needle []byte) int`
- `var ErrDuplicateHandler = errors.New("gnalloy/channel: duplicate handler name") ...`
- `var OptionAutoRead = NewChannelOption("AUTO_READ", true) ...`
- `func NewUnsafeChannel(cfg UnsafeConfig) (*LocalChannel, *Unsafe)`
- `type AttributeAssignment struct{ ... }`
- `type AttributeKey[T any] struct{ ... }`
- `type AttributeMap struct{ ... }`

## Example 3: Use Executable Tests as Behavioral Examples

Repository tests are executable examples of supported behavior. Start with the selected names below, then read the matching `_test.go` files for complete setup and assertions. See [Testing and Performance](testing.md) for the complete discovered list.

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run Test -count=1
```

Selected current test, benchmark, fuzz, and example entry points:
- `BenchmarkBase64DecoderComposite`
- `BenchmarkBase64EncoderComposite`
- `BenchmarkByteSliceDecoderComposite`
- `BenchmarkByteToMessageListDecoder`
- `BenchmarkChannelPoolMapGet`
- `BenchmarkCompositeGetByteFragmented`
- `BenchmarkCompositeIndexByteFragmented`
- `BenchmarkCompositeReadableSlicesFullComponents`
- `BenchmarkCompositeReadableSlicesPartialComponents`
- `BenchmarkCopyReadableBytesComposite`
- `BenchmarkDelimiterBasedFrameDecoder`
- `BenchmarkDelimiterBasedFrameDecoderFragmented`
- `BenchmarkEventLoopSubmitBurst`
- `BenchmarkFileRegionEncoderChunks`
- `BenchmarkFixedLengthFrameDecoder`
- `BenchmarkFixedPoolGetPut`
- `BenchmarkHeapAllocatorAcquireRelease`
- `BenchmarkLengthFieldDecoder`

## Example 4: Cross-Module Assembly

Direct Gnalloy dependencies for this module:
- No direct Gnalloy module dependency is required by the current `go.mod`.

Assembly guidance:
- Use this core module for buffers, channels, event loops, timers, queues, transport contracts, and generic codec primitives.
- Keep protocol-specific codecs, concrete transports, handlers, resolvers, recipes, examples, and benchmark harnesses in their own modules.
- Applications assemble the core with the selected transport, codec, and handler modules through explicit constructors and option structs.

## Example 5: Pressure-Test Harness

For sustained load, wire this module into a scenario under `gnalloy.org/benchmarks` or a runnable client under `gnalloy.org/examples` when the module participates in network traffic. Record host, OS, CPU, Go version, protocol, payload, concurrency, warmup, repetitions, throughput, and p99 latency in the report.
