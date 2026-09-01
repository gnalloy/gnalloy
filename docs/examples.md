# Examples

[简体中文](examples.zh-CN.md) | [Docs Index](README.md)

## Example 1: Add the Module to an Application

```bash
mkdir gnalloy-app && cd gnalloy-app
go mod init example.com/gnalloy-app
go get gnalloy.org/gnalloy@dev
go doc gnalloy.org/gnalloy
```

## Example 2: Use Executable Tests as Behavioral Examples

The repository tests are executable examples of supported behavior. Start with the focused names below, then read the matching `_test.go` files for complete setup and assertions.

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run Test -count=1
```

Current test and benchmark entry points:
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

## Example 3: Pipeline Assembly Shape

```go
// Select the concrete constructor from docs/api.md for this module.
// Keep the order explicit: transport -> decoder -> policy handlers -> application -> encoder.
pipeline := ch.Pipeline()
_ = pipeline.AddLast("module", component)
```

The snippet shows ownership shape only. Use the exact exported type from this repository and the transport module selected by your application.

## Example 4: Pressure-Test Harness

For sustained load, wire this module into a scenario under `gnalloy.org/benchmarks` or a runnable client under `gnalloy.org/examples`. Record host, OS, CPU, Go version, protocol, payload, concurrency, warmup, repetitions, throughput, and p99 latency in the report.
