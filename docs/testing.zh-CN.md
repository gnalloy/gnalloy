# 测试与性能

[English](testing.md) | [文档索引](README.zh-CN.md)

## 必跑检查

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -count=1
GOWORK=off GOTOOLCHAIN=local go vet ./...
gofmt -l .
git diff --check
```

## 聚焦行为检查

处理小范围行为变更时先跑聚焦测试：

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run 'TestName' -count=1
```

当前发现的测试和 benchmark 入口：
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

## Race 检查

```bash
GOWORK=off GOTOOLCHAIN=local go test -race ./... -count=1
```

Race 检查对 core、transport、handler、resolver、observability、examples 和 benchmark 模块最有价值。平台相关 transport 可能需要原生主机能力。

## 基准测试

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run '^$' -bench . -benchmem -benchtime=1s -count=5
```

`ns/op`、`B/op`、`allocs/op`、throughput 和 p99 latency 要分开报告。每份结果都要包含 host 与 OS 信息。

## 压测

压测应针对真实装配栈运行。使用 `gnalloy.org/benchmarks` 维护可重复矩阵，使用 `gnalloy.org/examples` 运行客户端。warmup 和 measurement 阶段必须分离。

## CI

仓库 validation workflow 会在 Linux、macOS 与 Windows 上为 push 和 pull request 运行格式检查、测试和 vet。
