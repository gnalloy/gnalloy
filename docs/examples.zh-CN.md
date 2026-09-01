# 案例

[English](examples.md) | [文档索引](README.zh-CN.md)

## 案例 1：把模块加入应用

```bash
mkdir gnalloy-app && cd gnalloy-app
go mod init example.com/gnalloy-app
go get gnalloy.org/gnalloy@dev
go doc gnalloy.org/gnalloy
```

## 案例 2：把可执行测试作为行为示例

仓库测试是受支持行为的可执行示例。先从下面这些聚焦名称开始，再阅读对应 `_test.go` 文件中的完整 setup 和断言。

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run Test -count=1
```

当前测试和 benchmark 入口：
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

## 案例 3：Pipeline 装配形态

```go
// 从 docs/api.zh-CN.md 中选择该模块的具体构造函数。
// 顺序保持显式：transport -> decoder -> policy handlers -> application -> encoder。
pipeline := ch.Pipeline()
_ = pipeline.AddLast("module", component)
```

该片段只表达所有权形态。实际使用时请选择本仓库导出的具体类型，并搭配应用选定的 transport 模块。

## 案例 4：压测 Harness

持续负载测试时，将该模块接入 `gnalloy.org/benchmarks` 的场景，或接入 `gnalloy.org/examples` 的可运行客户端。报告中记录 host、OS、CPU、Go version、protocol、payload、concurrency、warmup、repetitions、throughput 和 p99 latency。
