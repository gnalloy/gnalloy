# 案例

[English](examples.md) | [文档索引](README.zh-CN.md)

## 案例 1：将模块加入应用

```bash
mkdir gnalloy-app && cd gnalloy-app
go mod init example.com/gnalloy-app
go get gnalloy.org/gnalloy@dev
go doc gnalloy.org/gnalloy
```

## 案例 2：查看当前包

当前源码树暴露这些 package 导入路径：
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

按需要的行为对对应 package 执行 `go doc`：

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

精选当前导出入口：
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

## 案例 3：将可执行测试作为行为示例

仓库测试是受支持行为的可执行示例。先从下面的精选名称开始，再阅读对应 `_test.go` 文件中的完整 setup 和断言。完整发现列表见 [测试与性能](testing.zh-CN.md)。

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run Test -count=1
```

精选当前 test、benchmark、fuzz 与 example 入口：
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

## 案例 4：跨模块装配

本模块的直接 Gnalloy 依赖：
- No direct Gnalloy module dependency is required by the current `go.mod`.

装配说明：
- 用核心模块承载 buffer、channel、event loop、timer、queue、transport contract 与通用 codec 基座。
- 协议 codec、具体 transport、handler、resolver、recipes、examples 与 benchmark harness 保持独立模块。
- 应用通过显式构造函数和 option struct 把核心与选定 transport、codec、handler 模块装配起来。

## 案例 5：压测 Harness

持续负载测试时，如果该模块参与网络流量路径，将它接入 `gnalloy.org/benchmarks` 的场景，或接入 `gnalloy.org/examples` 的可运行客户端。报告中记录 host、OS、CPU、Go version、protocol、payload、concurrency、warmup、repetitions、throughput 和 p99 latency。
