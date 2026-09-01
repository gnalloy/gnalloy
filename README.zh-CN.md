# gnalloy

[English](README.md) | [文档](docs/README.zh-CN.md)

Go 原生、受 Netty 启发的网络核心库，提供 ByteBuf、Channel、EventLoop、poller、bootstrap 与通用 codec 基座。

这是核心契约模块，负责 ByteBuf、Channel、Pipeline、EventLoop、timer、queue、transport interface、bootstrap 契约与通用 codec 基座。协议实现和具体传输不放进核心。

## 状态

- 导入路径：`gnalloy.org/gnalloy`
- 仓库：`github.com/gnalloy/gnalloy`
- 默认分支：`dev`
- 预览安装：`go get gnalloy.org/gnalloy@dev`
- 许可证：Apache-2.0

## 安装
```bash
go get gnalloy.org/gnalloy@dev
go doc gnalloy.org/gnalloy
GOWORK=off GOTOOLCHAIN=local go test ./... -count=1
```

## 文档
- [概览](docs/overview.zh-CN.md) ([English](docs/overview.md))
- [用法](docs/usage.zh-CN.md) ([English](docs/usage.md))
- [案例](docs/examples.zh-CN.md) ([English](docs/examples.md))
- [配置说明](docs/configuration.zh-CN.md) ([English](docs/configuration.md))
- [测试与性能](docs/testing.zh-CN.md) ([English](docs/testing.md))
- [API 参考](docs/api.zh-CN.md) ([English](docs/api.md))
- [注意事项与备注](docs/notes.zh-CN.md) ([English](docs/notes.md))
- [ADR-001 模块边界](docs/decisions/0001-module-boundary.zh-CN.md) ([English](docs/decisions/0001-module-boundary.md))

## 模块边界

本仓库负责：Go 原生、受 Netty 启发的网络核心库，提供 ByteBuf、Channel、EventLoop、poller、bootstrap 与通用 codec 基座。

它不吸收相邻模块职责。核心基础能力保留在 `gnalloy.org/gnalloy`；协议 codec、transport、handler、resolver、examples 与 benchmarks 分别由独立仓库负责。

## 包结构
- `gnalloy.org/gnalloy`（`gnalloy`）
- `gnalloy.org/gnalloy/bootstrap`（`bootstrap`）
- `gnalloy.org/gnalloy/buffer`（`buffer`）
- `gnalloy.org/gnalloy/channel`（`channel`）
- `gnalloy.org/gnalloy/channel/embedded`（`embedded`）
- `gnalloy.org/gnalloy/channel/pool`（`pool`）
- `gnalloy.org/gnalloy/codec`（`codec`）
- `gnalloy.org/gnalloy/message`（`message`）
- `gnalloy.org/gnalloy/queue`（`queue`）
- `gnalloy.org/gnalloy/timer`（`timer`）
- `gnalloy.org/gnalloy/transport`（`transport`）
- `gnalloy.org/gnalloy/transport/poller`（`poller`）
- `gnalloy.org/gnalloy/transport/poller/epoll`（`epoll`）
- `gnalloy.org/gnalloy/transport/poller/iocp`（`iocp`）
- `gnalloy.org/gnalloy/transport/poller/iouring`（`iouring`）
- `gnalloy.org/gnalloy/transport/poller/kqueue`（`kqueue`）
- `gnalloy.org/gnalloy/transport/poller/memory`（`memory`）
- `gnalloy.org/gnalloy/transport/poller/std`（`std`）
- `gnalloy.org/gnalloy/validation/platformmatrix`（`platformmatrix`）

## Gnalloy 依赖

- 当前 `go.mod` 没有直接 Gnalloy 模块依赖。

## 常见集成方式
- 配置通过显式构造函数和 option struct 传入，不使用包级可变状态。
- 热路径默认值必须保守、有界，并注意分配成本。

## 当前公共入口

生成的 API 参考列出了完整公共面。当前常用构造函数或 option 类型包括：
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

## 验证

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -count=1
GOWORK=off GOTOOLCHAIN=local go vet ./...
GOWORK=off GOTOOLCHAIN=local go test ./... -run '^$' -bench . -benchmem -count=1
```

压测时，将该模块和相应 transport、codec、handler 栈装配后，使用 `gnalloy.org/benchmarks` 或 `gnalloy.org/examples` 中的场景运行。报告必须保留主机、操作系统、payload、并发度、warmup 和 repetition。

## 注意事项
- 本仓库保持窄边界。跨模块行为应在应用、recipes、examples 或 benchmark harness 中装配。
- 公共 API 必须保持 Go 原生和显式；热路径避免运行时扫描、隐藏全局注册表和重反射。
- 网络输入一律视为不可信。配置解析上限，返回类型化错误，不使用 panic 处理输入错误。
- 性能结论必须绑定具体主机、操作系统、协议、payload、并发度、warmup 和 repetition。
