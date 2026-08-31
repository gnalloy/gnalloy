# gnalloy

Go 原生、受 Netty 启发的网络核心库，提供 ByteBuf、Channel、EventLoop、poller、bootstrap 与通用 codec 基座。

本仓库属于 Gnalloy 模块化网络栈。默认分支是 `dev`；本次初始化不创建 release，不打 tag。

## 安装

```bash
go get gnalloy.org/gnalloy@dev
```

## 模块边界

- 模块路径：`gnalloy.org/gnalloy`
- 职责：Go 原生、受 Netty 启发的网络核心库，提供 ByteBuf、Channel、EventLoop、poller、bootstrap 与通用 codec 基座
- 核心依赖：需要 Gnalloy ByteBuf、Channel、EventLoop 或 Bootstrap 契约时依赖 `gnalloy.org/gnalloy`。

## Gnalloy 依赖

- 无 Gnalloy 扩展依赖。

## 开发验证

```bash
go test ./... -count=1
go vet ./...
go test ./... -run '^$' -bench . -benchmem -benchtime=100ms -count=1
```

跨仓库开发使用 `G:\opensource\gnalloy\go.work`。独立模块复验时设置 `GOWORK=off`。

## 许可证

Apache-2.0。
