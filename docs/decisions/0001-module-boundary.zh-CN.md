# ADR-001：保持 gnalloy 为独立 Gnalloy 模块

[English](0001-module-boundary.md) | [文档索引](../README.zh-CN.md)

## 状态

Accepted

## 背景

Gnalloy 以 `github.com/gnalloy` 下的聚焦仓库发布，公共 Go 导入路径位于 `gnalloy.org`。核心必须足够小，适合网络热路径，也适合只需要部分协议或传输能力的应用。

## 决策

`gnalloy` 作为 `gnalloy.org/gnalloy` 发布，并拥有该职责：Go 原生、受 Netty 启发的网络核心库，提供 ByteBuf、Channel、EventLoop、poller、bootstrap 与通用 codec 基座。

核心模块保持为 `gnalloy.org/gnalloy`。协议 codec、具体 transport、handler、resolver、observability adapter、examples 与 benchmark 工具依赖核心契约，而不是放进核心仓库。

## 结果

- 使用者可以只依赖需要的协议、传输、handler、resolver 或工具模块。
- 核心热路径避免可选协议、压缩、QUIC、OpenTelemetry、example 和 benchmark 依赖。
- 跨模块行为由应用、recipes、examples 或 benchmark harness 装配。
- 跨模块变更在推送前需要同时完成独立模块验证和集成 workspace 验证。
