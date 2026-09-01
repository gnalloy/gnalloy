# 概览

[English](overview.md) | [文档索引](README.zh-CN.md)

## 目标

Go 原生、受 Netty 启发的网络核心库，提供 ByteBuf、Channel、EventLoop、poller、bootstrap 与通用 codec 基座。

这是核心契约模块，负责 ByteBuf、Channel、Pipeline、EventLoop、timer、queue、transport interface、bootstrap 契约与通用 codec 基座。协议实现和具体传输不放进核心。

## 仓库身份

- 模块路径：`gnalloy.org/gnalloy`
- GitHub 仓库：`github.com/gnalloy/gnalloy`
- 默认分支：`dev`
- 许可证：Apache-2.0

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

## 直接 Gnalloy 依赖
- 按仓库规划，该模块没有直接 Gnalloy 模块依赖。

## 当前模块规划中的直接下游
- `gnalloy.org/benchmarks`
- `gnalloy.org/codec-compression`
- `gnalloy.org/codec-dns`
- `gnalloy.org/codec-haproxy`
- `gnalloy.org/codec-http1`
- `gnalloy.org/codec-http2`
- `gnalloy.org/codec-http3`
- `gnalloy.org/codec-icmp`
- `gnalloy.org/codec-ip`
- `gnalloy.org/codec-memcache`
- `gnalloy.org/codec-mqtt`
- `gnalloy.org/codec-protobuf`
- `gnalloy.org/codec-redis`
- `gnalloy.org/codec-rtsp`
- `gnalloy.org/codec-sctp`
- `gnalloy.org/codec-smtp`
- `gnalloy.org/codec-socks`
- `gnalloy.org/codec-spdy`
- `gnalloy.org/codec-stomp`
- `gnalloy.org/codec-websocket`
- `gnalloy.org/codec-xml`
- `gnalloy.org/examples`
- `gnalloy.org/handler-cors`
- `gnalloy.org/handler-executor`
- `gnalloy.org/handler-flow`
- `gnalloy.org/handler-flush`
- `gnalloy.org/handler-ipfilter`
- `gnalloy.org/handler-logging`
- `gnalloy.org/handler-metrics`
- `gnalloy.org/handler-pcap`
- `gnalloy.org/handler-proxy`
- `gnalloy.org/handler-timeout`
- `gnalloy.org/handler-tls`
- `gnalloy.org/handler-traffic`
- `gnalloy.org/observability`
- `gnalloy.org/observability-otel`
- `gnalloy.org/protocol`
- `gnalloy.org/recipes`
- `gnalloy.org/transport-http3`
- `gnalloy.org/transport-l2`
- `gnalloy.org/transport-local`
- `gnalloy.org/transport-quic`
- `gnalloy.org/transport-raw`
- `gnalloy.org/transport-sctp`
- `gnalloy.org/transport-tcp`
- `gnalloy.org/transport-udp`
- `gnalloy.org/transport-unix`
- `gnalloy.org/transport-webtransport`
- `gnalloy.org/transport-zerocopy`

## 架构位置

Gnalloy 保持核心小而轻依赖。本仓库围绕单一职责形成可替换模块，通过显式 Go package 连接，而不是依靠运行时发现。

## 兼容性

公共导入路径是 `gnalloy.org/gnalloy`。首个稳定 tag 发布前，请按依赖策略使用 `@dev` 或明确的 pseudo-version。
