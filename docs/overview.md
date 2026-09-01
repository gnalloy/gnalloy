# Overview

[简体中文](overview.zh-CN.md) | [Docs Index](README.md)

## Purpose

Go-native, Netty-inspired networking core for buffers, channels, event loops, pollers, bootstrap, and generic codec primitives.

This is the core contract module. It owns ByteBuf, Channel, Pipeline, EventLoop, timer, queue, transport interfaces, bootstrap contracts, and generic codec primitives. Protocol implementations and concrete transports stay outside the core.

## Repository Identity

- Module path: `gnalloy.org/gnalloy`
- GitHub repository: `github.com/gnalloy/gnalloy`
- Default branch: `dev`
- License: Apache-2.0

## Package Map
- `gnalloy.org/gnalloy` (`gnalloy`)
- `gnalloy.org/gnalloy/bootstrap` (`bootstrap`)
- `gnalloy.org/gnalloy/buffer` (`buffer`)
- `gnalloy.org/gnalloy/channel` (`channel`)
- `gnalloy.org/gnalloy/channel/embedded` (`embedded`)
- `gnalloy.org/gnalloy/channel/pool` (`pool`)
- `gnalloy.org/gnalloy/codec` (`codec`)
- `gnalloy.org/gnalloy/message` (`message`)
- `gnalloy.org/gnalloy/queue` (`queue`)
- `gnalloy.org/gnalloy/timer` (`timer`)
- `gnalloy.org/gnalloy/transport` (`transport`)
- `gnalloy.org/gnalloy/transport/poller` (`poller`)
- `gnalloy.org/gnalloy/transport/poller/epoll` (`epoll`)
- `gnalloy.org/gnalloy/transport/poller/iocp` (`iocp`)
- `gnalloy.org/gnalloy/transport/poller/iouring` (`iouring`)
- `gnalloy.org/gnalloy/transport/poller/kqueue` (`kqueue`)
- `gnalloy.org/gnalloy/transport/poller/memory` (`memory`)
- `gnalloy.org/gnalloy/transport/poller/std` (`std`)
- `gnalloy.org/gnalloy/validation/platformmatrix` (`platformmatrix`)

## Direct Gnalloy Dependencies
- No direct Gnalloy module dependency is required by the repository plan.

## Direct Dependents in the Current Module Plan
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

## Architecture Position

Gnalloy keeps the core small and dependency-light. This repository is a replaceable module around one responsibility, connected through explicit Go packages instead of runtime discovery.

## Compatibility

The public import path is `gnalloy.org/gnalloy`. Until the first stable tag is published, use `@dev` or an explicit pseudo-version selected by your dependency policy.
