// Package transport 提供 EventLoop、EventLoopGroup 和 Poller 装配契约。
//
// 稳定公共面：
//   - EventLoop 表示单线程 I/O 与任务执行单元。
//   - EventLoopGroup 管理一组 EventLoop，并按 Round-Robin 分配 Channel。
//   - EventLoopLocal 为每个 EventLoop 绑定懒加载本地资源。
//   - Config、BackendKind 和 NewPoller 描述平台 poller 选择。
//
// 该包不直接实现 TCP 协议生命周期；TCP 服务端位于 transport/tcp。
// 平台 poller 实现位于 transport/poller 子包，后续可按当前接口拆分为独立仓库。
package transport
