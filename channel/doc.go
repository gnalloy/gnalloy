// Package channel 提供 Channel、Pipeline 和 Handler 契约。
//
// 稳定公共面：
//   - Channel 是业务层看到的连接抽象，不暴露 fd、CQE 或平台事件。
//   - Pipeline 和 HandlerContext 提供类似 Netty 的入站/出站事件传播。
//   - ChannelReadHandler、WriteHandler、FlushHandler、CloseHandler 等接口构成 Handler 扩展点。
//
// Unsafe 是传输层与业务 Pipeline 的边界，属于高级 API。应用业务代码通常不应直接依赖它；
// 新传输后端可通过 Unsafe 统一 readiness 与 completion 事件模型。
package channel
