// Package embedded 提供内存态 Channel 测试工具。
//
// 该包对齐 Netty EmbeddedChannel 的核心体验：调用方可以直接驱动 inbound、
// outbound 和生命周期事件，在不启动真实 socket 的情况下验证 Pipeline 行为。
package embedded
