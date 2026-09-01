# 注意事项与备注

[English](notes.md) | [文档索引](README.zh-CN.md)

## 运行备注
- 本仓库保持窄边界。跨模块行为应在应用、recipes、examples 或 benchmark harness 中装配。
- 公共 API 必须保持 Go 原生和显式；热路径避免运行时扫描、隐藏全局注册表和重反射。
- 网络输入一律视为不可信。配置解析上限，返回类型化错误，不使用 panic 处理输入错误。
- 性能结论必须绑定具体主机、操作系统、协议、payload、并发度、warmup 和 repetition。

## 安全备注

- 对端不可信时，解析限制和超时边界必须显式。
- 不要在热路径日志里记录原始 payload、凭据、token、私钥或完整用户可控标识。
- TLS、ALPN、proxy 与 DNS 行为需要在最终应用装配栈中验证，因为这些策略跨越模块边界。

## 性能备注

- 只有目标场景测量结果才能驱动优化。microbenchmark 用于识别候选点，pressure test 用于证明端到端收益。
- 保持低分配所有权模型。只有模块边界或生命周期规则要求时才复制。
- throughput 和 latency 必须分开报告，尤其是持续并发下的 p99 latency。

## 发布备注

本文档不假设已经存在稳定 release tag。稳定 tag 发布前，消费者应固定 `@dev` 或明确 pseudo-version。

## 排障

- 多模块 checkout 中出现 package 缺失时，先用 `GOWORK=off` 独立运行该模块，再改依赖。
- 平台功能不可用时，先确认该模块是否在该操作系统上有意返回 unsupported 错误。
- benchmark 结果变化时，先用同一主机、payload、并发度、warmup 和 repetition 重跑，再调整阈值。
