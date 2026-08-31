// Package poller 定义 gnalloy 平台 I/O 后端的最小接口。
//
// Poller 同时覆盖 readiness 模型和 completion 模型：
//   - readiness 后端返回 fd 可读/可写状态，例如 epoll 和 kqueue。
//   - completion 后端返回已完成的 I/O 请求，例如 io_uring 和 IOCP。
//
// 上层 transport.EventLoop 只依赖本包类型。新增平台后端必须保持 Event、IORequest
// 和 Poller 的语义稳定，避免向业务 Channel 泄露平台细节。
package poller
