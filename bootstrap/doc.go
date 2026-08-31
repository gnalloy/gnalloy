// Package bootstrap 提供服务端启动装配 API。
//
// 稳定公共面：
//   - ServerBootstrap 用于绑定 Boss/Worker EventLoopGroup、传输实现和子 Channel 初始化器。
//   - Server 表示绑定后的服务端句柄，当前仅承诺 Addr 与 Close 生命周期。
//   - ServerTransport 是 transport/tcp 等传输包接入 bootstrap 的最小接口。
//
// bootstrap 不持有平台 fd，也不暴露 poller 细节；平台能力应通过具体
// ServerTransport 的 Config 注入。
package bootstrap
