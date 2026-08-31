// Package epoll 实现 Linux epoll ET readiness poller。
//
// 该包是平台后端实现包，公共构造函数仅用于 transport.NewPoller 或高级用户显式选择后端。
// fd 必须是非阻塞模式；读写方必须循环处理到 EAGAIN。
package epoll
