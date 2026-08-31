// Package queue 提供 EventLoop 间通信使用的无锁队列。
//
// 当前稳定公共面是 MPSC。它适用于多个生产者向单个 EventLoop 投递任务的场景，
// 容量固定，不做动态扩容；Offer 返回 false 表示队列已满，调用方必须自行做背压或失败处理。
package queue
