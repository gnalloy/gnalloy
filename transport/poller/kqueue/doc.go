// Package kqueue 实现 macOS/BSD kqueue readiness poller。
//
// 该包是平台后端实现包，使用 EV_CLEAR 语义对齐边缘触发读取模型。
package kqueue
