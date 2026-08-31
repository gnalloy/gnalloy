// Package iocp 实现 Windows IOCP completion poller。
//
// 该包封装 CreateIoCompletionPort、AcceptEx、WSARecv、WSASend 和 close completion。
// 提交到 IORequest 的 ByteBuf 会在 OVERLAPPED pending 期间 Retain。
package iocp
