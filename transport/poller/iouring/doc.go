// Package iouring 实现 Linux io_uring completion poller。
//
// 该包通过 golang.org/x/sys/unix 直接调用 io_uring setup/enter 与 mmap ring。
// accept/read/write/close 都以 SQE/CQE 完成事件上抛；提交到 IORequest 的 ByteBuf
// 会在 pending 期间 Retain，并在完成、关闭或提交失败回滚时 Release。
package iouring
