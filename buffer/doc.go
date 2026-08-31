// Package buffer 提供 gnalloy 的 ByteBuf 与 allocator 契约。
//
// 稳定公共面：
//   - ByteBuf 是业务 Handler 读写字节的核心接口，支持读写指针、零拷贝 Slice 和显式引用计数。
//   - CompositeByteBuf 负责跨多个 ByteBuf 组成连续逻辑视图，用于半包累积和跨组件零拷贝切帧。
//   - Allocator 为 Channel 提供 ByteBuf，调用方必须遵守 Retain/Release 生命周期。
//   - StatAllocator 是可选观测接口，用于压测后检查 in-use/free 与 off-heap 状态。
//   - HeapAllocator 是跨平台调试与默认实现；Linux mmap allocator 面向 per-EventLoop off-heap 场景。
//
// ByteBuf 的 Bytes 方法返回视图，不转移所有权。跨异步边界保存 ByteBuf 时必须先 Retain，
// 使用完成后必须 Release。
package buffer
