package buffer

import "sync"

// Allocator 为 Channel 提供 ByteBuf，具体实现可来自堆、slab 或 mmap。
type Allocator interface {
	Acquire(size int) (ByteBuf, error)
	Release(buf *DirectByteBuf)
	Close() error
}

// FixedBufferProvider 暴露可注册到 completion 后端的稳定内存块。
// 该接口只用于控制面注册，热路径通过 ByteBuf 的 FixedBufferIndex 查询块号。
type FixedBufferProvider interface {
	FixedBuffers() [][]byte
}

// FixedBuffer 标识某个 ByteBuf 来自已注册的固定内存块。
type FixedBuffer interface {
	FixedBufferIndex() (uint16, bool)
}

// FixedBufferIndex 返回 ByteBuf 对应的 registered buffer 下标。
func FixedBufferIndex(buf ByteBuf) (uint16, bool) {
	fixed, ok := buf.(FixedBuffer)
	if !ok {
		return 0, false
	}
	return fixed.FixedBufferIndex()
}

// AllocatorStats 暴露 allocator 的容量与泄漏观测信息。
// OffHeap 为 true 表示 payload 不在 Go heap 上，适合 io_uring 共享内存场景。
type AllocatorStats struct {
	BlockSize int
	Blocks    int
	Fixed     int
	InUse     int
	Free      int
	Closed    bool
	OffHeap   bool

	// EventLoopID 是 allocator 所属 EventLoop 的低基数观测标签。
	// EventLoopLocal 为 false 时该字段没有归属语义。
	EventLoopID    uint32
	EventLoopLocal bool
}

// StatAllocator 是可观测 allocator 的可选接口，不影响热路径分配契约。
type StatAllocator interface {
	Allocator
	Stats() AllocatorStats
}

// HeapAllocator 是跨平台的基础分配器，用于测试、调试和通用场景。
type HeapAllocator struct {
	pool sync.Pool
}

func NewHeapAllocator() *HeapAllocator {
	return &HeapAllocator{}
}

func NewHeapBuffer(size int) *DirectByteBuf {
	if size < 0 {
		size = 0
	}
	return newDirectByteBuf(make([]byte, size), nil)
}

func (a *HeapAllocator) Acquire(size int) (ByteBuf, error) {
	if size <= 0 {
		return nil, ErrInvalidSize
	}
	if v := a.pool.Get(); v != nil {
		buf := v.(*DirectByteBuf)
		if cap(buf.data) >= size {
			buf.reset(buf.data[:size], a)
			return buf, nil
		}
	}
	return newDirectByteBuf(make([]byte, size), a), nil
}

func (a *HeapAllocator) Release(buf *DirectByteBuf) {
	if buf == nil {
		return
	}
	buf.readerIndex = 0
	buf.writerIndex = 0
	buf.owner = a
	a.pool.Put(buf)
}

func (a *HeapAllocator) releaseDirect(buf *DirectByteBuf) {
	a.Release(buf)
}

func (a *HeapAllocator) Close() error {
	return nil
}

func (a *HeapAllocator) Stats() AllocatorStats {
	return AllocatorStats{OffHeap: false}
}

// MmapAllocatorConfig 描述 Linux mmap allocator 的静态容量配置。
type MmapAllocatorConfig struct {
	BlockSize int
	Blocks    int
}
