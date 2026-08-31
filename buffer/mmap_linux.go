//go:build linux

package buffer

import "golang.org/x/sys/unix"

const maxInt = int(^uint(0) >> 1)
const maxFixedBufferCount = 1 << 16

type mmapAllocator struct {
	data      []byte
	blockSize int
	buffers   []DirectByteBuf
	freeList  []uint32
	inUse     []bool
	freeTop   int
	inUseCnt  int
	closed    bool
}

// NewMmapAllocator 创建 Linux mmap-backed allocator。
// 该分配器面向单 EventLoop 使用，payload 来自 mmap，分配路径无锁、无 make。
func NewMmapAllocator(cfg MmapAllocatorConfig) (Allocator, error) {
	if cfg.BlockSize <= 0 || cfg.Blocks <= 0 {
		return nil, ErrInvalidSize
	}
	if cfg.BlockSize > maxInt/cfg.Blocks {
		return nil, ErrInvalidSize
	}
	data, err := unix.Mmap(-1, 0, cfg.BlockSize*cfg.Blocks, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANONYMOUS|unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	a := &mmapAllocator{
		data:      data,
		blockSize: cfg.BlockSize,
		buffers:   make([]DirectByteBuf, cfg.Blocks),
		freeList:  make([]uint32, cfg.Blocks),
		inUse:     make([]bool, cfg.Blocks),
		freeTop:   cfg.Blocks - 1,
	}
	for i := 0; i < cfg.Blocks; i++ {
		a.freeList[i] = uint32(i)
		start := i * cfg.BlockSize
		a.buffers[i].data = a.data[start : start+cfg.BlockSize]
		a.buffers[i].owner = a
		a.buffers[i].ownerIndex = uint32(i)
	}
	return a, nil
}

func (a *mmapAllocator) Acquire(size int) (ByteBuf, error) {
	if a.closed {
		return nil, ErrAllocatorClosed
	}
	if size <= 0 || size > a.blockSize {
		return nil, ErrInvalidSize
	}
	if a.freeTop < 0 {
		return nil, ErrAllocatorExhausted
	}
	idx := a.freeList[a.freeTop]
	a.freeTop--
	a.inUse[idx] = true
	a.inUseCnt++
	buf := &a.buffers[idx]
	buf.reset(buf.data[:a.blockSize], a)
	buf.ownerIndex = idx
	return buf, nil
}

func (a *mmapAllocator) Release(buf *DirectByteBuf) {
	if buf == nil || a.closed {
		return
	}
	idx := buf.ownerIndex
	if int(idx) >= len(a.buffers) || &a.buffers[idx] != buf {
		return
	}
	if !a.inUse[idx] {
		return
	}
	buf.readerIndex = 0
	buf.writerIndex = 0
	a.inUse[idx] = false
	a.inUseCnt--
	a.freeTop++
	if a.freeTop >= len(a.freeList) {
		a.freeTop = len(a.freeList) - 1
		return
	}
	a.freeList[a.freeTop] = idx
}

func (a *mmapAllocator) releaseDirect(buf *DirectByteBuf) {
	a.Release(buf)
}

func (a *mmapAllocator) Stats() AllocatorStats {
	free := 0
	if a.freeTop >= 0 {
		free = a.freeTop + 1
	}
	return AllocatorStats{
		BlockSize: a.blockSize,
		Blocks:    len(a.buffers),
		Fixed:     len(a.buffers),
		InUse:     a.inUseCnt,
		Free:      free,
		Closed:    a.closed,
		OffHeap:   true,
	}
}

func (a *mmapAllocator) FixedBuffers() [][]byte {
	if a.closed || len(a.buffers) == 0 || len(a.buffers) > maxFixedBufferCount {
		return nil
	}
	out := make([][]byte, len(a.buffers))
	for i := range a.buffers {
		out[i] = a.buffers[i].data[:a.blockSize]
	}
	return out
}

func (a *mmapAllocator) fixedBufferIndex(buf *DirectByteBuf) (uint16, bool) {
	if buf == nil || a.closed {
		return 0, false
	}
	idx := buf.ownerIndex
	if int(idx) >= len(a.buffers) || idx >= maxFixedBufferCount || &a.buffers[idx] != buf {
		return 0, false
	}
	return uint16(idx), true
}

func (a *mmapAllocator) Close() error {
	if a.data == nil {
		return nil
	}
	if a.inUseCnt > 0 {
		return ErrAllocatorInUse
	}
	a.closed = true
	err := unix.Munmap(a.data)
	a.data = nil
	a.buffers = nil
	a.freeList = nil
	a.inUse = nil
	a.freeTop = -1
	return err
}
