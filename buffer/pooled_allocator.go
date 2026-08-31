package buffer

import (
	"math/bits"
	"sync/atomic"
)

const pooledOversizedClass = ^uint32(0)

// PooledAllocatorConfig 描述跨平台 size-class allocator 的容量策略。
type PooledAllocatorConfig struct {
	// MinSize 是最小 size class，0 表示 256 字节。
	MinSize int
	// MaxSize 是最大可池化 size class，0 表示 64 KiB。
	MaxSize int
	// MaxCachedPerClass 限制每个 size class 可回收对象数，0 表示 1024。
	MaxCachedPerClass int
	// SizeClasses 显式指定 size class；为空时使用 2 的幂次。
	SizeClasses []int
	// ZeroOnAcquire 在复用 buffer 时清零底层内存，默认关闭以保留热路径性能。
	ZeroOnAcquire bool
}

// PooledSizeClassStats 描述一个 size class 的运行时快照。
type PooledSizeClassStats struct {
	Size     int
	Cached   int64
	InUse    int64
	Acquired uint64
	Reused   uint64
	Dropped  uint64
}

// PooledAllocatorStats 描述 PooledAllocator 的完整观测快照。
type PooledAllocatorStats struct {
	Classes   []PooledSizeClassStats
	Oversized int64
	Closed    bool
}

type pooledSizeClass struct {
	size     int
	cache    chan *DirectByteBuf
	inUse    atomic.Int64
	acquired atomic.Uint64
	reused   atomic.Uint64
	dropped  atomic.Uint64
}

// PooledAllocator 是 Netty PooledByteBufAllocator 的 Go 化基础实现。
//
// 该实现以 size class 为边界缓存 DirectByteBuf 对象和底层 byte slice，
// 适合跨平台 TCP/HTTP 热路径；Linux 固定块 off-heap 场景仍优先使用 mmap allocator。
type PooledAllocator struct {
	classes       []pooledSizeClass
	zeroOnAcquire bool
	closed        atomic.Bool
	oversized     atomic.Int64
}

// NewPooledAllocator 创建跨平台池化 allocator。
func NewPooledAllocator(cfg PooledAllocatorConfig) (*PooledAllocator, error) {
	classes, err := normalizeSizeClasses(cfg)
	if err != nil {
		return nil, err
	}
	maxCached := cfg.MaxCachedPerClass
	if maxCached == 0 {
		maxCached = 1024
	}
	if maxCached < 0 {
		return nil, ErrInvalidSize
	}
	a := &PooledAllocator{
		classes:       make([]pooledSizeClass, len(classes)),
		zeroOnAcquire: cfg.ZeroOnAcquire,
	}
	for i, size := range classes {
		a.classes[i].size = size
		a.classes[i].cache = make(chan *DirectByteBuf, maxCached)
	}
	return a, nil
}

// Acquire 返回容量不小于 size 的 ByteBuf。
func (a *PooledAllocator) Acquire(size int) (ByteBuf, error) {
	if a == nil || a.closed.Load() {
		return nil, ErrAllocatorClosed
	}
	if size <= 0 {
		return nil, ErrInvalidSize
	}
	idx := a.classIndex(size)
	if idx < 0 {
		a.oversized.Add(1)
		buf := newDirectByteBuf(make([]byte, size), a)
		buf.ownerIndex = pooledOversizedClass
		return buf, nil
	}
	class := &a.classes[idx]
	class.acquired.Add(1)
	select {
	case buf := <-class.cache:
		class.reused.Add(1)
		buf.reset(buf.data[:class.size], a)
		buf.ownerIndex = uint32(idx)
		if a.zeroOnAcquire {
			clear(buf.data)
		}
		class.inUse.Add(1)
		return buf, nil
	default:
	}
	buf := newDirectByteBuf(make([]byte, class.size), a)
	buf.ownerIndex = uint32(idx)
	class.inUse.Add(1)
	return buf, nil
}

// Release 释放 DirectByteBuf；非本 allocator 产出的 buffer 会被忽略。
func (a *PooledAllocator) Release(buf *DirectByteBuf) {
	if a == nil || buf == nil {
		return
	}
	if buf.owner != a {
		return
	}
	a.releaseOwnedDirect(buf)
}

func (a *PooledAllocator) releaseDirect(buf *DirectByteBuf) {
	if a == nil || buf == nil {
		return
	}
	a.releaseOwnedDirect(buf)
}

func (a *PooledAllocator) releaseOwnedDirect(buf *DirectByteBuf) {
	idx := buf.ownerIndex
	if idx == pooledOversizedClass || int(idx) >= len(a.classes) {
		a.oversized.Add(-1)
		return
	}
	class := &a.classes[idx]
	class.inUse.Add(-1)
	if a.closed.Load() {
		class.dropped.Add(1)
		return
	}
	buf.readerIndex = 0
	buf.writerIndex = 0
	if a.zeroOnAcquire {
		clear(buf.data)
	}
	select {
	case class.cache <- buf:
	default:
		class.dropped.Add(1)
	}
}

// Close 标记 allocator 不再接受新分配，已借出的 buffer 归还后会直接丢弃。
func (a *PooledAllocator) Close() error {
	if a == nil {
		return nil
	}
	a.closed.Store(true)
	return nil
}

// Stats 返回通用 allocator 统计，用于和 mmap/heap allocator 统一观测。
func (a *PooledAllocator) Stats() AllocatorStats {
	if a == nil {
		return AllocatorStats{Closed: true}
	}
	var blocks, inUse, free int
	blockSize := 0
	for i := range a.classes {
		class := &a.classes[i]
		if class.size > blockSize {
			blockSize = class.size
		}
		cached := len(class.cache)
		active := int(class.inUse.Load())
		blocks += cached + active
		inUse += active
		free += cached
	}
	return AllocatorStats{
		BlockSize: blockSize,
		Blocks:    blocks,
		InUse:     inUse,
		Free:      free,
		Closed:    a.closed.Load(),
		OffHeap:   false,
	}
}

// PooledStats 返回包含 size class 细节的快照。
func (a *PooledAllocator) PooledStats() PooledAllocatorStats {
	if a == nil {
		return PooledAllocatorStats{Closed: true}
	}
	stats := PooledAllocatorStats{
		Classes:   make([]PooledSizeClassStats, len(a.classes)),
		Oversized: a.oversized.Load(),
		Closed:    a.closed.Load(),
	}
	for i := range a.classes {
		class := &a.classes[i]
		stats.Classes[i] = PooledSizeClassStats{
			Size:     class.size,
			Cached:   int64(len(class.cache)),
			InUse:    class.inUse.Load(),
			Acquired: class.acquired.Load(),
			Reused:   class.reused.Load(),
			Dropped:  class.dropped.Load(),
		}
	}
	return stats
}

func (a *PooledAllocator) classIndex(size int) int {
	for i := range a.classes {
		if size <= a.classes[i].size {
			return i
		}
	}
	return -1
}

func normalizeSizeClasses(cfg PooledAllocatorConfig) ([]int, error) {
	if len(cfg.SizeClasses) > 0 {
		return normalizeExplicitSizeClasses(cfg.SizeClasses)
	}
	minSize := cfg.MinSize
	if minSize == 0 {
		minSize = 256
	}
	maxSize := cfg.MaxSize
	if maxSize == 0 {
		maxSize = 64 * 1024
	}
	if minSize <= 0 || maxSize <= 0 || minSize > maxSize {
		return nil, ErrInvalidSize
	}
	minSize = nextPowerOfTwo(minSize)
	maxSize = nextPowerOfTwo(maxSize)
	classes := make([]int, 0, bits.Len(uint(maxSize/minSize))+1)
	for size := minSize; size <= maxSize; size <<= 1 {
		classes = append(classes, size)
		if size > maxSize/2 {
			break
		}
	}
	return classes, nil
}

func normalizeExplicitSizeClasses(classes []int) ([]int, error) {
	out := make([]int, 0, len(classes))
	last := 0
	for _, size := range classes {
		if size <= 0 || size <= last {
			return nil, ErrInvalidSize
		}
		out = append(out, size)
		last = size
	}
	return out, nil
}

func nextPowerOfTwo(v int) int {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(v-1))
}
