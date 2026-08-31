package buffer

import (
	"errors"
	"testing"
)

func TestPooledAllocatorReusesNearestSizeClass(t *testing.T) {
	alloc, err := NewPooledAllocator(PooledAllocatorConfig{
		SizeClasses:       []int{128, 512},
		MaxCachedPerClass: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := alloc.Acquire(129)
	if err != nil {
		t.Fatal(err)
	}
	if first.Capacity() != 512 {
		t.Fatalf("capacity=%d, want 512", first.Capacity())
	}
	first.Release()

	second, err := alloc.Acquire(256)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if second.Capacity() != 512 {
		t.Fatalf("capacity=%d, want 512", second.Capacity())
	}
	stats := alloc.PooledStats()
	if stats.Classes[1].Reused != 1 || stats.Classes[1].InUse != 1 {
		t.Fatalf("class stats=%+v, want reused=1 inUse=1", stats.Classes[1])
	}
}

func TestPooledAllocatorDropsWhenClassCacheFull(t *testing.T) {
	alloc, err := NewPooledAllocator(PooledAllocatorConfig{
		SizeClasses:       []int{64},
		MaxCachedPerClass: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := alloc.Acquire(8)
	if err != nil {
		t.Fatal(err)
	}
	second, err := alloc.Acquire(8)
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	second.Release()

	stats := alloc.PooledStats()
	if stats.Classes[0].Cached != 1 || stats.Classes[0].Dropped != 1 {
		t.Fatalf("class stats=%+v, want cached=1 dropped=1", stats.Classes[0])
	}
}

func TestPooledAllocatorOversizedBuffersAreNotCached(t *testing.T) {
	alloc, err := NewPooledAllocator(PooledAllocatorConfig{SizeClasses: []int{64}})
	if err != nil {
		t.Fatal(err)
	}
	buf, err := alloc.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	if buf.Capacity() != 128 {
		t.Fatalf("capacity=%d, want 128", buf.Capacity())
	}
	if got := alloc.PooledStats().Oversized; got != 1 {
		t.Fatalf("oversized=%d, want 1", got)
	}
	buf.Release()
	if got := alloc.PooledStats().Oversized; got != 0 {
		t.Fatalf("oversized=%d, want 0", got)
	}
}

func TestPooledAllocatorCloseRejectsAcquireAndDropsRelease(t *testing.T) {
	alloc, err := NewPooledAllocator(PooledAllocatorConfig{SizeClasses: []int{64}})
	if err != nil {
		t.Fatal(err)
	}
	buf, err := alloc.Acquire(32)
	if err != nil {
		t.Fatal(err)
	}
	if err := alloc.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := alloc.Acquire(32); !errors.Is(err, ErrAllocatorClosed) {
		t.Fatalf("err=%v, want %v", err, ErrAllocatorClosed)
	}
	buf.Release()
	stats := alloc.PooledStats()
	if !stats.Closed || stats.Classes[0].Cached != 0 {
		t.Fatalf("stats=%+v, want closed and empty cache", stats)
	}
}

func TestPooledAllocatorIgnoresForeignBufferRelease(t *testing.T) {
	alloc, err := NewPooledAllocator(PooledAllocatorConfig{SizeClasses: []int{64}})
	if err != nil {
		t.Fatal(err)
	}
	foreign := NewHeapBuffer(64)
	defer foreign.Release()

	alloc.Release(foreign)

	stats := alloc.PooledStats()
	if stats.Classes[0].InUse != 0 || stats.Classes[0].Cached != 0 || stats.Oversized != 0 {
		t.Fatalf("stats=%+v, want unchanged for foreign buffer", stats)
	}
}

func TestPooledAllocatorValidatesSizeClasses(t *testing.T) {
	if _, err := NewPooledAllocator(PooledAllocatorConfig{SizeClasses: []int{128, 64}}); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("err=%v, want %v", err, ErrInvalidSize)
	}
	if _, err := NewPooledAllocator(PooledAllocatorConfig{MinSize: 1024, MaxSize: 128}); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("err=%v, want %v", err, ErrInvalidSize)
	}
}
