//go:build linux

package buffer

import (
	"errors"
	"testing"
	"unsafe"
)

func TestMmapAllocatorRejectsInvalidAndOverflowSizes(t *testing.T) {
	if _, err := NewMmapAllocator(MmapAllocatorConfig{}); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("err=%v, want %v", err, ErrInvalidSize)
	}
	if _, err := NewMmapAllocator(MmapAllocatorConfig{BlockSize: maxInt, Blocks: 2}); !errors.Is(err, ErrInvalidSize) {
		t.Fatalf("overflow err=%v, want %v", err, ErrInvalidSize)
	}
}

func TestMmapAllocatorCloseRejectsInUseBuffers(t *testing.T) {
	alloc, err := NewMmapAllocator(MmapAllocatorConfig{BlockSize: 1024, Blocks: 1})
	if err != nil {
		t.Fatal(err)
	}
	buf, err := alloc.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	if err := alloc.Close(); !errors.Is(err, ErrAllocatorInUse) {
		t.Fatalf("close err=%v, want %v", err, ErrAllocatorInUse)
	}
	buf.Release()
	if err := alloc.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := alloc.Acquire(128); !errors.Is(err, ErrAllocatorClosed) {
		t.Fatalf("acquire err=%v, want %v", err, ErrAllocatorClosed)
	}
}

func TestMmapAllocatorStatsTracksInUseAndClosed(t *testing.T) {
	alloc, err := NewMmapAllocator(MmapAllocatorConfig{BlockSize: 1024, Blocks: 2})
	if err != nil {
		t.Fatal(err)
	}
	statsAlloc := alloc.(StatAllocator)
	if got := statsAlloc.Stats(); got.BlockSize != 1024 || got.Blocks != 2 || got.Free != 2 || got.InUse != 0 || !got.OffHeap {
		t.Fatalf("initial stats=%+v", got)
	}
	buf, err := alloc.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	if got := statsAlloc.Stats(); got.Free != 1 || got.InUse != 1 || got.Closed {
		t.Fatalf("acquired stats=%+v", got)
	}
	buf.Release()
	if got := statsAlloc.Stats(); got.Free != 2 || got.InUse != 0 {
		t.Fatalf("released stats=%+v", got)
	}
	if err := alloc.Close(); err != nil {
		t.Fatal(err)
	}
	if got := statsAlloc.Stats(); !got.Closed || got.Blocks != 0 || got.Free != 0 {
		t.Fatalf("closed stats=%+v", got)
	}
}

func TestMmapAllocatorExposesFixedBuffers(t *testing.T) {
	alloc, err := NewMmapAllocator(MmapAllocatorConfig{BlockSize: 1024, Blocks: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer alloc.Close()

	provider := alloc.(FixedBufferProvider)
	fixed := provider.FixedBuffers()
	if len(fixed) != 2 || len(fixed[0]) != 1024 || len(fixed[1]) != 1024 {
		t.Fatalf("fixed buffers=%d/%d/%d", len(fixed), len(fixed[0]), len(fixed[1]))
	}
	buf, err := alloc.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	idx, ok := FixedBufferIndex(buf)
	if !ok || idx > 1 {
		t.Fatalf("fixed index=(%d,%v), want block index", idx, ok)
	}
	slice, err := buf.Slice(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sliceIdx, sliceOK := FixedBufferIndex(slice)
	if !sliceOK || sliceIdx != idx {
		t.Fatalf("slice fixed index=(%d,%v), want (%d,true)", sliceIdx, sliceOK, idx)
	}
	slice.Release()
	buf.Release()
}

func TestMmapAllocatorFixedBufferPointerStable(t *testing.T) {
	alloc, err := NewMmapAllocator(MmapAllocatorConfig{BlockSize: 1024, Blocks: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer alloc.Close()

	fixed := alloc.(FixedBufferProvider).FixedBuffers()
	buf, err := alloc.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()
	idx, ok := FixedBufferIndex(buf)
	if !ok {
		t.Fatalf("fixed index not available")
	}
	view := buf.WritableBytesView()
	if len(view) == 0 {
		t.Fatalf("writable view is empty")
	}
	got := uintptr(unsafe.Pointer(&view[0]))
	want := uintptr(unsafe.Pointer(&fixed[idx][0]))
	if got != want {
		t.Fatalf("buffer pointer=%#x, fixed pointer=%#x", got, want)
	}
}

func TestMmapAllocatorAcquireReleaseAllocationBudget(t *testing.T) {
	alloc, err := NewMmapAllocator(MmapAllocatorConfig{BlockSize: 4096, Blocks: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer alloc.Close()
	warm, err := alloc.Acquire(4096)
	if err != nil {
		t.Fatal(err)
	}
	warm.Release()

	var runErr error
	allocs := testing.AllocsPerRun(1000, func() {
		buf, err := alloc.Acquire(4096)
		if err != nil {
			runErr = err
			return
		}
		buf.Release()
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf("allocs/run=%f, want 0", allocs)
	}
}

func TestMmapAllocatorDirectDoubleReleaseDoesNotCorruptFreeList(t *testing.T) {
	alloc, err := NewMmapAllocator(MmapAllocatorConfig{BlockSize: 1024, Blocks: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer alloc.Close()

	buf, err := alloc.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	direct := buf.(*DirectByteBuf)
	alloc.Release(direct)
	alloc.Release(direct)

	first, err := alloc.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := alloc.Acquire(128); !errors.Is(err, ErrAllocatorExhausted) {
		t.Fatalf("second acquire err=%v, want %v", err, ErrAllocatorExhausted)
	}
	first.Release()
}
