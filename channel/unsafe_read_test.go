package channel

import (
	"strings"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

func TestUnsafeReadinessStopsAfterShortRead(t *testing.T) {
	rw := &scriptedReadRW{steps: []readStep{
		{data: "ab"},
		{data: "cd", again: true},
	}}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         &fakeReadyPoller{},
		ReadWriter:     rw,
		ReadBufferSize: 4,
	})
	reader := &releaseReadHandler{}
	if err := ch.Pipeline().AddLast("reader", reader); err != nil {
		t.Fatal(err)
	}

	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if rw.reads != 1 || reader.reads != 1 {
		t.Fatalf("reads=%d handler=%d, want one short read", rw.reads, reader.reads)
	}

	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if rw.reads != 2 || reader.reads != 2 {
		t.Fatalf("reads=%d handler=%d, want second read on next readiness cycle", rw.reads, reader.reads)
	}
}

func TestUnsafeReadinessStopsAfterTinyShortRead(t *testing.T) {
	rw := &scriptedReadRW{steps: []readStep{
		{data: "x"},
		{again: true},
	}}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         &fakeReadyPoller{},
		ReadWriter:     rw,
		ReadBufferSize: 16,
	})
	reader := &releaseReadHandler{}
	if err := ch.Pipeline().AddLast("reader", reader); err != nil {
		t.Fatal(err)
	}

	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if rw.reads != 1 || reader.reads != 1 {
		t.Fatalf("reads=%d handler=%d, want tiny short read without EAGAIN probe", rw.reads, reader.reads)
	}
}

func TestUnsafeReadinessContinuesAfterFullRead(t *testing.T) {
	rw := &scriptedReadRW{steps: []readStep{
		{data: "abcd"},
		{data: "ef"},
		{again: true},
	}}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         &fakeReadyPoller{},
		ReadWriter:     rw,
		ReadBufferSize: 4,
	})
	reader := &releaseReadHandler{}
	if err := ch.Pipeline().AddLast("reader", reader); err != nil {
		t.Fatal(err)
	}

	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if rw.reads != 2 || reader.reads != 2 {
		t.Fatalf("reads=%d handler=%d, want full read followed by short read", rw.reads, reader.reads)
	}
}

func TestShouldStopAfterShortRead(t *testing.T) {
	tests := []struct {
		name      string
		n         int
		attempted int
		want      bool
	}{
		{name: "meaningful default buffer short read", n: 1024, attempted: defaultReadBufferSize, want: true},
		{name: "tiny short read", n: 64, attempted: defaultReadBufferSize, want: true},
		{name: "full read", n: defaultReadBufferSize, attempted: defaultReadBufferSize, want: false},
		{name: "large buffer short read", n: defaultReadBufferSize, attempted: defaultReadBufferSize * 4, want: true},
		{name: "empty read", n: 0, attempted: defaultReadBufferSize, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldStopAfterShortRead(tt.n, tt.attempted); got != tt.want {
				t.Fatalf("shouldStopAfterShortRead(%d, %d)=%t, want %t", tt.n, tt.attempted, got, tt.want)
			}
		})
	}
}

func TestReadBufferSizerAdaptsWithinBounds(t *testing.T) {
	sizer := newReadBufferSizer(defaultReadBufferSize)
	if got := sizer.nextSize(); got != defaultInitialReadBufferSize {
		t.Fatalf("initial size=%d, want %d", got, defaultInitialReadBufferSize)
	}

	sizer.record(defaultInitialReadBufferSize, defaultInitialReadBufferSize)
	if got := sizer.nextSize(); got != defaultInitialReadBufferSize*2 {
		t.Fatalf("grown size=%d, want %d", got, defaultInitialReadBufferSize*2)
	}

	sizer.record(64, defaultInitialReadBufferSize*2)
	if got := sizer.nextSize(); got != defaultInitialReadBufferSize {
		t.Fatalf("shrunk size=%d, want %d", got, defaultInitialReadBufferSize)
	}
}

func TestReadBufferSizerHonorsSmallExplicitLimit(t *testing.T) {
	sizer := newReadBufferSizer(128)
	if got := sizer.nextSize(); got != 128 {
		t.Fatalf("initial size=%d, want explicit limit", got)
	}
	sizer.record(128, 128)
	if got := sizer.nextSize(); got != 128 {
		t.Fatalf("grown size=%d, want explicit limit", got)
	}
}

func TestUnsafeReadinessUsesAdaptiveReadBufferSize(t *testing.T) {
	rw := &scriptedReadRW{steps: []readStep{
		{data: strings.Repeat("a", defaultInitialReadBufferSize)},
		{data: "b"},
	}}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         &fakeReadyPoller{},
		ReadWriter:     rw,
		ReadBufferSize: defaultReadBufferSize,
	})
	reader := &releaseReadHandler{}
	if err := ch.Pipeline().AddLast("reader", reader); err != nil {
		t.Fatal(err)
	}

	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if len(rw.attempts) != 2 {
		t.Fatalf("attempts=%v, want two reads", rw.attempts)
	}
	if rw.attempts[0] != defaultInitialReadBufferSize || rw.attempts[1] != defaultInitialReadBufferSize*2 {
		t.Fatalf("attempts=%v, want adaptive growth", rw.attempts)
	}
}

func TestUnsafeReadBufferSizeOptionResetsSizer(t *testing.T) {
	rw := &scriptedReadRW{steps: []readStep{{data: "a"}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         &fakeReadyPoller{},
		ReadWriter:     rw,
		ReadBufferSize: defaultReadBufferSize,
	})
	OptionReadBufferSize.Set(ch.Options(), 128)
	if got := unsafeCh.nextReadBufferSize(); got != 128 {
		t.Fatalf("next read size=%d, want 128", got)
	}
}
