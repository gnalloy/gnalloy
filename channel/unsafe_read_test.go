package channel

import (
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

func TestUnsafeReadinessContinuesAfterTinyShortRead(t *testing.T) {
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
	if rw.reads != 2 || reader.reads != 1 {
		t.Fatalf("reads=%d handler=%d, want tiny read plus EAGAIN probe", rw.reads, reader.reads)
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
		{name: "tiny short read", n: 64, attempted: defaultReadBufferSize, want: false},
		{name: "full read", n: defaultReadBufferSize, attempted: defaultReadBufferSize, want: false},
		{name: "large buffer short read", n: defaultReadBufferSize, attempted: defaultReadBufferSize * 4, want: false},
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
