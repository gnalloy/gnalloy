package channel

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

type fullWriteRW struct {
	writes int
}

func (rw *fullWriteRW) Read(transport.FDRef, []byte) (int, bool, error) {
	return 0, true, nil
}

func (rw *fullWriteRW) Write(_ transport.FDRef, src []byte) (int, bool, error) {
	rw.writes++
	return len(src), false, nil
}

func (rw *fullWriteRW) Close(transport.FDRef) error {
	return nil
}

func TestUnsafeWriteAndFlushBelowWatermarkDoesNotFireWritabilityChanged(t *testing.T) {
	rw := &fullWriteRW{}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:                 1,
		FD:                 transport.FDRef{FD: 1},
		Allocator:          buffer.NewHeapAllocator(),
		Poller:             &fakeReadyPoller{},
		ReadWriter:         rw,
		WriteHighWatermark: 1024,
		WriteLowWatermark:  512,
	})
	recorder := &writabilityRecorder{}
	if err := ch.Pipeline().AddLast("writable", recorder); err != nil {
		t.Fatal(err)
	}

	buf := buffer.NewHeapBuffer(64)
	if _, err := buf.WriteBytes([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}

	if !ch.IsWritable() {
		t.Fatal("channel should stay writable below high watermark")
	}
	if recorder.changes != 0 {
		t.Fatalf("writability changes=%d, want 0", recorder.changes)
	}
	if pending := ch.PendingOutboundBytes(); pending != 0 {
		t.Fatalf("pending outbound bytes=%d, want 0", pending)
	}
	if rw.writes != 1 {
		t.Fatalf("writes=%d, want 1", rw.writes)
	}
}
