package channel

import (
	"bytes"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

type benchVectorWriteRW struct {
	scalar int
	writev int
}

type benchFileRegionWriter struct {
	calls int
	bytes int64
}

func (rw *benchVectorWriteRW) Read(transport.FDRef, []byte) (int, bool, error) {
	return 0, true, nil
}

func (rw *benchVectorWriteRW) Write(_ transport.FDRef, src []byte) (int, bool, error) {
	rw.scalar++
	return len(src), false, nil
}

func (rw *benchVectorWriteRW) Writev(_ transport.FDRef, src [][]byte) (int, bool, error) {
	rw.writev++
	total := 0
	for _, part := range src {
		total += len(part)
	}
	return total, false, nil
}

func (rw *benchVectorWriteRW) Close(transport.FDRef) error {
	return nil
}

func (w *benchFileRegionWriter) WriteFileRegion(_ transport.FDRef, region FileRegion) (int64, bool, error) {
	n := region.Count() - region.Transferred()
	if n > 0 {
		if err := advanceFileRegion(region, n); err != nil {
			return 0, false, err
		}
		w.calls++
		w.bytes += n
	}
	return n, false, nil
}

func BenchmarkUnsafeWriteAndFlushDrained(b *testing.B) {
	alloc := buffer.NewHeapAllocator()
	rw := &fullWriteRW{}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  alloc,
		Poller:     &fakeReadyPoller{},
		ReadWriter: rw,
	})
	payload := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, err := alloc.Acquire(len(payload))
		if err != nil {
			b.Fatal(err)
		}
		payload[0] = byte(i)
		if _, err := buf.WriteBytes(payload); err != nil {
			b.Fatal(err)
		}
		if err := ch.WriteAndFlush(buf); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if pending := ch.PendingOutboundBytes(); pending != 0 {
		b.Fatalf("pending outbound bytes=%d, want 0", pending)
	}
	if rw.writes != b.N {
		b.Fatalf("writes=%d, want %d", rw.writes, b.N)
	}
}

func BenchmarkUnsafeEventLoopBatchFlushDrained(b *testing.B) {
	rw := &benchVectorWriteRW{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: rw,
	})
	scheduler := &recordingTailSubmitter{}
	unsafeCh.BindEventExecutor(scheduler)
	OptionFlushStrategy.Set(ch.Options(), FlushOnEventLoopBatch)
	payload := buffer.NewSharedBuffer([]byte("benchmark"))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload.Retain()
		if err := unsafeCh.WriteAndFlush(payload); err != nil {
			b.Fatal(err)
		}
		scheduler.drain()
	}
	b.StopTimer()
	payload.Release()
}

func BenchmarkUnsafeFileRegionDirectWriterDrained(b *testing.B) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 4096)
	writer := &benchFileRegionWriter{}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:               1,
		FD:               transport.FDRef{FD: 1},
		Allocator:        buffer.NewHeapAllocator(),
		Poller:           &fakeReadyPoller{},
		FileRegionWriter: writer,
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region, err := NewFileRegion(bytes.NewReader(payload), 0, int64(len(payload)))
		if err != nil {
			b.Fatal(err)
		}
		if err := ch.WriteAndFlush(region); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	want := int64(len(payload)) * int64(b.N)
	if writer.bytes != want || writer.calls != b.N {
		b.Fatalf("bytes=%d calls=%d, want %d/%d", writer.bytes, writer.calls, want, b.N)
	}
	if pending := ch.PendingOutboundBytes(); pending != 0 {
		b.Fatalf("pending outbound bytes=%d, want 0", pending)
	}
}

func BenchmarkUnsafeVectorWriteAndFlushSingleDirectBufferDrained(b *testing.B) {
	alloc := buffer.NewHeapAllocator()
	rw := &benchVectorWriteRW{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  alloc,
		Poller:     &fakeReadyPoller{},
		ReadWriter: rw,
	})
	payload := make([]byte, 64)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, err := alloc.Acquire(len(payload))
		if err != nil {
			b.Fatal(err)
		}
		payload[0] = byte(i)
		if _, err := buf.WriteBytes(payload); err != nil {
			b.Fatal(err)
		}
		if err := ch.WriteAndFlush(buf); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if pending := ch.PendingOutboundBytes(); pending != 0 {
		b.Fatalf("pending outbound bytes=%d, want 0", pending)
	}
	if rw.scalar != b.N || rw.writev != 0 {
		b.Fatalf("scalar=%d writev=%d, want %d/0", rw.scalar, rw.writev, b.N)
	}
	if len(unsafeCh.writeSlices) != 0 {
		b.Fatalf("write slices=%d, want 0", len(unsafeCh.writeSlices))
	}
}
