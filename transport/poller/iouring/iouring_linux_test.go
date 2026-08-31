//go:build linux

package iouring

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/unix"
)

var _ poller.BatchSubmitter = (*Poller)(nil)

func TestNewWithConfigRejectsInvalidSQPollAffinity(t *testing.T) {
	_, err := NewWithConfig(Config{SQPollAffinity: true})
	if !errors.Is(err, poller.ErrInvalidIORequest) {
		t.Fatalf("err=%v, want %v", err, poller.ErrInvalidIORequest)
	}
}

func TestStatsReflectsMultishotAccept(t *testing.T) {
	raw, err := NewWithConfig(Config{Entries: 8, MultishotAccept: true})
	if err != nil {
		t.Skip(err)
	}
	p := raw.(*Poller)
	defer p.Close()

	stats := p.Stats()
	if !stats.MultishotAccept {
		t.Fatalf("stats=%+v, want multishot accept", stats)
	}
	if stats.SQAvailable == 0 {
		t.Fatalf("stats=%+v, want available SQ entries", stats)
	}
}

func TestRegisterBuffersStatsAndUnregister(t *testing.T) {
	raw, err := NewWithConfig(Config{Entries: 8})
	if err != nil {
		t.Skip(err)
	}
	p := raw.(*Poller)
	defer p.Close()

	if err := p.RegisterBuffers(nil); !errors.Is(err, poller.ErrInvalidIORequest) {
		t.Fatalf("empty register err=%v, want %v", err, poller.ErrInvalidIORequest)
	}
	mem := make([]byte, 4096)
	if err := p.RegisterBuffers([][]byte{mem}); err != nil {
		t.Skip(err)
	}
	if stats := p.Stats(); !stats.RegisteredBuffers {
		t.Fatalf("stats=%+v, want registered buffers", stats)
	}
	if err := p.UnregisterBuffers(); err != nil {
		t.Fatal(err)
	}
	if stats := p.Stats(); stats.RegisteredBuffers {
		t.Fatalf("stats=%+v, want no registered buffers", stats)
	}
}

func TestRegisterMmapAllocatorFixedBuffers(t *testing.T) {
	raw, err := NewWithConfig(Config{Entries: 8})
	if err != nil {
		t.Skip(err)
	}
	p := raw.(*Poller)
	defer p.Close()

	alloc, err := buffer.NewMmapAllocator(buffer.MmapAllocatorConfig{BlockSize: 4096, Blocks: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer alloc.Close()

	fixed := alloc.(buffer.FixedBufferProvider).FixedBuffers()
	if err := p.RegisterBuffers(fixed); err != nil {
		t.Skip(err)
	}
	if stats := p.Stats(); !stats.RegisteredBuffers {
		t.Fatalf("stats=%+v, want registered buffers", stats)
	}
	buf, err := alloc.Acquire(128)
	if err != nil {
		t.Fatal(err)
	}
	idx, ok := buffer.FixedBufferIndex(buf)
	if !ok || int(idx) >= len(fixed) {
		t.Fatalf("fixed index=(%d,%v), fixed buffers=%d", idx, ok, len(fixed))
	}
	buf.Release()
	if err := p.UnregisterBuffers(); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitEnterFlagsWakeSQPollThread(t *testing.T) {
	var flags uint32
	p := &Poller{sq: sq{flags: &flags}}
	if got := p.submitEnterFlags(); got != 0 {
		t.Fatalf("flags=%d, want 0", got)
	}

	flags = sqNeedWakeup
	if got := p.submitEnterFlags(); got != enterSQWakeup {
		t.Fatalf("flags=%d, want %d", got, enterSQWakeup)
	}
}

func TestWakeupConcurrent(t *testing.T) {
	raw, err := NewWithConfig(Config{Entries: 1024})
	if err != nil {
		t.Skip(err)
	}
	p := raw.(*Poller)
	defer p.Close()

	const (
		goroutines        = 8
		wakeupsPerRoutine = 64
	)
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range wakeupsPerRoutine {
				if err := p.Wakeup(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	events := make([]poller.Event, 16)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		n, err := p.Poll(events, 10)
		if err != nil {
			t.Fatal(err)
		}
		for i := range n {
			if events[i].Op == poller.OpWakeup {
				return
			}
		}
	}
	t.Fatal("未收到 io_uring 唤醒事件")
}

func TestSubmitBatchRejectsDuplicateOperationID(t *testing.T) {
	raw, err := NewWithConfig(Config{Entries: 8})
	if err != nil {
		t.Skip(err)
	}
	p := raw.(*Poller)
	defer p.Close()

	first := buffer.NewHeapBuffer(8)
	defer first.Release()
	if _, err := first.WriteBytes([]byte("a")); err != nil {
		t.Fatal(err)
	}
	second := buffer.NewHeapBuffer(8)
	defer second.Release()
	if _, err := second.WriteBytes([]byte("b")); err != nil {
		t.Fatal(err)
	}

	err = p.SubmitBatch([]poller.IORequest{
		{Op: poller.OpWrite, FD: poller.FDRef{FD: 1}, OpID: 7, Buf: first},
		{Op: poller.OpWrite, FD: poller.FDRef{FD: 2}, OpID: 7, Buf: second},
	})
	if !errors.Is(err, poller.ErrInvalidIORequest) {
		t.Fatalf("err=%v, want %v", err, poller.ErrInvalidIORequest)
	}
	if first.RefCnt() != 1 || second.RefCnt() != 1 {
		t.Fatalf("refs=%d,%d, want 1,1", first.RefCnt(), second.RefCnt())
	}
	if stats := p.Stats(); stats.Pending != 0 {
		t.Fatalf("pending=%d, want 0", stats.Pending)
	}
}

func TestSubmitBatchRollsBackRetainedBuffersOnPrepareError(t *testing.T) {
	raw, err := NewWithConfig(Config{Entries: 8})
	if err != nil {
		t.Skip(err)
	}
	p := raw.(*Poller)
	defer p.Close()

	okBuf := buffer.NewHeapBuffer(8)
	defer okBuf.Release()
	if _, err := okBuf.WriteBytes([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	emptyBuf := buffer.NewHeapBuffer(8)
	defer emptyBuf.Release()

	err = p.SubmitBatch([]poller.IORequest{
		{Op: poller.OpWrite, FD: poller.FDRef{FD: 1}, Buf: okBuf},
		{Op: poller.OpWrite, FD: poller.FDRef{FD: 2}, Buf: emptyBuf},
	})
	if !errors.Is(err, poller.ErrInvalidIORequest) {
		t.Fatalf("err=%v, want %v", err, poller.ErrInvalidIORequest)
	}
	if okBuf.RefCnt() != 1 || emptyBuf.RefCnt() != 1 {
		t.Fatalf("refs=%d,%d, want 1,1", okBuf.RefCnt(), emptyBuf.RefCnt())
	}
	if stats := p.Stats(); stats.Pending != 0 {
		t.Fatalf("pending=%d, want 0", stats.Pending)
	}
}

func TestSubmitBatchCompletesPipeReadAndWrite(t *testing.T) {
	raw, err := NewWithConfig(Config{Entries: 8})
	if err != nil {
		t.Skip(err)
	}
	p := raw.(*Poller)
	defer p.Close()

	var fds [2]int
	if err := unix.Pipe2(fds[:], unix.O_NONBLOCK|unix.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fds[0])
	defer unix.Close(fds[1])

	readBuf := buffer.NewHeapBuffer(32)
	defer readBuf.Release()
	writeBuf := buffer.NewHeapBuffer(32)
	defer writeBuf.Release()
	payload := []byte("gnalloy-batch")
	if _, err := writeBuf.WriteBytes(payload); err != nil {
		t.Fatal(err)
	}

	err = p.SubmitBatch([]poller.IORequest{
		{Op: poller.OpRead, FD: poller.FDRef{FD: fds[0]}, ChannelID: 1, Buf: readBuf},
		{Op: poller.OpWrite, FD: poller.FDRef{FD: fds[1]}, ChannelID: 2, Buf: writeBuf},
	})
	if err != nil {
		t.Fatal(err)
	}

	events := make([]poller.Event, 4)
	var sawRead, sawWrite bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (!sawRead || !sawWrite) {
		n, err := p.Poll(events, 20)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			ev := events[i]
			if ev.Err != nil {
				t.Fatalf("event=%+v", ev)
			}
			switch ev.Op {
			case poller.OpRead:
				sawRead = true
				if ev.N != len(payload) || !bytes.Equal(ev.Buf.Bytes(), payload) {
					t.Fatalf("read n=%d bytes=%q, want %q", ev.N, ev.Buf.Bytes(), payload)
				}
				ev.Buf.Release()
			case poller.OpWrite:
				sawWrite = true
				if ev.N != len(payload) {
					t.Fatalf("write n=%d, want %d", ev.N, len(payload))
				}
				ev.Buf.Release()
			default:
				t.Fatalf("unexpected event=%+v", ev)
			}
		}
	}
	if !sawRead || !sawWrite {
		t.Fatalf("sawRead=%v sawWrite=%v", sawRead, sawWrite)
	}
	if readBuf.RefCnt() != 1 || writeBuf.RefCnt() != 1 {
		t.Fatalf("refs=%d,%d, want 1,1", readBuf.RefCnt(), writeBuf.RefCnt())
	}
}

func TestMakeIOVectorsExpandsCompositeWithoutCopy(t *testing.T) {
	buf := fragmentedWriteBuffer("ab", "cd")
	defer buf.Release()
	var inline [inlineWriteVectors]iovec
	vectors, err := makeIOVectors(poller.IORequest{Buf: buf}, inline[:0])
	if err != nil {
		t.Fatal(err)
	}
	if len(vectors) != 2 || vectors[0].len != 2 || vectors[1].len != 2 {
		t.Fatalf("vectors=%+v, want two 2-byte vectors", vectors)
	}
	if &vectors[0] != &inline[0] {
		t.Fatal("复合 ByteBuf 小片段应复用调用方内联 iovec 存储")
	}
}

func TestWriteVectorContextReusesInlineStorage(t *testing.T) {
	buf := fragmentedWriteBuffer("ab", "cd")
	defer buf.Release()

	p := &Poller{}
	ctx, err := p.acquireWriteVectorContext(poller.IORequest{Buf: buf})
	if err != nil {
		t.Fatal(err)
	}
	if len(ctx.vectors) != 2 || &ctx.vectors[0] != &ctx.inline[0] {
		t.Fatalf("vectors=%+v, want inline storage", ctx.vectors)
	}

	p.recycleWriteVectorContext(ctx)
	if p.freeWritev != ctx {
		t.Fatal("write-vector context should return to poller free list")
	}
	ctx2, err := p.acquireWriteVectorContext(poller.IORequest{Buf: buf})
	if err != nil {
		t.Fatal(err)
	}
	if ctx2 != ctx {
		t.Fatal("write-vector context should be reused")
	}
	p.recycleWriteVectorContext(ctx2)
}

func TestMsgContextUsesInlineVectors(t *testing.T) {
	readBuf := buffer.NewHeapBuffer(8)
	defer readBuf.Release()
	recv, err := makeRecvMsgContext(poller.IORequest{Buf: readBuf})
	if err != nil {
		t.Fatal(err)
	}
	if len(recv.iov) != 1 || &recv.iov[0] != &recv.inline[0] {
		t.Fatalf("recv iov=%+v, want inline storage", recv.iov)
	}

	writeBuf := fragmentedWriteBuffer("ab", "cd")
	defer writeBuf.Release()
	addr := poller.SocketAddress{Family: poller.SocketFamilyIPv4, Port: 53}
	copy(addr.IP[:4], []byte{127, 0, 0, 1})
	send, err := makeSendMsgContext(poller.IORequest{Buf: writeBuf, Addr: addr})
	if err != nil {
		t.Fatal(err)
	}
	if len(send.iov) != 2 || &send.iov[0] != &send.inline[0] {
		t.Fatalf("send iov=%+v, want inline storage", send.iov)
	}
}

func BenchmarkMakeIOVectorsComposite(b *testing.B) {
	buf := fragmentedWriteBuffer("abcd", "efgh", "ijkl", "mnop")
	defer buf.Release()
	req := poller.IORequest{Buf: buf}
	var inline [inlineWriteVectors]iovec

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vectors, err := makeIOVectors(req, inline[:0])
		if err != nil {
			b.Fatal(err)
		}
		if vectorBytes(vectors) != 16 {
			b.Fatalf("vectors=%+v", vectors)
		}
	}
}

func vectorBytes(vectors []iovec) uintptr {
	var total uintptr
	for _, vector := range vectors {
		total += vector.len
	}
	return total
}

func fragmentedWriteBuffer(parts ...string) buffer.ByteBuf {
	c := buffer.NewCompositeByteBuf()
	for _, part := range parts {
		buf := buffer.NewHeapBuffer(len(part))
		_, _ = buf.WriteBytes([]byte(part))
		c.Append(buf)
	}
	return c
}
