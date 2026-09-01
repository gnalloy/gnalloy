package channel

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

type concurrentWriteProbe struct {
	active    atomic.Int32
	maxActive atomic.Int32
	writes    atomic.Int32
}

type blockingWriteProbe struct {
	entered chan struct{}
	release chan struct{}
	closed  atomic.Int32
}

func (p *blockingWriteProbe) Read(transport.FDRef, []byte) (int, bool, error) {
	return 0, true, nil
}

func (p *blockingWriteProbe) Write(_ transport.FDRef, src []byte) (int, bool, error) {
	close(p.entered)
	<-p.release
	return len(src), false, nil
}

func (p *blockingWriteProbe) Close(transport.FDRef) error {
	p.closed.Add(1)
	return nil
}

func (p *concurrentWriteProbe) Read(transport.FDRef, []byte) (int, bool, error) {
	return 0, true, nil
}

func (p *concurrentWriteProbe) Write(_ transport.FDRef, src []byte) (int, bool, error) {
	active := p.active.Add(1)
	for {
		maximum := p.maxActive.Load()
		if active <= maximum || p.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	time.Sleep(100 * time.Microsecond)
	p.writes.Add(1)
	p.active.Add(-1)
	return len(src), false, nil
}

func (p *concurrentWriteProbe) Close(transport.FDRef) error {
	return nil
}

type passthroughOutboundHandler struct{}

func (passthroughOutboundHandler) Write(ctx *HandlerContext, msg any) error {
	return ctx.Write(msg)
}

type closeOnFlushCompleteHandler struct {
	err error
}

func (h *closeOnFlushCompleteHandler) FlushComplete(ctx *HandlerContext) {
	h.err = ctx.Close()
}

func newBoundUnsafeChannel(
	t testing.TB,
	poller transport.Poller,
	rw FDReadWriter,
) (*LocalChannel, *Unsafe, *recordingOwnerExecutor) {
	t.Helper()
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         poller,
		ReadWriter:     rw,
		ReadBufferSize: 64,
	})
	executor := &recordingOwnerExecutor{}
	unsafeCh.BindEventExecutor(executor)
	return ch, unsafeCh, executor
}

func TestLocalChannelWriteAndFlushUsesConcurrentReadinessSink(t *testing.T) {
	rw := &fullWriteRW{}
	ch, _, executor := newBoundUnsafeChannel(t, &fakeReadyPoller{}, rw)
	buf := buffer.NewSharedBuffer([]byte("payload"))

	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}

	if len(executor.tasks) != 0 {
		t.Fatalf("owner tasks=%d, want 0", len(executor.tasks))
	}
	if rw.writes != 1 {
		t.Fatalf("writes=%d, want 1", rw.writes)
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want released", buf.RefCnt())
	}
}

func TestLocalChannelConcurrentWriteFallsBackForOutboundHandler(t *testing.T) {
	rw := &fullWriteRW{}
	ch, _, executor := newBoundUnsafeChannel(t, &fakeReadyPoller{}, rw)
	if err := ch.Pipeline().AddLast("outbound", passthroughOutboundHandler{}); err != nil {
		t.Fatal(err)
	}
	buf := buffer.NewSharedBuffer([]byte("payload"))

	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}

	if len(executor.tasks) != 1 {
		t.Fatalf("owner tasks=%d, want 1", len(executor.tasks))
	}
	if rw.writes != 0 {
		t.Fatalf("writes=%d before owner drain, want 0", rw.writes)
	}
	executor.drain()
	if rw.writes != 1 || buf.RefCnt() != 0 {
		t.Fatalf("writes=%d ref=%d, want 1/0", rw.writes, buf.RefCnt())
	}
}

func TestLocalChannelConcurrentWriteFallsBackForCompletionPoller(t *testing.T) {
	poller := &fakeCompletionPoller{}
	ch, _, executor := newBoundUnsafeChannel(t, poller, &fullWriteRW{})
	buf := buffer.NewSharedBuffer([]byte("payload"))

	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}

	if len(executor.tasks) != 1 {
		t.Fatalf("owner tasks=%d, want 1", len(executor.tasks))
	}
	executor.drain()
	if len(poller.submitted) != 1 || poller.submitted[0].Op != transport.OpWrite {
		t.Fatalf("submitted=%v, want one completion write", poller.submitted)
	}
	for i := range poller.submitted {
		poller.submitted[i].ReleaseBuffers()
	}
	buf.Release()
}

func TestUnsafeWriteAllowsReentrantCloseFromFlushCompleteHandler(t *testing.T) {
	rw := &fullWriteRW{}
	ch, unsafeCh, _ := newBoundUnsafeChannel(t, &fakeReadyPoller{}, rw)
	handler := &closeOnFlushCompleteHandler{}
	if err := ch.Pipeline().AddLast("flush-close", handler); err != nil {
		t.Fatal(err)
	}
	buf := buffer.NewSharedBuffer([]byte("payload"))

	if err := unsafeCh.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}

	if handler.err != nil {
		t.Fatal(handler.err)
	}
	if !unsafeCh.closed.Load() {
		t.Fatal("channel should close from flush-complete callback")
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want released", buf.RefCnt())
	}
}

func TestLocalChannelConcurrentReadinessWritesSerializeSyscalls(t *testing.T) {
	rw := &concurrentWriteProbe{}
	ch, _, executor := newBoundUnsafeChannel(t, &fakeReadyPoller{}, rw)
	const writers = 16
	var wg sync.WaitGroup
	wg.Add(writers)
	for range writers {
		go func() {
			defer wg.Done()
			buf := buffer.NewSharedBuffer([]byte("payload"))
			if err := ch.WriteAndFlush(buf); err != nil {
				t.Errorf("write: %v", err)
			}
		}()
	}
	wg.Wait()

	if len(executor.tasks) != 0 {
		t.Fatalf("owner tasks=%d, want 0", len(executor.tasks))
	}
	if got := rw.writes.Load(); got != writers {
		t.Fatalf("writes=%d, want %d", got, writers)
	}
	if got := rw.maxActive.Load(); got != 1 {
		t.Fatalf("concurrent syscalls=%d, want 1", got)
	}
}

func TestLocalChannelConcurrentReadinessWritePreservesQueuedOrder(t *testing.T) {
	poller := &fakeReadyPoller{}
	rw := &partialWriteRW{steps: []writeStep{
		{n: 0, again: true},
		{n: 0, again: true},
		{n: 2},
		{n: 2},
	}}
	ch, unsafeCh, executor := newBoundUnsafeChannel(t, poller, rw)
	first := buffer.NewSharedBuffer([]byte("ab"))
	second := buffer.NewSharedBuffer([]byte("cd"))

	if err := ch.WriteAndFlush(first); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(second); err != nil {
		t.Fatal(err)
	}
	if len(executor.tasks) != 0 {
		t.Fatalf("owner tasks=%d, want 0", len(executor.tasks))
	}

	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if got := rw.writes; len(got) != 4 || got[0] != "ab" || got[1] != "ab" || got[2] != "ab" || got[3] != "cd" {
		t.Fatalf("writes=%v, want queued FIFO", got)
	}
	if first.RefCnt() != 0 || second.RefCnt() != 0 || ch.PendingOutboundBytes() != 0 {
		t.Fatalf("refs=%d/%d pending=%d, want drained", first.RefCnt(), second.RefCnt(), ch.PendingOutboundBytes())
	}
}

func TestUnsafeCloseWaitsForConcurrentReadinessWrite(t *testing.T) {
	rw := &blockingWriteProbe{entered: make(chan struct{}), release: make(chan struct{})}
	ch, unsafeCh, _ := newBoundUnsafeChannel(t, &fakeReadyPoller{}, rw)
	buf := buffer.NewSharedBuffer([]byte("payload"))
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- ch.WriteAndFlush(buf)
	}()
	<-rw.entered

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- unsafeCh.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("close completed during write: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	close(rw.release)
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want released", buf.RefCnt())
	}
	if got := rw.closed.Load(); got != 1 {
		t.Fatalf("close calls=%d, want 1", got)
	}

	rejected := buffer.NewSharedBuffer([]byte("closed"))
	if err := ch.WriteAndFlush(rejected); err == nil {
		t.Fatal("write after close should fail")
	}
	if rejected.RefCnt() != 0 {
		t.Fatalf("rejected ref=%d, want released", rejected.RefCnt())
	}
}

func BenchmarkLocalChannelConcurrentReadinessWriteAndFlush(b *testing.B) {
	rw := &fullWriteRW{}
	ch, _, executor := newBoundUnsafeChannel(b, &fakeReadyPoller{}, rw)
	payload := buffer.NewSharedBuffer([]byte("benchmark"))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		payload.Retain()
		if err := ch.WriteAndFlush(payload); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	payload.Release()
	if len(executor.tasks) != 0 {
		b.Fatalf("owner tasks=%d, want 0", len(executor.tasks))
	}
}
