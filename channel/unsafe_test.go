package channel

import (
	"errors"
	"strings"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

type fakeReadyPoller struct {
	modified []transport.ReadyMask
}

func (p *fakeReadyPoller) Model() transport.PollerModel {
	return transport.PollerReadiness
}

func (p *fakeReadyPoller) Backend() transport.BackendKind {
	return transport.BackendMemory
}

func (p *fakeReadyPoller) Register(transport.FDRef, transport.ChannelID, transport.ReadyMask) error {
	return nil
}

func (p *fakeReadyPoller) Modify(_ transport.FDRef, interest transport.ReadyMask) error {
	p.modified = append(p.modified, interest)
	return nil
}

func (p *fakeReadyPoller) Deregister(transport.FDRef) error {
	return nil
}

func (p *fakeReadyPoller) Submit(transport.IORequest) error {
	return nil
}

func (p *fakeReadyPoller) Poll([]transport.PollEvent, int) (int, error) {
	return 0, nil
}

func (p *fakeReadyPoller) Wakeup() error {
	return nil
}

func (p *fakeReadyPoller) Close() error {
	return nil
}

type fakeCompletionPoller struct {
	submitted []transport.IORequest
	backend   transport.BackendKind
}

func (p *fakeCompletionPoller) Model() transport.PollerModel {
	return transport.PollerCompletion
}

func (p *fakeCompletionPoller) Backend() transport.BackendKind {
	if p.backend != 0 {
		return p.backend
	}
	return transport.BackendIOCP
}

func (p *fakeCompletionPoller) Register(transport.FDRef, transport.ChannelID, transport.ReadyMask) error {
	return nil
}

func (p *fakeCompletionPoller) Modify(transport.FDRef, transport.ReadyMask) error {
	return nil
}

func (p *fakeCompletionPoller) Deregister(transport.FDRef) error {
	return nil
}

func (p *fakeCompletionPoller) Submit(req transport.IORequest) error {
	req.RetainBuffers()
	p.submitted = append(p.submitted, req)
	return nil
}

func (p *fakeCompletionPoller) Poll([]transport.PollEvent, int) (int, error) {
	return 0, nil
}

func (p *fakeCompletionPoller) Wakeup() error {
	return nil
}

func (p *fakeCompletionPoller) Close() error {
	return nil
}

type fakeBatchCompletionPoller struct {
	fakeCompletionPoller
	batches [][]transport.IORequest
}

func (p *fakeBatchCompletionPoller) Backend() transport.BackendKind {
	return transport.BackendIOUring
}

func (p *fakeBatchCompletionPoller) SubmitBatch(reqs []transport.IORequest) error {
	copied := make([]transport.IORequest, len(reqs))
	for i := range reqs {
		reqs[i].RetainBuffers()
		copied[i] = reqs[i]
	}
	p.batches = append(p.batches, copied)
	return nil
}

type writeStep struct {
	n     int
	again bool
}

type fileRegionWriteStep struct {
	n     int64
	again bool
	err   error
}

type partialWriteRW struct {
	steps  []writeStep
	writes []string
}

func (rw *partialWriteRW) Read(transport.FDRef, []byte) (int, bool, error) {
	return 0, true, nil
}

func (rw *partialWriteRW) Write(_ transport.FDRef, src []byte) (int, bool, error) {
	rw.writes = append(rw.writes, string(src))
	step := rw.steps[0]
	rw.steps = rw.steps[1:]
	return step.n, step.again, nil
}

func (rw *partialWriteRW) Close(transport.FDRef) error {
	return nil
}

type readStep struct {
	data  string
	again bool
}

type scriptedReadRW struct {
	steps    []readStep
	reads    int
	attempts []int
}

func (rw *scriptedReadRW) Read(_ transport.FDRef, dst []byte) (int, bool, error) {
	rw.reads++
	rw.attempts = append(rw.attempts, len(dst))
	if len(rw.steps) == 0 {
		return 0, true, nil
	}
	step := rw.steps[0]
	rw.steps = rw.steps[1:]
	n := copy(dst, step.data)
	return n, step.again, nil
}

func (rw *scriptedReadRW) Write(transport.FDRef, []byte) (int, bool, error) {
	return 0, true, nil
}

func (rw *scriptedReadRW) Close(transport.FDRef) error {
	return nil
}

type vectorWriteRW struct {
	n      int
	again  bool
	writes [][]string
	writev int
	scalar int
}

func (rw *vectorWriteRW) Read(transport.FDRef, []byte) (int, bool, error) {
	return 0, true, nil
}

func (rw *vectorWriteRW) Write(_ transport.FDRef, src []byte) (int, bool, error) {
	rw.scalar++
	rw.writes = append(rw.writes, []string{string(src)})
	if rw.n > 0 {
		return rw.n, rw.again, nil
	}
	return len(src), rw.again, nil
}

func (rw *vectorWriteRW) Writev(_ transport.FDRef, src [][]byte) (int, bool, error) {
	rw.writev++
	parts := make([]string, 0, len(src))
	total := 0
	for _, part := range src {
		parts = append(parts, string(part))
		total += len(part)
	}
	rw.writes = append(rw.writes, parts)
	if rw.n > 0 {
		return rw.n, rw.again, nil
	}
	return total, rw.again, nil
}

func (rw *vectorWriteRW) Close(transport.FDRef) error {
	return nil
}

type scriptedReadWriteRW struct {
	vectorWriteRW
	steps []readStep
	reads int
}

func (rw *scriptedReadWriteRW) Read(_ transport.FDRef, dst []byte) (int, bool, error) {
	rw.reads++
	if len(rw.steps) == 0 {
		return 0, true, nil
	}
	step := rw.steps[0]
	rw.steps = rw.steps[1:]
	n := copy(dst, step.data)
	return n, step.again, nil
}

func (rw *scriptedReadWriteRW) Close(transport.FDRef) error {
	return nil
}

type recordingTailSubmitter struct {
	tasks []transport.Task
}

func (s *recordingTailSubmitter) Submit(task transport.Task) error {
	if task != nil {
		task()
	}
	return nil
}

func (s *recordingTailSubmitter) SubmitAfterBatch(task transport.Task) error {
	s.tasks = append(s.tasks, task)
	return nil
}

func (s *recordingTailSubmitter) drain() {
	for len(s.tasks) > 0 {
		task := s.tasks[0]
		copy(s.tasks, s.tasks[1:])
		s.tasks = s.tasks[:len(s.tasks)-1]
		task()
	}
}

type recordingFileRegionWriter struct {
	steps   []fileRegionWriteStep
	calls   int
	fd      transport.FDRef
	bytes   int64
	regions []FileRegion
}

type advancingFileRegion interface {
	Advance(int64) error
}

func (w *recordingFileRegionWriter) WriteFileRegion(fd transport.FDRef, region FileRegion) (int64, bool, error) {
	w.calls++
	w.fd = fd
	w.regions = append(w.regions, region)
	if len(w.steps) == 0 {
		n := region.Count() - region.Transferred()
		if n > 0 {
			if err := advanceFileRegion(region, n); err != nil {
				return 0, false, err
			}
			w.bytes += n
		}
		return n, false, nil
	}
	step := w.steps[0]
	w.steps = w.steps[1:]
	if step.n > 0 {
		if err := advanceFileRegion(region, step.n); err != nil {
			return 0, false, err
		}
		w.bytes += step.n
	}
	return step.n, step.again, step.err
}

func advanceFileRegion(region FileRegion, n int64) error {
	advancing, ok := region.(advancingFileRegion)
	if !ok {
		return ErrInvalidFileRegion
	}
	return advancing.Advance(n)
}

type writabilityRecorder struct {
	changes int
}

func (r *writabilityRecorder) ChannelWritabilityChanged(*HandlerContext) {
	r.changes++
}

type inactiveRecorder struct {
	count int
}

func (r *inactiveRecorder) ChannelInactive(*HandlerContext) {
	r.count++
}

type flushCompleteRecorder struct {
	count int
}

func (r *flushCompleteRecorder) FlushComplete(*HandlerContext) {
	r.count++
}

type releaseReadHandler struct {
	reads      int
	closeAfter bool
}

type echoReadHandler struct {
	writes int
	err    error
}

type readFlushProbe struct {
	rw               *scriptedReadWriteRW
	writesInRead     int
	writesInComplete int
	err              error
}

type fixedTestBuf struct {
	buffer.ByteBuf
	idx uint16
}

func (b fixedTestBuf) FixedBufferIndex() (uint16, bool) {
	return b.idx, true
}

func (h *releaseReadHandler) ChannelRead(ctx *HandlerContext, msg any) {
	h.reads++
	if buf, ok := msg.(buffer.ByteBuf); ok {
		buf.Release()
	}
	if h.closeAfter {
		_ = ctx.Pipeline().Close()
	}
}

func (h *echoReadHandler) ChannelRead(ctx *HandlerContext, msg any) {
	h.writes++
	h.err = ctx.WriteAndFlush(msg)
}

func (h *readFlushProbe) ChannelRead(ctx *HandlerContext, msg any) {
	h.err = ctx.WriteAndFlush(msg)
	h.writesInRead = h.writeCalls()
}

func (h *readFlushProbe) ChannelReadComplete(ctx *HandlerContext) {
	h.writesInComplete = h.writeCalls()
	ctx.FireChannelReadComplete()
}

func (h *readFlushProbe) writeCalls() int {
	if h.rw == nil {
		return 0
	}
	return h.rw.scalar + h.rw.writev
}

func TestUnsafeReadinessDefersWriteAndFlushUntilReadCallbackReturns(t *testing.T) {
	rw := &scriptedReadWriteRW{steps: []readStep{{data: "ok", again: true}}}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         &fakeReadyPoller{},
		ReadWriter:     rw,
		ReadBufferSize: 16,
	})
	probe := &readFlushProbe{rw: rw}
	if err := ch.Pipeline().AddLast("probe", probe); err != nil {
		t.Fatal(err)
	}

	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if probe.err != nil {
		t.Fatal(probe.err)
	}
	if probe.writesInRead != 0 || probe.writesInComplete != 0 {
		t.Fatalf("writes in callbacks=%d/%d, want 0/0", probe.writesInRead, probe.writesInComplete)
	}
	if rw.scalar != 1 || rw.writev != 0 {
		t.Fatalf("scalar=%d writev=%d, want one scalar write after read callback", rw.scalar, rw.writev)
	}
	if len(rw.writes) != 1 || len(rw.writes[0]) != 1 || rw.writes[0][0] != "ok" {
		t.Fatalf("writes=%v, want echo payload", rw.writes)
	}
}

func TestUnsafeEventLoopBatchFlushSchedulesOnce(t *testing.T) {
	rw := &scriptedReadWriteRW{steps: []readStep{{data: "ok", again: true}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         &fakeReadyPoller{},
		ReadWriter:     rw,
		ReadBufferSize: 16,
	})
	OptionFlushStrategy.Set(ch.Options(), FlushOnEventLoopBatch)
	scheduler := &recordingTailSubmitter{}
	unsafeCh.BindEventExecutor(scheduler)
	probe := &readFlushProbe{rw: rw}
	if err := ch.Pipeline().AddLast("probe", probe); err != nil {
		t.Fatal(err)
	}

	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if probe.err != nil {
		t.Fatal(probe.err)
	}
	if got := len(scheduler.tasks); got != 1 {
		t.Fatalf("scheduled flush tasks=%d, want 1", got)
	}
	if rw.scalar != 0 || rw.writev != 0 {
		t.Fatalf("scalar=%d writev=%d, want no write before tail task", rw.scalar, rw.writev)
	}

	scheduler.drain()
	if rw.scalar != 1 || rw.writev != 0 {
		t.Fatalf("scalar=%d writev=%d, want one scalar write after tail task", rw.scalar, rw.writev)
	}
	if len(scheduler.tasks) != 0 {
		t.Fatalf("scheduled flush tasks=%d, want drained", len(scheduler.tasks))
	}
}

func TestUnsafeOutboundPartialWrite(t *testing.T) {
	poller := &fakeReadyPoller{}
	rw := &partialWriteRW{steps: []writeStep{{n: 2, again: true}, {n: 2}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:                 1,
		FD:                 transport.FDRef{FD: 1},
		Allocator:          buffer.NewHeapAllocator(),
		Poller:             poller,
		ReadWriter:         rw,
		WriteHighWatermark: 4,
		WriteLowWatermark:  1,
	})
	recorder := &writabilityRecorder{}
	if err := ch.Pipeline().AddLast("writable", recorder); err != nil {
		t.Fatal(err)
	}

	buf := buffer.NewHeapBuffer(4)
	if _, err := buf.WriteBytes([]byte("abcd")); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}
	if ch.IsWritable() {
		t.Fatal("channel should be unwritable after crossing high watermark")
	}
	if ch.PendingOutboundBytes() != 2 {
		t.Fatalf("pending outbound bytes=%d, want 2 after partial write", ch.PendingOutboundBytes())
	}
	watermark := ch.WriteBufferWatermark()
	if watermark.Low != 1 || watermark.High != 4 {
		t.Fatalf("watermark=%+v", watermark)
	}
	if len(poller.modified) != 1 || poller.modified[0] != transport.ReadyRead|transport.ReadyWrite {
		t.Fatalf("modified=%v", poller.modified)
	}

	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if !ch.IsWritable() {
		t.Fatal("channel should become writable after draining below low watermark")
	}
	if ch.PendingOutboundBytes() != 0 {
		t.Fatalf("pending outbound bytes=%d, want 0", ch.PendingOutboundBytes())
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want 0", buf.RefCnt())
	}
	if len(rw.writes) != 2 || rw.writes[0] != "abcd" || rw.writes[1] != "cd" {
		t.Fatalf("writes=%v", rw.writes)
	}
	if len(poller.modified) != 2 || poller.modified[1] != transport.ReadyRead {
		t.Fatalf("modified=%v", poller.modified)
	}
	if recorder.changes != 2 {
		t.Fatalf("writability changes=%d, want 2", recorder.changes)
	}
}

func TestUnsafeWriteAndFlushUseNoPromiseFastPath(t *testing.T) {
	poller := &fakeReadyPoller{}
	rw := &partialWriteRW{steps: []writeStep{{n: 1, again: true}, {n: 1}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     poller,
		ReadWriter: rw,
	})

	buf := buffer.NewHeapBuffer(2)
	if _, err := buf.WriteBytes([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if err := ch.Write(buf); err != nil {
		t.Fatal(err)
	}
	if unsafeCh.outHead == nil || unsafeCh.outHead.promise != nil {
		t.Fatalf("outbound promise=%v, want nil fast path", unsafeCh.outHead)
	}
	if err := ch.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(unsafeCh.flushWaiters) != 0 {
		t.Fatalf("flush waiters=%d, want none on non-future flush", len(unsafeCh.flushWaiters))
	}
	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if buf.RefCnt() != 0 || ch.PendingOutboundBytes() != 0 {
		t.Fatalf("ref=%d pending=%d, want drained", buf.RefCnt(), ch.PendingOutboundBytes())
	}
}

func TestUnsafeWriteAndFlushDirectSmallBufferSkipsOutboundEntry(t *testing.T) {
	rw := &fullWriteRW{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:                 1,
		FD:                 transport.FDRef{FD: 1},
		Allocator:          buffer.NewHeapAllocator(),
		Poller:             &fakeReadyPoller{},
		ReadWriter:         rw,
		WriteHighWatermark: 1024,
		WriteLowWatermark:  512,
	})
	recorder := &flushCompleteRecorder{}
	if err := ch.Pipeline().AddLast("flush", recorder); err != nil {
		t.Fatal(err)
	}

	buf := buffer.NewSharedBuffer([]byte("ok"))
	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}

	if rw.writes != 1 {
		t.Fatalf("writes=%d, want 1", rw.writes)
	}
	if recorder.count != 1 {
		t.Fatalf("flush complete count=%d, want 1", recorder.count)
	}
	if ch.PendingOutboundBytes() != 0 || unsafeCh.outHead != nil || unsafeCh.outFree != nil {
		t.Fatalf("pending=%d outHead=%v outFree=%v, want direct drain", ch.PendingOutboundBytes(), unsafeCh.outHead, unsafeCh.outFree)
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want 0", buf.RefCnt())
	}
}

func TestUnsafeWriteAndFlushDirectPartialQueuesRemainingBytes(t *testing.T) {
	poller := &fakeReadyPoller{}
	rw := &partialWriteRW{steps: []writeStep{{n: 1, again: true}, {n: 1}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:                 1,
		FD:                 transport.FDRef{FD: 1},
		Allocator:          buffer.NewHeapAllocator(),
		Poller:             poller,
		ReadWriter:         rw,
		WriteHighWatermark: 1024,
		WriteLowWatermark:  512,
	})

	buf := buffer.NewSharedBuffer([]byte("ok"))
	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}
	if ch.PendingOutboundBytes() != 1 || unsafeCh.outHead == nil {
		t.Fatalf("pending=%d outHead=%v, want queued remainder", ch.PendingOutboundBytes(), unsafeCh.outHead)
	}
	if len(poller.modified) != 1 || poller.modified[0] != transport.ReadyRead|transport.ReadyWrite {
		t.Fatalf("modified=%v, want write interest", poller.modified)
	}
	if buf.RefCnt() != 1 {
		t.Fatalf("ref=%d, want queued buffer", buf.RefCnt())
	}

	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if ch.PendingOutboundBytes() != 0 || buf.RefCnt() != 0 {
		t.Fatalf("pending=%d ref=%d, want drained", ch.PendingOutboundBytes(), buf.RefCnt())
	}
	if len(rw.writes) != 2 || rw.writes[0] != "ok" || rw.writes[1] != "k" {
		t.Fatalf("writes=%v, want direct partial then queued tail", rw.writes)
	}
}

func TestUnsafeWriteStaticBytesAndFlushDirectDrains(t *testing.T) {
	rw := &vectorWriteRW{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:                 1,
		FD:                 transport.FDRef{FD: 1},
		Allocator:          buffer.NewHeapAllocator(),
		Poller:             &fakeReadyPoller{},
		ReadWriter:         rw,
		WriteHighWatermark: 1024,
		WriteLowWatermark:  512,
	})
	recorder := &flushCompleteRecorder{}
	if err := ch.Pipeline().AddLast("flush", recorder); err != nil {
		t.Fatal(err)
	}

	if err := unsafeCh.WriteStaticBytesAndFlush([]byte("ok")); err != nil {
		t.Fatal(err)
	}

	if rw.scalar != 1 || rw.writev != 0 {
		t.Fatalf("scalar=%d writev=%d, want 1/0", rw.scalar, rw.writev)
	}
	if len(rw.writes) != 1 || len(rw.writes[0]) != 1 || rw.writes[0][0] != "ok" {
		t.Fatalf("writes=%v, want static bytes", rw.writes)
	}
	if recorder.count != 1 {
		t.Fatalf("flush complete count=%d, want 1", recorder.count)
	}
	if ch.PendingOutboundBytes() != 0 || unsafeCh.outHead != nil || unsafeCh.outFree != nil {
		t.Fatalf("pending=%d outHead=%v outFree=%v, want direct static drain", ch.PendingOutboundBytes(), unsafeCh.outHead, unsafeCh.outFree)
	}
}

func TestUnsafeWriteStaticBytesAndFlushPartialQueuesRemainder(t *testing.T) {
	poller := &fakeReadyPoller{}
	rw := &partialWriteRW{steps: []writeStep{{n: 1, again: true}, {n: 1}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:                 1,
		FD:                 transport.FDRef{FD: 1},
		Allocator:          buffer.NewHeapAllocator(),
		Poller:             poller,
		ReadWriter:         rw,
		WriteHighWatermark: 1024,
		WriteLowWatermark:  512,
	})

	if err := unsafeCh.WriteStaticBytesAndFlush([]byte("ok")); err != nil {
		t.Fatal(err)
	}
	if ch.PendingOutboundBytes() != 1 || unsafeCh.outHead == nil {
		t.Fatalf("pending=%d outHead=%v, want queued static remainder", ch.PendingOutboundBytes(), unsafeCh.outHead)
	}
	if len(poller.modified) != 1 || poller.modified[0] != transport.ReadyRead|transport.ReadyWrite {
		t.Fatalf("modified=%v, want write interest", poller.modified)
	}

	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if ch.PendingOutboundBytes() != 0 {
		t.Fatalf("pending=%d, want drained", ch.PendingOutboundBytes())
	}
	if len(rw.writes) != 2 || rw.writes[0] != "ok" || rw.writes[1] != "k" {
		t.Fatalf("writes=%v, want direct partial then queued tail", rw.writes)
	}
}

func TestUnsafeWriteAndFlushFileRegionUsesFileRegionWriter(t *testing.T) {
	writer := &recordingFileRegionWriter{}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:               1,
		FD:               transport.FDRef{FD: 7},
		Allocator:        buffer.NewHeapAllocator(),
		Poller:           &fakeReadyPoller{},
		FileRegionWriter: writer,
	})
	recorder := &flushCompleteRecorder{}
	if err := ch.Pipeline().AddLast("flush", recorder); err != nil {
		t.Fatal(err)
	}
	region, err := NewFileRegion(strings.NewReader("abcdefgh"), 0, 8)
	if err != nil {
		t.Fatal(err)
	}

	if err := ch.WriteAndFlush(region); err != nil {
		t.Fatal(err)
	}
	if writer.calls != 1 || writer.fd.FD != 7 || writer.bytes != 8 {
		t.Fatalf("writer calls=%d fd=%d bytes=%d, want 1/7/8", writer.calls, writer.fd.FD, writer.bytes)
	}
	if ch.PendingOutboundBytes() != 0 {
		t.Fatalf("pending outbound bytes=%d, want 0", ch.PendingOutboundBytes())
	}
	if region.Transferred() != 8 {
		t.Fatalf("transferred=%d, want 8", region.Transferred())
	}
	if _, err := region.Read(make([]byte, 1)); !errors.Is(err, ErrFileRegionClosed) {
		t.Fatalf("err=%v, want closed region", err)
	}
	if recorder.count != 1 {
		t.Fatalf("flush complete count=%d, want 1", recorder.count)
	}
}

func TestUnsafeFileRegionPartialWriteKeepsFuturePendingUntilDrain(t *testing.T) {
	poller := &fakeReadyPoller{}
	writer := &recordingFileRegionWriter{
		steps: []fileRegionWriteStep{{n: 3, again: true}, {n: 5}},
	}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:                 1,
		FD:                 transport.FDRef{FD: 9},
		Allocator:          buffer.NewHeapAllocator(),
		Poller:             poller,
		FileRegionWriter:   writer,
		WriteHighWatermark: 8,
		WriteLowWatermark:  1,
	})
	region, err := NewFileRegion(strings.NewReader("abcdefgh"), 0, 8)
	if err != nil {
		t.Fatal(err)
	}

	future := ch.WriteFuture(region)
	if future.IsDone() {
		t.Fatal("write future completed before flush")
	}
	if err := ch.Flush(); err != nil {
		t.Fatal(err)
	}
	if future.IsDone() {
		t.Fatal("write future completed before region drained")
	}
	if got := ch.PendingOutboundBytes(); got != 5 {
		t.Fatalf("pending outbound bytes=%d, want 5", got)
	}
	if len(poller.modified) != 1 || poller.modified[0] != transport.ReadyRead|transport.ReadyWrite {
		t.Fatalf("modified=%v, want write interest", poller.modified)
	}

	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if !future.IsSuccess() {
		t.Fatalf("future success=%v err=%v", future.IsSuccess(), future.Err())
	}
	if ch.PendingOutboundBytes() != 0 {
		t.Fatalf("pending outbound bytes=%d, want 0", ch.PendingOutboundBytes())
	}
	if region.Transferred() != 8 {
		t.Fatalf("transferred=%d, want 8", region.Transferred())
	}
	if len(poller.modified) != 2 || poller.modified[1] != transport.ReadyRead {
		t.Fatalf("modified=%v, want write interest cleared", poller.modified)
	}
	if _, err := region.Read(make([]byte, 1)); !errors.Is(err, ErrFileRegionClosed) {
		t.Fatalf("err=%v, want closed region", err)
	}
}

func TestUnsafeFlushFastPathFiresCompleteWithoutWaiter(t *testing.T) {
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: &partialWriteRW{},
	})
	recorder := &flushCompleteRecorder{}
	if err := ch.Pipeline().AddLast("flush", recorder); err != nil {
		t.Fatal(err)
	}

	if err := ch.Flush(); err != nil {
		t.Fatal(err)
	}
	if recorder.count != 1 {
		t.Fatalf("flush complete count=%d, want 1", recorder.count)
	}
	if len(unsafeCh.flushWaiters) != 0 {
		t.Fatalf("flush waiters=%d, want none", len(unsafeCh.flushWaiters))
	}
}

func TestUnsafeReadinessWriteInterestRespectsAutoReadFalse(t *testing.T) {
	poller := &fakeReadyPoller{}
	rw := &partialWriteRW{steps: []writeStep{{n: 1, again: true}, {n: 1}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     poller,
		ReadWriter: rw,
	})
	OptionAutoRead.Set(ch.Options(), false)

	buf := buffer.NewHeapBuffer(2)
	if _, err := buf.WriteBytes([]byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}
	if len(poller.modified) != 1 || poller.modified[0] != transport.ReadyWrite {
		t.Fatalf("modified=%v, want write-only interest", poller.modified)
	}

	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if len(poller.modified) != 2 || poller.modified[1] != 0 {
		t.Fatalf("modified=%v, want no read interest after write drain", poller.modified)
	}
}

func TestUnsafeReadinessHonorsWriteSpinCount(t *testing.T) {
	poller := &fakeReadyPoller{}
	rw := &partialWriteRW{steps: []writeStep{
		{n: 1},
		{n: 1},
		{n: 1},
	}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     poller,
		ReadWriter: rw,
	})
	OptionWriteSpinCount.Set(ch.Options(), 2)

	buf := buffer.NewHeapBuffer(3)
	if _, err := buf.WriteBytes([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}
	if len(rw.writes) != 2 || rw.writes[0] != "abc" || rw.writes[1] != "bc" {
		t.Fatalf("writes after first flush=%v, want two write attempts", rw.writes)
	}
	if ch.PendingOutboundBytes() != 1 {
		t.Fatalf("pending outbound bytes=%d, want 1 after spin budget", ch.PendingOutboundBytes())
	}
	if len(poller.modified) != 1 || poller.modified[0] != transport.ReadyRead|transport.ReadyWrite {
		t.Fatalf("modified=%v, want write interest kept", poller.modified)
	}

	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyWrite})
	if len(rw.writes) != 3 || rw.writes[2] != "c" {
		t.Fatalf("writes after ready event=%v, want final write", rw.writes)
	}
	if ch.PendingOutboundBytes() != 0 {
		t.Fatalf("pending outbound bytes=%d, want drained", ch.PendingOutboundBytes())
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want 0", buf.RefCnt())
	}
	if len(poller.modified) != 2 || poller.modified[1] != transport.ReadyRead {
		t.Fatalf("modified=%v, want write interest cleared", poller.modified)
	}
}

func TestUnsafeReadinessUsesGatheringWrite(t *testing.T) {
	rw := &vectorWriteRW{}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: rw,
	})

	a := buffer.NewHeapBuffer(4)
	_, _ = a.WriteBytes([]byte("ab"))
	b := buffer.NewHeapBuffer(4)
	_, _ = b.WriteBytes([]byte("cd"))
	if err := ch.Write(a); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(b); err != nil {
		t.Fatal(err)
	}
	if rw.writev != 1 || rw.scalar != 0 {
		t.Fatalf("writev=%d scalar=%d", rw.writev, rw.scalar)
	}
	if len(rw.writes) != 1 || len(rw.writes[0]) != 2 || rw.writes[0][0] != "ab" || rw.writes[0][1] != "cd" {
		t.Fatalf("writes=%v", rw.writes)
	}
	if a.RefCnt() != 0 || b.RefCnt() != 0 {
		t.Fatalf("refs=%d,%d", a.RefCnt(), b.RefCnt())
	}
}

func TestUnsafeReadinessSkipsGatheringForSingleDirectBuffer(t *testing.T) {
	rw := &vectorWriteRW{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: rw,
	})

	buf := buffer.NewHeapBuffer(4)
	_, _ = buf.WriteBytes([]byte("pong"))
	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}
	if rw.scalar != 1 || rw.writev != 0 {
		t.Fatalf("scalar=%d writev=%d, want direct scalar write", rw.scalar, rw.writev)
	}
	if len(unsafeCh.writeSlices) != 0 {
		t.Fatalf("write slices=%d, want 0 for single direct buffer", len(unsafeCh.writeSlices))
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want 0", buf.RefCnt())
	}
}

func TestUnsafeCompletionSubmitsGatheringWrite(t *testing.T) {
	poller := &fakeCompletionPoller{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:        1,
		FD:        transport.FDRef{FD: 1},
		Allocator: buffer.NewHeapAllocator(),
		Poller:    poller,
	})

	a := buffer.NewHeapBuffer(4)
	_, _ = a.WriteBytes([]byte("ab"))
	b := buffer.NewHeapBuffer(4)
	_, _ = b.WriteBytes([]byte("cd"))
	if err := ch.Write(a); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(b); err != nil {
		t.Fatal(err)
	}
	if len(poller.submitted) != 1 || len(poller.submitted[0].Bufs) != 2 {
		t.Fatalf("submitted=%+v", poller.submitted)
	}
	if a.RefCnt() != 2 || b.RefCnt() != 2 {
		t.Fatalf("pending refs=%d,%d, want 2,2", a.RefCnt(), b.RefCnt())
	}
	unsafeCh.HandleEvent(transport.PollEvent{
		Model: transport.PollerCompletion,
		Op:    transport.OpWrite,
		FD:    transport.FDRef{FD: 1},
		Bufs:  poller.submitted[0].Bufs,
		N:     4,
	})
	if a.RefCnt() != 0 || b.RefCnt() != 0 {
		t.Fatalf("refs=%d,%d, want 0,0", a.RefCnt(), b.RefCnt())
	}
}

func TestUnsafeCloseFiresInactiveOnce(t *testing.T) {
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: &partialWriteRW{},
	})
	recorder := &inactiveRecorder{}
	if err := ch.Pipeline().AddLast("inactive", recorder); err != nil {
		t.Fatal(err)
	}

	if err := ch.Pipeline().Close(); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().Close(); err != nil {
		t.Fatal(err)
	}
	if recorder.count != 1 {
		t.Fatalf("inactive count=%d, want 1", recorder.count)
	}
}

func TestUnsafeCompletionReadKeepsPendingBufferAliveUntilEvent(t *testing.T) {
	poller := &fakeCompletionPoller{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         poller,
		ReadBufferSize: 4,
	})
	reader := &releaseReadHandler{closeAfter: true}
	if err := ch.Pipeline().AddLast("reader", reader); err != nil {
		t.Fatal(err)
	}

	if err := unsafeCh.BeginRead(); err != nil {
		t.Fatal(err)
	}
	if len(poller.submitted) != 1 {
		t.Fatalf("submitted=%d, want 1", len(poller.submitted))
	}
	if !poller.submitted[0].TransferBufferOwnership {
		t.Fatal("completion read should transfer buffer ownership to poller")
	}
	buf := poller.submitted[0].Buf
	if buf.RefCnt() != 1 {
		t.Fatalf("pending read ref=%d, want 1", buf.RefCnt())
	}
	if _, err := buf.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}

	unsafeCh.HandleEvent(transport.PollEvent{
		Model: transport.PollerCompletion,
		Op:    transport.OpRead,
		FD:    transport.FDRef{FD: 1},
		Buf:   buf,
		N:     4,
	})
	if reader.reads != 1 {
		t.Fatalf("reads=%d, want 1", reader.reads)
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("read buf ref=%d, want 0", buf.RefCnt())
	}
}

func TestUnsafeCompletionBatchesEchoWriteAndFollowUpRead(t *testing.T) {
	poller := &fakeBatchCompletionPoller{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         poller,
		ReadBufferSize: 16,
	})
	echo := &echoReadHandler{}
	if err := ch.Pipeline().AddLast("echo", echo); err != nil {
		t.Fatal(err)
	}
	if err := unsafeCh.BeginRead(); err != nil {
		t.Fatal(err)
	}
	if len(poller.submitted) != 1 {
		t.Fatalf("initial submitted=%d, want 1", len(poller.submitted))
	}

	readBuf := poller.submitted[0].Buf
	if _, err := readBuf.WriteBytes([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	unsafeCh.HandleEvent(transport.PollEvent{
		Model: transport.PollerCompletion,
		Op:    transport.OpRead,
		FD:    transport.FDRef{FD: 1},
		Buf:   readBuf,
		N:     4,
	})
	if echo.err != nil {
		t.Fatal(echo.err)
	}
	if echo.writes != 1 {
		t.Fatalf("echo writes=%d, want 1", echo.writes)
	}
	if len(poller.submitted) != 1 {
		t.Fatalf("standalone submitted=%d, want only initial read", len(poller.submitted))
	}
	if len(poller.batches) != 1 || len(poller.batches[0]) != 2 {
		t.Fatalf("batches=%v, want one write+read batch", poller.batches)
	}
	writeReq, nextReadReq := poller.batches[0][0], poller.batches[0][1]
	if writeReq.Op != transport.OpWrite || nextReadReq.Op != transport.OpRead {
		t.Fatalf("batch ops=%v,%v, want write,read", writeReq.Op, nextReadReq.Op)
	}
	if !nextReadReq.TransferBufferOwnership {
		t.Fatal("follow-up read should transfer buffer ownership")
	}
	if !unsafeCh.writePending || !unsafeCh.readPending {
		t.Fatalf("pending write=%v read=%v, want both pending", unsafeCh.writePending, unsafeCh.readPending)
	}

	unsafeCh.HandleEvent(transport.PollEvent{
		Model: transport.PollerCompletion,
		Op:    transport.OpWrite,
		FD:    transport.FDRef{FD: 1},
		Buf:   writeReq.Buf,
		N:     4,
	})
	unsafeCh.HandleEvent(transport.PollEvent{
		Model: transport.PollerCompletion,
		Op:    transport.OpRead,
		FD:    transport.FDRef{FD: 1},
		Buf:   nextReadReq.Buf,
	})
	if readBuf.RefCnt() != 0 {
		t.Fatalf("echo buf ref=%d, want 0", readBuf.RefCnt())
	}
}

func TestUnsafeCompletionAutoReadFalseRequiresManualRead(t *testing.T) {
	poller := &fakeCompletionPoller{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         poller,
		ReadBufferSize: 4,
	})
	OptionAutoRead.Set(ch.Options(), false)
	reader := &releaseReadHandler{}
	if err := ch.Pipeline().AddLast("reader", reader); err != nil {
		t.Fatal(err)
	}

	if err := unsafeCh.Activate(); err != nil {
		t.Fatal(err)
	}
	if len(poller.submitted) != 0 {
		t.Fatalf("submitted=%d, want 0 with AUTO_READ=false", len(poller.submitted))
	}
	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if len(poller.submitted) != 1 {
		t.Fatalf("submitted=%d, want 1 after manual read", len(poller.submitted))
	}
	if !poller.submitted[0].TransferBufferOwnership {
		t.Fatal("manual completion read should transfer buffer ownership to poller")
	}
	buf := poller.submitted[0].Buf
	if _, err := buf.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	unsafeCh.HandleEvent(transport.PollEvent{
		Model: transport.PollerCompletion,
		Op:    transport.OpRead,
		FD:    transport.FDRef{FD: 1},
		Buf:   buf,
		N:     4,
	})
	if reader.reads != 1 {
		t.Fatalf("reads=%d, want 1", reader.reads)
	}
	if len(poller.submitted) != 1 {
		t.Fatalf("submitted=%d, want no automatic follow-up read", len(poller.submitted))
	}
}

func TestUnsafeReadinessAutoReadFalseIgnoresReadableEventUntilManualRead(t *testing.T) {
	rw := &scriptedReadRW{steps: []readStep{{data: "ok", again: true}}}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: rw,
	})
	OptionAutoRead.Set(ch.Options(), false)
	reader := &releaseReadHandler{}
	if err := ch.Pipeline().AddLast("reader", reader); err != nil {
		t.Fatal(err)
	}

	unsafeCh.HandleEvent(transport.PollEvent{Model: transport.PollerReadiness, Ready: transport.ReadyRead})
	if rw.reads != 0 || reader.reads != 0 {
		t.Fatalf("reads=%d handler=%d, want 0 before manual read", rw.reads, reader.reads)
	}
	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if rw.reads != 1 || reader.reads != 1 {
		t.Fatalf("reads=%d handler=%d, want 1 after manual read", rw.reads, reader.reads)
	}
}

func TestUnsafeReadinessHonorsMaxMessagesPerRead(t *testing.T) {
	rw := &scriptedReadRW{steps: []readStep{
		{data: "a"},
		{data: "b"},
		{data: "c", again: true},
	}}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:             1,
		FD:             transport.FDRef{FD: 1},
		Allocator:      buffer.NewHeapAllocator(),
		Poller:         &fakeReadyPoller{},
		ReadWriter:     rw,
		ReadBufferSize: 1,
	})
	OptionMaxMessagesPerRead.Set(ch.Options(), 2)
	reader := &releaseReadHandler{}
	if err := ch.Pipeline().AddLast("reader", reader); err != nil {
		t.Fatal(err)
	}

	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if rw.reads != 2 || reader.reads != 2 {
		t.Fatalf("reads=%d handler=%d, want 2 after first read loop", rw.reads, reader.reads)
	}
	if err := ch.Read(); err != nil {
		t.Fatal(err)
	}
	if rw.reads != 3 || reader.reads != 3 {
		t.Fatalf("reads=%d handler=%d, want remaining message after second read loop", rw.reads, reader.reads)
	}
}

func TestUnsafeOptionCacheTracksSetAndRemove(t *testing.T) {
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:         1,
		FD:         transport.FDRef{FD: 1},
		Allocator:  buffer.NewHeapAllocator(),
		Poller:     &fakeReadyPoller{},
		ReadWriter: &scriptedReadRW{},
	})

	OptionAutoRead.Set(ch.Options(), false)
	if unsafeCh.AutoRead() {
		t.Fatal("auto read cache still true after set false")
	}
	OptionAutoRead.Remove(ch.Options())
	if !unsafeCh.AutoRead() {
		t.Fatal("auto read cache did not restore default after remove")
	}

	OptionWriteSpinCount.Set(ch.Options(), 0)
	if got := unsafeCh.maxWriteSpinCount(); got != 1 {
		t.Fatalf("write spin count=%d, want 1 for non-positive value", got)
	}
	OptionWriteSpinCount.Remove(ch.Options())
	if got := unsafeCh.maxWriteSpinCount(); got != OptionWriteSpinCount.Default() {
		t.Fatalf("write spin count=%d, want default %d", got, OptionWriteSpinCount.Default())
	}

	OptionMaxMessagesPerRead.Set(ch.Options(), 2)
	if got := unsafeCh.maxMessagesPerRead(); got != 2 {
		t.Fatalf("max messages per read=%d, want 2", got)
	}
	OptionMaxMessagesPerRead.Remove(ch.Options())
	if got := unsafeCh.maxMessagesPerRead(); got != OptionMaxMessagesPerRead.Default() {
		t.Fatalf("max messages per read=%d, want default %d", got, OptionMaxMessagesPerRead.Default())
	}

	OptionFlushStrategy.Set(ch.Options(), FlushImmediate)
	if got := unsafeCh.flushStrategy(); got != FlushImmediate {
		t.Fatalf("flush strategy=%d, want immediate", got)
	}
	OptionFlushStrategy.Remove(ch.Options())
	if got := unsafeCh.flushStrategy(); got != OptionFlushStrategy.Default() {
		t.Fatalf("flush strategy=%d, want default %d", got, OptionFlushStrategy.Default())
	}
}

func TestUnsafeCompletionWriteKeepsPendingBufferAliveUntilEvent(t *testing.T) {
	poller := &fakeCompletionPoller{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:        1,
		FD:        transport.FDRef{FD: 1},
		Allocator: buffer.NewHeapAllocator(),
		Poller:    poller,
	})

	buf := buffer.NewHeapBuffer(4)
	if _, err := buf.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(buf); err != nil {
		t.Fatal(err)
	}
	if len(poller.submitted) != 1 {
		t.Fatalf("submitted=%d, want 1", len(poller.submitted))
	}
	if buf.RefCnt() != 2 {
		t.Fatalf("pending write ref=%d, want 2", buf.RefCnt())
	}

	unsafeCh.HandleEvent(transport.PollEvent{
		Model: transport.PollerCompletion,
		Op:    transport.OpWrite,
		FD:    transport.FDRef{FD: 1},
		Buf:   buf,
		N:     4,
	})
	if buf.RefCnt() != 0 {
		t.Fatalf("write buf ref=%d, want 0", buf.RefCnt())
	}
}

func TestUnsafeCompletionWriteUsesFixedBufferIndexForIOUring(t *testing.T) {
	poller := &fakeCompletionPoller{backend: transport.BackendIOUring}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:           1,
		FD:           transport.FDRef{FD: 1},
		Allocator:    buffer.NewHeapAllocator(),
		Poller:       poller,
		FixedBuffers: true,
	})

	inner := buffer.NewHeapBuffer(4)
	if _, err := inner.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := ch.WriteAndFlush(fixedTestBuf{ByteBuf: inner, idx: 7}); err != nil {
		t.Fatal(err)
	}
	if len(poller.submitted) != 1 {
		t.Fatalf("submitted=%d, want 1", len(poller.submitted))
	}
	req := poller.submitted[0]
	if !req.UseFixedBuffer || req.FixedBufferIndex != 7 {
		t.Fatalf("fixed request=%+v, want fixed buffer index 7", req)
	}
}

func TestUnsafeCompletionCloseFiresInactiveOnCloseCompletion(t *testing.T) {
	poller := &fakeCompletionPoller{}
	ch, unsafeCh := NewUnsafeChannel(UnsafeConfig{
		ID:        1,
		FD:        transport.FDRef{FD: 1},
		Allocator: buffer.NewHeapAllocator(),
		Poller:    poller,
	})
	unsafeCh.MarkRegistered()
	recorder := &inactiveRecorder{}
	if err := ch.Pipeline().AddLast("inactive", recorder); err != nil {
		t.Fatal(err)
	}

	if err := ch.Pipeline().Close(); err != nil {
		t.Fatal(err)
	}
	if recorder.count != 0 {
		t.Fatalf("inactive count before completion=%d, want 0", recorder.count)
	}
	if len(poller.submitted) != 1 || poller.submitted[0].Op != transport.OpClose {
		t.Fatalf("submitted=%v, want close", poller.submitted)
	}

	unsafeCh.HandleEvent(transport.PollEvent{
		Model: transport.PollerCompletion,
		Op:    transport.OpClose,
		FD:    transport.FDRef{FD: 1},
	})
	if recorder.count != 1 {
		t.Fatalf("inactive count=%d, want 1", recorder.count)
	}
}

func TestUnsafeCompletionCloseBeforeRegisterFiresInactiveSynchronously(t *testing.T) {
	poller := &fakeCompletionPoller{}
	ch, _ := NewUnsafeChannel(UnsafeConfig{
		ID:        1,
		FD:        transport.FDRef{FD: 1},
		Allocator: buffer.NewHeapAllocator(),
		Poller:    poller,
	})
	recorder := &inactiveRecorder{}
	if err := ch.Pipeline().AddLast("inactive", recorder); err != nil {
		t.Fatal(err)
	}

	if err := ch.Pipeline().Close(); err != nil {
		t.Fatal(err)
	}
	if recorder.count != 1 {
		t.Fatalf("inactive count=%d, want 1", recorder.count)
	}
	if len(poller.submitted) != 0 {
		t.Fatalf("submitted=%v, want no async close before register", poller.submitted)
	}
}
