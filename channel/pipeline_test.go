package channel

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
)

type captureInbound struct {
	msgs []any
}

func (h *captureInbound) ChannelRead(_ *HandlerContext, msg any) {
	h.msgs = append(h.msgs, msg)
}

type forwardingInbound struct{}

func (forwardingInbound) ChannelRead(ctx *HandlerContext, msg any) {
	ctx.FireChannelRead(msg)
}

type captureSink struct {
	writes  []any
	flushed bool
	closed  bool
}

func (s *captureSink) Write(msg any) error {
	s.writes = append(s.writes, msg)
	return nil
}

func (s *captureSink) Flush() error {
	s.flushed = true
	return nil
}

func (s *captureSink) Close() error {
	s.closed = true
	return nil
}

type captureWriteAndFlushSink struct {
	captureSink
	writeAndFlushes []any
}

func (s *captureWriteAndFlushSink) WriteAndFlush(msg any) error {
	s.writeAndFlushes = append(s.writeAndFlushes, msg)
	return nil
}

type countingWriteAndFlushSink struct {
	writeAndFlushes int
}

func (s *countingWriteAndFlushSink) Write(any) error {
	return nil
}

func (s *countingWriteAndFlushSink) Flush() error {
	return nil
}

func (s *countingWriteAndFlushSink) Close() error {
	return nil
}

func (s *countingWriteAndFlushSink) WriteAndFlush(any) error {
	s.writeAndFlushes++
	return nil
}

type outboundRecorder struct {
	writes  int
	flushes int
}

func (h *outboundRecorder) Write(ctx *HandlerContext, msg any) error {
	h.writes++
	return ctx.Write(msg)
}

func (h *outboundRecorder) Flush(ctx *HandlerContext) error {
	h.flushes++
	return ctx.Flush()
}

func TestPipelineInboundPropagation(t *testing.T) {
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), &captureSink{})
	capture := &captureInbound{}
	if err := ch.Pipeline().AddLast("forward", forwardingInbound{}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("capture", capture); err != nil {
		t.Fatal(err)
	}
	ch.Pipeline().FireChannelRead("hello")
	if len(capture.msgs) != 1 || capture.msgs[0] != "hello" {
		t.Fatalf("msgs=%v", capture.msgs)
	}
}

func TestPipelineOutboundSink(t *testing.T) {
	sink := &captureSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().Write("payload"); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().Close(); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 1 || sink.writes[0] != "payload" || !sink.flushed || !sink.closed {
		t.Fatalf("sink=%+v", sink)
	}
}

func TestPipelineWriteAndFlushUsesDirectSinkWithoutOutboundHandlers(t *testing.T) {
	sink := &captureWriteAndFlushSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("inbound", forwardingInbound{}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().WriteAndFlush("payload"); err != nil {
		t.Fatal(err)
	}
	if len(sink.writeAndFlushes) != 1 || sink.writeAndFlushes[0] != "payload" {
		t.Fatalf("writeAndFlushes=%v, want payload", sink.writeAndFlushes)
	}
	if len(sink.writes) != 0 || sink.flushed {
		t.Fatalf("separate write/flush used: writes=%v flushed=%t", sink.writes, sink.flushed)
	}
}

func TestPipelineWriteAndFlushPreservesOutboundHandlers(t *testing.T) {
	sink := &captureWriteAndFlushSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	recorder := &outboundRecorder{}
	if err := ch.Pipeline().AddLast("outbound", recorder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().WriteAndFlush("payload"); err != nil {
		t.Fatal(err)
	}
	if recorder.writes != 1 || recorder.flushes != 1 {
		t.Fatalf("recorder writes=%d flushes=%d, want 1/1", recorder.writes, recorder.flushes)
	}
	if len(sink.writeAndFlushes) != 0 {
		t.Fatalf("direct sink used despite outbound handler: %v", sink.writeAndFlushes)
	}
	if len(sink.writes) != 1 || sink.writes[0] != "payload" || !sink.flushed {
		t.Fatalf("sink writes=%v flushed=%t, want separate path", sink.writes, sink.flushed)
	}
}

func TestPipelineWriteAndFlushRestoresDirectSinkAfterOutboundRemove(t *testing.T) {
	sink := &captureWriteAndFlushSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("outbound", &outboundRecorder{}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().Remove("outbound"); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().WriteAndFlush("payload"); err != nil {
		t.Fatal(err)
	}
	if len(sink.writeAndFlushes) != 1 || len(sink.writes) != 0 || sink.flushed {
		t.Fatalf("sink=%+v", sink)
	}
}

func TestPipelineWriteAndFlushRestoresDirectSinkAfterOutboundReplace(t *testing.T) {
	sink := &captureWriteAndFlushSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("outbound", &outboundRecorder{}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().Replace("outbound", "inbound", forwardingInbound{}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().WriteAndFlush("payload"); err != nil {
		t.Fatal(err)
	}
	if len(sink.writeAndFlushes) != 1 || len(sink.writes) != 0 || sink.flushed {
		t.Fatalf("sink=%+v", sink)
	}
}

func TestPipelineWriteAndFlushDisablesDirectSinkAfterInboundReplace(t *testing.T) {
	sink := &captureWriteAndFlushSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	recorder := &outboundRecorder{}
	if err := ch.Pipeline().AddLast("inbound", forwardingInbound{}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().Replace("inbound", "outbound", recorder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().WriteAndFlush("payload"); err != nil {
		t.Fatal(err)
	}
	if recorder.writes != 1 || recorder.flushes != 1 {
		t.Fatalf("recorder writes=%d flushes=%d, want 1/1", recorder.writes, recorder.flushes)
	}
	if len(sink.writeAndFlushes) != 0 {
		t.Fatalf("direct sink used despite replaced outbound handler: %v", sink.writeAndFlushes)
	}
}

func TestPipelineTailReleasesByteBuf(t *testing.T) {
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), &captureSink{})
	buf := buffer.NewHeapBuffer(8)
	_, _ = buf.WriteBytes([]byte("abc"))
	ch.Pipeline().FireChannelRead(buf)
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want 0", buf.RefCnt())
	}
}

func TestPipelineAddBeforeAfterAndReplaceKeepOrder(t *testing.T) {
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), &captureSink{})
	pipeline := ch.Pipeline()
	if err := pipeline.AddLast("first", forwardingInbound{}); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.AddAfter("first", "third", forwardingInbound{}); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.AddBefore("third", "second", forwardingInbound{}); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Replace("third", "last", forwardingInbound{}); err != nil {
		t.Fatal(err)
	}
	got := pipeline.Names()
	want := []string{"first", "second", "last"}
	if len(got) != len(want) {
		t.Fatalf("names=%v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("names=%v, want %v", got, want)
		}
	}
	first, ok := pipeline.FirstContext()
	if !ok || first.Name() != "first" {
		t.Fatalf("first=%v ok=%v", first, ok)
	}
	last, ok := pipeline.LastContext()
	if !ok || last.Name() != "last" {
		t.Fatalf("last=%v ok=%v", last, ok)
	}
	if _, ok := pipeline.Context("third"); ok {
		t.Fatal("old handler name still exists after replace")
	}
}

func BenchmarkPipelineInboundNoop(b *testing.B) {
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), &captureSink{})
	if err := ch.Pipeline().AddLast("forward", forwardingInbound{}); err != nil {
		b.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("capture", &captureInbound{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ch.Pipeline().FireChannelRead("msg")
	}
}

func BenchmarkPipelineWriteAndFlushDirectSink(b *testing.B) {
	sink := &countingWriteAndFlushSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("inbound", forwardingInbound{}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := ch.Pipeline().WriteAndFlush("msg"); err != nil {
			b.Fatal(err)
		}
	}
	if sink.writeAndFlushes != b.N {
		b.Fatalf("writeAndFlushes=%d, want %d", sink.writeAndFlushes, b.N)
	}
}
