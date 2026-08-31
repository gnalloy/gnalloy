package codec

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type codecOutboundSink struct {
	writes   []any
	flushes  int
	closes   int
	writeAt  int
	writeErr error
	calls    int
}

func (s *codecOutboundSink) Write(msg any) error {
	s.calls++
	if s.writeErr != nil && s.calls == s.writeAt {
		return s.writeErr
	}
	s.writes = append(s.writes, msg)
	return nil
}

func (s *codecOutboundSink) Flush() error {
	s.flushes++
	return nil
}

func (s *codecOutboundSink) Close() error {
	s.closes++
	return nil
}

func (s *codecOutboundSink) release() {
	for _, msg := range s.writes {
		if buf, ok := msg.(buffer.ByteBuf); ok {
			buf.Release()
		}
	}
	s.writes = nil
}

func TestLengthFieldPrependerWritesHeaderThenPayload(t *testing.T) {
	prepender, err := NewLengthFieldPrepender(4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	_ = ch.Pipeline().AddLast("prepender", prepender)
	defer sink.release()

	payload := testBuf([]byte("hello"))
	if err := ch.WriteAndFlush(payload); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 2 {
		t.Fatalf("writes=%d, want 2", len(sink.writes))
	}
	header := sink.writes[0].(buffer.ByteBuf)
	body := sink.writes[1].(buffer.ByteBuf)
	if got := header.Bytes(); string(got) != string([]byte{0, 0, 0, 5}) {
		t.Fatalf("header=%v", got)
	}
	if string(body.Bytes()) != "hello" {
		t.Fatalf("body=%q", body.Bytes())
	}
}

func TestLengthFieldPrependerIncludesHeaderWidth(t *testing.T) {
	prepender, err := NewLengthFieldPrependerWithOptions(2, 0, true, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	_ = ch.Pipeline().AddLast("prepender", prepender)
	defer sink.release()

	if err := ch.Write(testBuf([]byte("abc"))); err != nil {
		t.Fatal(err)
	}
	header := sink.writes[0].(buffer.ByteBuf)
	if got := header.Bytes(); string(got) != string([]byte{0, 5}) {
		t.Fatalf("header=%v", got)
	}
}

func TestStringEncoderAndLengthPrependerOutboundOrder(t *testing.T) {
	prepender, err := NewLengthFieldPrepender(4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	_ = ch.Pipeline().AddLast("prepender", prepender)
	_ = ch.Pipeline().AddLast("string", NewStringEncoder())
	defer sink.release()

	if err := ch.Write("ping"); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 2 {
		t.Fatalf("writes=%d, want 2", len(sink.writes))
	}
	header := sink.writes[0].(buffer.ByteBuf)
	body := sink.writes[1].(buffer.ByteBuf)
	if got := header.Bytes(); string(got) != string([]byte{0, 0, 0, 4}) {
		t.Fatalf("header=%v", got)
	}
	if string(body.Bytes()) != "ping" {
		t.Fatalf("body=%q", body.Bytes())
	}
}

func TestStringDecoderReleasesInput(t *testing.T) {
	collector := &captureStringInbound{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", NewStringDecoder())
	_ = ch.Pipeline().AddLast("collector", collector)

	in := testBuf([]byte("hello"))
	ch.Pipeline().FireChannelRead(in)
	if in.RefCnt() != 0 {
		t.Fatalf("ref=%d, want 0", in.RefCnt())
	}
	if len(collector.msgs) != 1 || collector.msgs[0] != "hello" {
		t.Fatalf("msgs=%v", collector.msgs)
	}
}

func TestByteSliceEncoderDecoder(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	_ = ch.Pipeline().AddLast("bytes", NewByteSliceEncoder())
	defer sink.release()

	if err := ch.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 1 {
		t.Fatalf("writes=%d, want 1", len(sink.writes))
	}
	buf := sink.writes[0].(buffer.ByteBuf)
	if string(buf.Bytes()) != "abc" {
		t.Fatalf("buf=%q", buf.Bytes())
	}

	collector := &captureBytesInbound{}
	inCh := channel.NewLocalChannel(2, buffer.NewHeapAllocator(), nil)
	_ = inCh.Pipeline().AddLast("bytes", NewByteSliceDecoder())
	_ = inCh.Pipeline().AddLast("collector", collector)
	inCh.Pipeline().FireChannelRead(testBuf([]byte("xyz")))
	if len(collector.msgs) != 1 || string(collector.msgs[0]) != "xyz" {
		t.Fatalf("msgs=%q", collector.msgs)
	}
}

func TestLengthFieldPrependerReleasesPayloadWhenBodyWriteFails(t *testing.T) {
	writeErr := errors.New("write failed")
	prepender, err := NewLengthFieldPrepender(4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	sink := &codecOutboundSink{writeAt: 2, writeErr: writeErr}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("prepender", prepender); err != nil {
		t.Fatal(err)
	}
	defer sink.release()

	payload := testBuf([]byte("abc"))
	if err := ch.Write(payload); !errors.Is(err, writeErr) {
		t.Fatalf("err=%v, want writeErr", err)
	}
	if payload.RefCnt() != 0 {
		t.Fatalf("payload ref=%d, want 0", payload.RefCnt())
	}
	if len(sink.writes) != 1 {
		t.Fatalf("writes=%d, want header accepted before body failure", len(sink.writes))
	}
}

type captureStringInbound struct {
	msgs []string
}

func (h *captureStringInbound) ChannelRead(_ *channel.HandlerContext, msg any) {
	if s, ok := msg.(string); ok {
		h.msgs = append(h.msgs, s)
	}
}

type captureBytesInbound struct {
	msgs [][]byte
}

func (h *captureBytesInbound) ChannelRead(_ *channel.HandlerContext, msg any) {
	if b, ok := msg.([]byte); ok {
		h.msgs = append(h.msgs, b)
	}
}
