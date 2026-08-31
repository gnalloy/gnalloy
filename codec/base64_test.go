package codec

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestBase64EncoderWritesEncodedBufferAndReleasesInput(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("base64", NewBase64Encoder()); err != nil {
		t.Fatal(err)
	}
	defer sink.release()

	in := testBuf([]byte("hello"))
	if err := ch.Write(in); err != nil {
		t.Fatal(err)
	}
	if in.RefCnt() != 0 {
		t.Fatalf("input ref=%d, want 0", in.RefCnt())
	}
	if len(sink.writes) != 1 {
		t.Fatalf("writes=%d, want 1", len(sink.writes))
	}
	out := sink.writes[0].(buffer.ByteBuf)
	if got := string(out.Bytes()); got != "aGVsbG8=" {
		t.Fatalf("encoded=%q", got)
	}
}

func TestBase64URLDialect(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("base64", NewBase64EncoderWithDialect(Base64URLDialect)); err != nil {
		t.Fatal(err)
	}
	defer sink.release()

	if err := ch.Write(testBuf([]byte{0xfb, 0xff})); err != nil {
		t.Fatal(err)
	}
	out := sink.writes[0].(buffer.ByteBuf)
	if got := string(out.Bytes()); got != "-_8=" {
		t.Fatalf("encoded=%q", got)
	}
}

func TestBase64DecoderEmitsDecodedBuffer(t *testing.T) {
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("base64", NewBase64Decoder()); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	in := testBuf([]byte("cGluZw=="))
	ch.Pipeline().FireChannelRead(in)
	if in.RefCnt() != 0 {
		t.Fatalf("input ref=%d, want 0", in.RefCnt())
	}
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(collector.frames))
	}
	if got := string(collector.frames[0].Bytes()); got != "ping" {
		t.Fatalf("decoded=%q", got)
	}
	collector.release()
}

func TestBase64DecoderReportsInvalidInput(t *testing.T) {
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("base64", NewBase64Decoder()); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	in := testBuf([]byte("not-base64!!"))
	ch.Pipeline().FireChannelRead(in)
	if in.RefCnt() != 0 {
		t.Fatalf("input ref=%d, want 0", in.RefCnt())
	}
	if len(collector.frames) != 0 {
		t.Fatalf("frames=%d, want 0", len(collector.frames))
	}
	if len(collector.errs) != 1 {
		t.Fatalf("errs=%d, want 1", len(collector.errs))
	}
}
