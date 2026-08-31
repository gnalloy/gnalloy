package codec

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestLineEncoderWritesStringWithLineSeparator(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("line", NewLineEncoder(LineSeparatorUnix)); err != nil {
		t.Fatal(err)
	}
	defer sink.release()

	if err := ch.Write("ok"); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 1 {
		t.Fatalf("writes=%d, want 1", len(sink.writes))
	}
	if got := string(sink.writes[0].(buffer.ByteBuf).Bytes()); got != "ok\n" {
		t.Fatalf("encoded=%q", got)
	}
}

func TestDelimiterBasedFrameEncoderAppendsDelimiterWithoutCopyingByteBufPayload(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	encoder, err := NewDelimiterBasedFrameEncoder([]byte("|"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("delimiter", encoder); err != nil {
		t.Fatal(err)
	}
	defer sink.release()

	payload := testBuf([]byte("abc"))
	if err := ch.Write(payload); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 2 {
		t.Fatalf("writes=%d, want payload and delimiter", len(sink.writes))
	}
	if sink.writes[0] != payload {
		t.Fatal("payload ByteBuf should be forwarded without copying")
	}
	if got := string(sink.writes[1].(buffer.ByteBuf).Bytes()); got != "|" {
		t.Fatalf("delimiter=%q", got)
	}
}

func TestFixedLengthFrameEncoderRejectsWrongSizeAndReleasesInput(t *testing.T) {
	encoder, err := NewFixedLengthFrameEncoder(4)
	if err != nil {
		t.Fatal(err)
	}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), &codecOutboundSink{})
	if err := ch.Pipeline().AddLast("fixed", encoder); err != nil {
		t.Fatal(err)
	}

	payload := testBuf([]byte("abc"))
	if err := ch.Write(payload); !errors.Is(err, ErrInvalidFrameLength) {
		t.Fatalf("err=%v, want ErrInvalidFrameLength", err)
	}
	if payload.RefCnt() != 0 {
		t.Fatalf("payload ref=%d, want 0", payload.RefCnt())
	}
}

func TestFixedLengthFrameEncoderForwardsExactByteBuf(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	encoder, err := NewFixedLengthFrameEncoder(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("fixed", encoder); err != nil {
		t.Fatal(err)
	}
	defer sink.release()

	payload := testBuf([]byte("abc"))
	if err := ch.Write(payload); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 1 || sink.writes[0] != payload {
		t.Fatalf("writes=%v, want original payload", sink.writes)
	}
}

func TestLineEncoderWriteAndFlushPropagatesFlush(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("line", NewLineEncoder(LineSeparatorUnix)); err != nil {
		t.Fatal(err)
	}
	defer sink.release()

	if err := ch.WriteAndFlush("ok"); err != nil {
		t.Fatal(err)
	}
	if sink.flushes != 1 {
		t.Fatalf("flushes=%d, want 1", sink.flushes)
	}
}
