package embedded

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestEmbeddedChannelCapturesInboundMessages(t *testing.T) {
	ch, err := New(inboundUpper{})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.FinishAndReleaseAll()

	buf := buffer.NewHeapBuffer(8)
	if _, err := buf.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	wrote, err := ch.WriteInbound(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !wrote {
		t.Fatal("inbound message was not captured")
	}
	msg, ok := ch.ReadInbound()
	if !ok {
		t.Fatal("missing inbound message")
	}
	defer releaseMessage(msg)
	if got := string(msg.(buffer.ByteBuf).Bytes()); got != "PING" {
		t.Fatalf("inbound=%q, want PING", got)
	}
}

func TestEmbeddedChannelCapturesOutboundAndFlush(t *testing.T) {
	ch, err := New(outboundPrefix{})
	if err != nil {
		t.Fatal(err)
	}
	defer ch.FinishAndReleaseAll()

	buf := buffer.NewHeapBuffer(16)
	if _, err := buf.WriteBytes([]byte("data")); err != nil {
		t.Fatal(err)
	}
	wrote, err := ch.WriteOutbound(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !wrote || ch.Flushes() != 1 {
		t.Fatalf("wrote=%v flushes=%d", wrote, ch.Flushes())
	}
	msg, ok := ch.ReadOutbound()
	if !ok {
		t.Fatal("missing outbound message")
	}
	defer releaseMessage(msg)
	if got := string(msg.(buffer.ByteBuf).Bytes()); got != "out:data" {
		t.Fatalf("outbound=%q, want out:data", got)
	}
}

func TestEmbeddedChannelCloseRejectsNewMessages(t *testing.T) {
	ch, err := New()
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	buf := buffer.NewHeapBuffer(4)
	if _, err := buf.WriteBytes([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.WriteInbound(buf); !errors.Is(err, ErrClosed) {
		t.Fatalf("err=%v, want ErrClosed", err)
	}
	if buf.RefCnt() != 0 {
		t.Fatalf("refcnt=%d, want released", buf.RefCnt())
	}
}

type inboundUpper struct{}

func (inboundUpper) ChannelRead(ctx *channel.HandlerContext, msg any) {
	in, ok := msg.(buffer.ByteBuf)
	if !ok {
		ctx.FireChannelRead(msg)
		return
	}
	out := buffer.NewHeapBuffer(in.ReadableBytes())
	data := append([]byte(nil), in.Bytes()...)
	for i, b := range data {
		if b >= 'a' && b <= 'z' {
			data[i] = b - 'a' + 'A'
		}
	}
	_, _ = out.WriteBytes(data)
	in.Release()
	ctx.FireChannelRead(out)
}

type outboundPrefix struct{}

func (outboundPrefix) Write(ctx *channel.HandlerContext, msg any) error {
	in, ok := msg.(buffer.ByteBuf)
	if !ok {
		return ctx.Write(msg)
	}
	out := buffer.NewHeapBuffer(in.ReadableBytes() + len("out:"))
	_, _ = out.WriteBytes([]byte("out:"))
	_, _ = out.WriteBytes(in.Bytes())
	in.Release()
	return ctx.Write(out)
}
