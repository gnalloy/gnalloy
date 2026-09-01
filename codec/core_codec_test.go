package codec

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestMessageToByteEncoder(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	encoder := NewMessageToByteEncoderFunc(
		func(msg any) bool { _, ok := msg.(string); return ok },
		func(*channel.HandlerContext, any) int { return 4 },
		func(_ *channel.HandlerContext, msg any, out buffer.ByteBuf) error {
			_, err := out.WriteBytes([]byte(msg.(string)))
			return err
		},
	)
	_ = ch.Pipeline().AddLast("encoder", encoder)
	defer sink.release()

	if err := ch.Write("ping"); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 1 {
		t.Fatalf("writes=%d, want 1", len(sink.writes))
	}
	buf := sink.writes[0].(buffer.ByteBuf)
	if string(buf.Bytes()) != "ping" {
		t.Fatalf("buf=%q", buf.Bytes())
	}
}

func TestMessageToByteEncoderReleasesOutputOnWriteError(t *testing.T) {
	writeErr := errors.New("write failed")
	sink := &failingOutboundSink{err: writeErr}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	var encoded buffer.ByteBuf
	encoder := NewMessageToByteEncoderFunc(
		func(msg any) bool { _, ok := msg.(string); return ok },
		func(*channel.HandlerContext, any) int { return 4 },
		func(_ *channel.HandlerContext, msg any, out buffer.ByteBuf) error {
			encoded = out
			_, err := out.WriteBytes([]byte(msg.(string)))
			return err
		},
	)
	_ = ch.Pipeline().AddLast("encoder", encoder)

	err := ch.Write("ping")
	if !errors.Is(err, writeErr) {
		t.Fatalf("err=%v, want %v", err, writeErr)
	}
	if refs := encoded.RefCnt(); refs != 0 {
		encoded.Release()
		t.Fatalf("encoded refs=%d, want 0", refs)
	}
}

func TestByteSliceEncoderReleasesOutputOnWriteError(t *testing.T) {
	writeErr := errors.New("write failed")
	sink := &failingOutboundSink{err: writeErr}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	_ = ch.Pipeline().AddLast("bytes", NewByteSliceEncoder())

	err := ch.Write([]byte("ping"))
	if !errors.Is(err, writeErr) {
		t.Fatalf("err=%v, want %v", err, writeErr)
	}
	if len(sink.writes) != 1 {
		t.Fatalf("writes=%d, want 1", len(sink.writes))
	}
	buf := sink.writes[0].(buffer.ByteBuf)
	if refs := buf.RefCnt(); refs != 0 {
		buf.Release()
		t.Fatalf("buf refs=%d, want 0", refs)
	}
}

func TestMessageToMessageDecoder(t *testing.T) {
	collector := &captureStringInbound{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	decoder := NewMessageToMessageDecoderFunc(
		func(msg any) bool { _, ok := msg.(string); return ok },
		func(_ *channel.HandlerContext, msg any, out *MessageList) error {
			out.Add(msg.(string) + "-1")
			out.Add(msg.(string) + "-2")
			return nil
		},
	)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead("m")
	if len(collector.msgs) != 2 || collector.msgs[0] != "m-1" || collector.msgs[1] != "m-2" {
		t.Fatalf("msgs=%v", collector.msgs)
	}
}

func TestMessageToMessageEncoder(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	encoder := NewMessageToMessageEncoderFunc(
		func(msg any) bool { _, ok := msg.(string); return ok },
		func(_ *channel.HandlerContext, msg any, out *MessageList) error {
			out.Add(msg.(string) + "a")
			out.Add(msg.(string) + "b")
			return nil
		},
	)
	_ = ch.Pipeline().AddLast("encoder", encoder)

	if err := ch.Write("x"); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 2 || sink.writes[0] != "xa" || sink.writes[1] != "xb" {
		t.Fatalf("writes=%v", sink.writes)
	}
}

func TestByteToMessageListDecoderEmitsMultipleMessages(t *testing.T) {
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	decoder := NewByteToMessageListDecoder(byteListDecoderFunc{
		decode: func(_ *channel.HandlerContext, in *buffer.CompositeByteBuf, out *MessageList) error {
			for in.ReadableBytes() >= 2 {
				frame, err := in.Slice(in.ReaderIndex(), 2)
				if err != nil {
					return err
				}
				if err := in.SkipBytes(2); err != nil {
					frame.Release()
					return err
				}
				out.Add(frame)
			}
			return nil
		},
	})
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("abcdef")))
	if len(collector.frames) != 3 {
		t.Fatalf("frames=%d, want 3", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "ab" || string(collector.frames[1].Bytes()) != "cd" || string(collector.frames[2].Bytes()) != "ef" {
		t.Fatalf("frames=%q", []string{string(collector.frames[0].Bytes()), string(collector.frames[1].Bytes()), string(collector.frames[2].Bytes())})
	}
	collector.release()
}

func TestByteToMessageDecoderRequiresProgressWhenEmitting(t *testing.T) {
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	out := testBuf([]byte("x"))
	decoder := NewByteToMessageDecoder(byteDecoderFunc{
		decode: func(*channel.HandlerContext, *buffer.CompositeByteBuf) (any, error) {
			return out, nil
		},
	})
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("abc")))
	if len(collector.errs) != 1 || !errors.Is(collector.errs[0], ErrDecoderNoProgress) {
		t.Fatalf("errs=%v, want ErrDecoderNoProgress", collector.errs)
	}
	if refs := out.RefCnt(); refs != 0 {
		out.Release()
		t.Fatalf("emitted buffer ref=%d, want 0", refs)
	}
}

func TestMessageToMessageCodec(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	collector := &captureStringInbound{}
	decoder := messageDecoderFunc{
		accept: func(msg any) bool { _, ok := msg.(string); return ok },
		decode: func(_ *channel.HandlerContext, msg any, out *MessageList) error {
			out.Add("in-" + msg.(string))
			return nil
		},
	}
	encoder := messageEncoderFunc{
		accept: func(msg any) bool { _, ok := msg.(int); return ok },
		encode: func(_ *channel.HandlerContext, msg any, out *MessageList) error {
			out.Add(msg.(int) + 1)
			return nil
		},
	}
	_ = ch.Pipeline().AddLast("codec", NewMessageToMessageCodec(decoder, encoder))
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead("x")
	if len(collector.msgs) != 1 || collector.msgs[0] != "in-x" {
		t.Fatalf("in=%v", collector.msgs)
	}
	if err := ch.Write(41); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 1 || sink.writes[0] != 42 {
		t.Fatalf("out=%v", sink.writes)
	}
}

func TestCombinedChannelDuplexHandler(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	collector := &captureStringInbound{}
	encoder := NewMessageToMessageEncoderFunc(
		func(msg any) bool { _, ok := msg.(string); return ok },
		func(_ *channel.HandlerContext, msg any, out *MessageList) error {
			out.Add("out-" + msg.(string))
			return nil
		},
	)
	combined := NewCombinedChannelDuplexHandler(collector, encoder)
	_ = ch.Pipeline().AddLast("combined", combined)

	ch.Pipeline().FireChannelRead("in")
	if len(collector.msgs) != 1 || collector.msgs[0] != "in" {
		t.Fatalf("in=%v", collector.msgs)
	}
	if err := ch.Write("msg"); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 1 || sink.writes[0] != "out-msg" {
		t.Fatalf("out=%v", sink.writes)
	}
}

type byteListDecoderFunc struct {
	decode func(*channel.HandlerContext, *buffer.CompositeByteBuf, *MessageList) error
}

func (f byteListDecoderFunc) DecodeBytes(ctx *channel.HandlerContext, in *buffer.CompositeByteBuf, out *MessageList) error {
	return f.decode(ctx, in, out)
}

type byteDecoderFunc struct {
	decode func(*channel.HandlerContext, *buffer.CompositeByteBuf) (any, error)
}

func (f byteDecoderFunc) Decode(ctx *channel.HandlerContext, in *buffer.CompositeByteBuf) (any, error) {
	return f.decode(ctx, in)
}

type failingOutboundSink struct {
	err    error
	writes []any
}

func (s *failingOutboundSink) Write(msg any) error {
	s.writes = append(s.writes, msg)
	return s.err
}

func (s *failingOutboundSink) Flush() error { return nil }
func (s *failingOutboundSink) Close() error { return nil }
