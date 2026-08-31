package codec

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type frameCollector struct {
	frames []buffer.ByteBuf
	errs   []error
}

func (c *frameCollector) ChannelRead(_ *channel.HandlerContext, msg any) {
	if frame, ok := msg.(buffer.ByteBuf); ok {
		c.frames = append(c.frames, frame)
	}
}

func (c *frameCollector) ExceptionCaught(_ *channel.HandlerContext, err error) {
	c.errs = append(c.errs, err)
}

func TestLengthFieldDecoderSplitFrame(t *testing.T) {
	decoder, err := NewLengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("decoder", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte{0, 0, 0, 5, 'h'}))
	if len(collector.frames) != 0 {
		t.Fatal("split frame should not emit")
	}
	ch.Pipeline().FireChannelRead(testBuf([]byte{'e', 'l', 'l', 'o'}))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "hello" {
		t.Fatalf("frame=%q", collector.frames[0].Bytes())
	}
	collector.release()
}

func TestLengthFieldDecoderMergesFragmentedDefaultCumulation(t *testing.T) {
	decoder, err := NewLengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("decoder", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	for _, b := range []byte{0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'} {
		ch.Pipeline().FireChannelRead(testBuf([]byte{b}))
	}
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d", len(collector.frames))
	}
	if got := string(collector.frames[0].Bytes()); got != "hello" {
		t.Fatalf("frame=%q", got)
	}
	if slices := collector.frames[0].ReadableSlices(nil); len(slices) != 1 {
		t.Fatalf("slice count=%d, want 1", len(slices))
	}
	collector.release()
}

func TestLengthFieldDecoderStickyFrames(t *testing.T) {
	decoder, err := NewLengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte{
		0, 0, 0, 4, 't', 'e', 's', 't',
		0, 0, 0, 3, 'a', 'b', 'c',
	}))
	if len(collector.frames) != 2 {
		t.Fatalf("frames=%d", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "test" || string(collector.frames[1].Bytes()) != "abc" {
		t.Fatalf("frames=%q,%q", collector.frames[0].Bytes(), collector.frames[1].Bytes())
	}
	collector.release()
}

func TestLengthFieldDecoderTooLong(t *testing.T) {
	decoder, err := NewLengthFieldBasedFrameDecoder(4, 0, 4, 0, 4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte{0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'}))
	if len(collector.errs) != 1 || !errors.Is(collector.errs[0], ErrFrameTooLong) {
		t.Fatalf("errs=%v", collector.errs)
	}
}

func TestLengthFieldDecoderThreeByteLittleEndianWithOffset(t *testing.T) {
	decoder, err := NewLengthFieldBasedFrameDecoder(1024, 1, 3, 0, 4, buffer.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte{0x7e, 2, 0, 0, 'o', 'k'}))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "ok" {
		t.Fatalf("frame=%q", collector.frames[0].Bytes())
	}
	collector.release()
}

func TestLengthFieldDecoderLengthIncludesHeaderViaAdjustment(t *testing.T) {
	decoder, err := NewLengthFieldBasedFrameDecoder(1024, 0, 4, -4, 4, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte{0, 0, 0, 6, 'o', 'k'}))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "ok" {
		t.Fatalf("frame=%q", collector.frames[0].Bytes())
	}
	collector.release()
}

func TestLengthFieldPrependerRejectsOverflowAndReleasesInput(t *testing.T) {
	prepender, err := NewLengthFieldPrepender(1, buffer.BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), &codecOutboundSink{})
	_ = ch.Pipeline().AddLast("prepender", prepender)

	payload := buffer.NewHeapBuffer(256)
	if _, err := payload.WriteBytes(make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := ch.Write(payload); err != ErrEncodedLengthRange {
		t.Fatalf("err=%v, want %v", err, ErrEncodedLengthRange)
	}
	if payload.RefCnt() != 0 {
		t.Fatalf("ref=%d, want 0", payload.RefCnt())
	}
}

func (c *frameCollector) release() {
	for _, frame := range c.frames {
		frame.Release()
	}
	c.frames = c.frames[:0]
}

func testBuf(data []byte) buffer.ByteBuf {
	buf := buffer.NewHeapBuffer(len(data))
	_, _ = buf.WriteBytes(data)
	return buf
}

func BenchmarkLengthFieldDecoder(b *testing.B) {
	decoder, err := NewLengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4, buffer.BigEndian)
	if err != nil {
		b.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	payload := []byte{0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o'}
	alloc := buffer.NewHeapAllocator()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, err := alloc.Acquire(len(payload))
		if err != nil {
			b.Fatal(err)
		}
		_, _ = buf.WriteBytes(payload)
		ch.Pipeline().FireChannelRead(buf)
		collector.release()
	}
}
