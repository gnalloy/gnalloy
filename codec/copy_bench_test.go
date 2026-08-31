package codec

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type releasingSink struct{}

func (releasingSink) Write(msg any) error {
	if buf, ok := msg.(buffer.ByteBuf); ok {
		buf.Release()
	}
	return nil
}
func (releasingSink) Flush() error { return nil }
func (releasingSink) Close() error { return nil }

func BenchmarkByteSliceDecoderComposite(b *testing.B) {
	collector := &captureBytesInbound{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", NewByteSliceDecoder())
	_ = ch.Pipeline().AddLast("collector", collector)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Pipeline().FireChannelRead(fragmentedCodecBuffer("abcd", "efgh", "ijkl", "mnop"))
		if len(collector.msgs) != 1 || len(collector.msgs[0]) != 16 {
			b.Fatalf("msgs=%d", len(collector.msgs))
		}
		collector.msgs = collector.msgs[:0]
	}
}

func BenchmarkStringDecoderComposite(b *testing.B) {
	collector := &captureStringInbound{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", NewStringDecoder())
	_ = ch.Pipeline().AddLast("collector", collector)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Pipeline().FireChannelRead(fragmentedCodecBuffer("abcd", "efgh", "ijkl", "mnop"))
		if len(collector.msgs) != 1 || len(collector.msgs[0]) != 16 {
			b.Fatalf("msgs=%d", len(collector.msgs))
		}
		collector.msgs = collector.msgs[:0]
	}
}

func BenchmarkBase64EncoderComposite(b *testing.B) {
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), releasingSink{})
	_ = ch.Pipeline().AddLast("encoder", NewBase64Encoder())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ch.Write(fragmentedCodecBuffer("abcd", "efgh", "ijkl", "mnop")); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBase64DecoderComposite(b *testing.B) {
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", NewBase64Decoder())
	_ = ch.Pipeline().AddLast("collector", collector)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.Pipeline().FireChannelRead(fragmentedCodecBuffer("YWJj", "ZGVm", "Z2hp", "amts"))
		if len(collector.frames) != 1 || collector.frames[0].ReadableBytes() != 12 {
			b.Fatalf("frames=%d", len(collector.frames))
		}
		collector.release()
	}
}

func fragmentedCodecBuffer(parts ...string) buffer.ByteBuf {
	c := buffer.NewCompositeByteBuf()
	for _, part := range parts {
		c.Append(testBuf([]byte(part)))
	}
	return c
}
