package codec

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestByteToMessageDecoderMergeCumulatorProducesContiguousFrame(t *testing.T) {
	decoder, err := NewFixedLengthFrameDecoder(4)
	if err != nil {
		t.Fatal(err)
	}
	decoder.SetCumulator(MergeCumulator)
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("decoder", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte("ab")))
	ch.Pipeline().FireChannelRead(testBuf([]byte("cd")))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(collector.frames))
	}
	if got := string(collector.frames[0].Bytes()); got != "abcd" {
		t.Fatalf("frame=%q", got)
	}
	if slices := collector.frames[0].ReadableSlices(nil); len(slices) != 1 {
		t.Fatalf("slice count=%d, want 1 with merge cumulator", len(slices))
	}
	collector.release()
}

func TestByteToMessageDecoderRejectsNilCumulator(t *testing.T) {
	decoder, err := NewFixedLengthFrameDecoder(2)
	if err != nil {
		t.Fatal(err)
	}
	if decoder.SetCumulator(nil) != decoder.ByteToMessageDecoder {
		t.Fatal("SetCumulator should be chainable")
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("decoder", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}
	ch.Pipeline().FireChannelRead(testBuf([]byte("ab")))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want default composite cumulator to remain active", len(collector.frames))
	}
	collector.release()
}
