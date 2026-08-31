package codec

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type replayLengthFrameDecoder struct{}

func (replayLengthFrameDecoder) DecodeReplay(_ *channel.HandlerContext, in *ReplayBuffer) (any, error) {
	length, err := in.ReadUnsigned(2, buffer.BigEndian)
	if err != nil {
		return nil, err
	}
	return in.ReadSlice(int(length))
}

func TestReplayingDecoderWaitsForCompleteFrame(t *testing.T) {
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("decoder", NewReplayingDecoder(replayLengthFrameDecoder{})); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte{0, 3, 'a'}))
	if len(collector.frames) != 0 {
		t.Fatalf("frames=%d, want 0", len(collector.frames))
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte{'b', 'c'}))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(collector.frames))
	}
	frame := collector.frames[0]
	if got := string(frame.Bytes()); got != "abc" {
		t.Fatalf("frame=%q", got)
	}
	if slices := frame.ReadableSlices(nil); len(slices) != 2 {
		t.Fatalf("slice count=%d, want 2 for cross-buffer zero-copy frame", len(slices))
	}
	collector.release()
}

func TestReplayingDecoderDoesNotConsumeOnReplay(t *testing.T) {
	decoder := NewReplayingDecoder(replayLengthFrameDecoder{})
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("decoder", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte{0, 2}))
	ch.Pipeline().FireChannelRead(testBuf([]byte{'o', 'k'}))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(collector.frames))
	}
	if got := string(collector.frames[0].Bytes()); got != "ok" {
		t.Fatalf("frame=%q", got)
	}
	collector.release()
}
