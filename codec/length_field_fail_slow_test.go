package codec

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestLengthFieldDecoderFailSlowReportsAfterDiscardingWholeFrame(t *testing.T) {
	decoder, err := NewLengthFieldBasedFrameDecoderWithOptions(4, 0, 4, 0, 4, buffer.BigEndian, false)
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

	ch.Pipeline().FireChannelRead(testBuf([]byte{0, 0, 0, 6, 'a'}))
	if len(collector.errs) != 0 {
		t.Fatalf("errs=%v, want no fail-slow error until frame is discarded", collector.errs)
	}
	ch.Pipeline().FireChannelRead(testBuf([]byte("bcdef")))
	if len(collector.errs) != 1 || !errors.Is(collector.errs[0], ErrFrameTooLong) {
		t.Fatalf("errs=%v, want ErrFrameTooLong", collector.errs)
	}
	if len(collector.frames) != 0 {
		t.Fatalf("frames=%d, want 0", len(collector.frames))
	}
}

func TestLengthFieldDecoderFailFastReportsImmediately(t *testing.T) {
	decoder, err := NewLengthFieldBasedFrameDecoderWithOptions(4, 0, 4, 0, 4, buffer.BigEndian, true)
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

	ch.Pipeline().FireChannelRead(testBuf([]byte{0, 0, 0, 6, 'a'}))
	if len(collector.errs) != 1 || !errors.Is(collector.errs[0], ErrFrameTooLong) {
		t.Fatalf("errs=%v, want immediate ErrFrameTooLong", collector.errs)
	}
}
