package codec

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestFixedLengthFrameDecoderSplitAndSticky(t *testing.T) {
	decoder, err := NewFixedLengthFrameDecoder(2)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("abcde")))
	if len(collector.frames) != 2 {
		t.Fatalf("frames=%d, want 2", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "ab" || string(collector.frames[1].Bytes()) != "cd" {
		t.Fatalf("frames=%q,%q", collector.frames[0].Bytes(), collector.frames[1].Bytes())
	}
	collector.release()

	ch.Pipeline().FireChannelRead(testBuf([]byte("f")))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "ef" {
		t.Fatalf("frame=%q", collector.frames[0].Bytes())
	}
	collector.release()
}

func TestLineBasedFrameDecoderSplitCRLFAndLF(t *testing.T) {
	decoder, err := NewLineBasedFrameDecoder(32)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("hello\r\nwor")))
	ch.Pipeline().FireChannelRead(testBuf([]byte("ld\n")))
	if len(collector.frames) != 2 {
		t.Fatalf("frames=%d, want 2", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "hello" || string(collector.frames[1].Bytes()) != "world" {
		t.Fatalf("frames=%q,%q", collector.frames[0].Bytes(), collector.frames[1].Bytes())
	}
	collector.release()
}

func TestLineBasedFrameDecoderKeepsDelimiterWhenConfigured(t *testing.T) {
	decoder, err := NewLineBasedFrameDecoderWithOptions(32, false, true)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("ok\r\n")))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "ok\r\n" {
		t.Fatalf("frame=%q", collector.frames[0].Bytes())
	}
	collector.release()
}

func TestLineBasedFrameDecoderTooLong(t *testing.T) {
	decoder, err := NewLineBasedFrameDecoder(3)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("abcd")))
	if len(collector.errs) != 1 || collector.errs[0] != ErrFrameTooLong {
		t.Fatalf("errs=%v", collector.errs)
	}
}

func TestLineBasedFrameDecoderFailSlowReportsAfterDelimiter(t *testing.T) {
	decoder, err := NewLineBasedFrameDecoderWithOptions(3, true, false)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("abcd")))
	if len(collector.errs) != 0 {
		t.Fatalf("errs=%v", collector.errs)
	}
	ch.Pipeline().FireChannelRead(testBuf([]byte("\n")))
	if len(collector.errs) != 1 || collector.errs[0] != ErrFrameTooLong {
		t.Fatalf("errs=%v", collector.errs)
	}
	if len(collector.frames) != 0 {
		t.Fatalf("frames=%d, want 0", len(collector.frames))
	}
}

func TestDelimiterBasedFrameDecoderAcrossBuffers(t *testing.T) {
	decoder, err := NewDelimiterBasedFrameDecoder(32, true, true, []byte("<END>"))
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("abc<EN")))
	ch.Pipeline().FireChannelRead(testBuf([]byte("D>def<END>")))
	if len(collector.frames) != 2 {
		t.Fatalf("frames=%d, want 2", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "abc" || string(collector.frames[1].Bytes()) != "def" {
		t.Fatalf("frames=%q,%q", collector.frames[0].Bytes(), collector.frames[1].Bytes())
	}
	collector.release()
}

func TestDelimiterBasedFrameDecoderChoosesEarliestFrame(t *testing.T) {
	decoder, err := NewDelimiterBasedFrameDecoder(32, true, true, []byte("|"), []byte("<END>"))
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("abc<END>def|")))
	if len(collector.frames) != 2 {
		t.Fatalf("frames=%d, want 2", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "abc" || string(collector.frames[1].Bytes()) != "def" {
		t.Fatalf("frames=%q,%q", collector.frames[0].Bytes(), collector.frames[1].Bytes())
	}
	collector.release()
}

func TestDelimiterBasedFrameDecoderKeepsDelimiterWhenConfigured(t *testing.T) {
	decoder, err := NewDelimiterBasedFrameDecoder(32, false, true, []byte("|"))
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	_ = ch.Pipeline().AddLast("decoder", decoder)
	_ = ch.Pipeline().AddLast("collector", collector)

	ch.Pipeline().FireChannelRead(testBuf([]byte("abc|")))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(collector.frames))
	}
	if string(collector.frames[0].Bytes()) != "abc|" {
		t.Fatalf("frame=%q", collector.frames[0].Bytes())
	}
	collector.release()
}
