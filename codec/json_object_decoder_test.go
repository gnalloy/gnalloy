package codec

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestJsonObjectDecoderHandlesSplitNestedObject(t *testing.T) {
	decoder, err := NewJsonObjectDecoder(128)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("json", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte(" \r\n{\"a\":[1,{\"b\":\"")))
	ch.Pipeline().FireChannelRead(testBuf([]byte("x{y}\\\"z\"}],\"ok\":true}  ")))
	if len(collector.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(collector.frames))
	}
	if got := string(collector.frames[0].Bytes()); got != "{\"a\":[1,{\"b\":\"x{y}\\\"z\"}],\"ok\":true}" {
		t.Fatalf("frame=%q", got)
	}
	collector.release()
}

func TestJsonObjectDecoderHandlesArrayAndStickyFrames(t *testing.T) {
	decoder, err := NewJsonObjectDecoder(128)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("json", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte("[1,{\"a\":2}]{\"next\":true}")))
	if len(collector.frames) != 2 {
		t.Fatalf("frames=%d, want 2", len(collector.frames))
	}
	if got := string(collector.frames[0].Bytes()); got != "[1,{\"a\":2}]" {
		t.Fatalf("first=%q", got)
	}
	if got := string(collector.frames[1].Bytes()); got != "{\"next\":true}" {
		t.Fatalf("second=%q", got)
	}
	collector.release()
}

func TestJsonObjectDecoderReportsTooLongFrame(t *testing.T) {
	decoder, err := NewJsonObjectDecoder(8)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("json", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte("{\"long\":true}")))
	if len(collector.errs) != 1 {
		t.Fatalf("errs=%d, want 1", len(collector.errs))
	}
	if !errors.Is(collector.errs[0], ErrFrameTooLong) {
		t.Fatalf("err=%v, want ErrFrameTooLong", collector.errs[0])
	}
}

func TestJsonObjectDecoderRejectsInvalidStart(t *testing.T) {
	decoder, err := NewJsonObjectDecoder(32)
	if err != nil {
		t.Fatal(err)
	}
	collector := &frameCollector{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), nil)
	if err := ch.Pipeline().AddLast("json", decoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Pipeline().AddLast("collector", collector); err != nil {
		t.Fatal(err)
	}

	ch.Pipeline().FireChannelRead(testBuf([]byte("x")))
	if len(collector.errs) != 1 || !errors.Is(collector.errs[0], ErrInvalidFrameLength) {
		t.Fatalf("errs=%v", collector.errs)
	}
}
