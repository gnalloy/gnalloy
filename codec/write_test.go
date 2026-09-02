package codec

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type combinedWriteSink struct {
	err     error
	message any
	calls   int
}

type combinedBufferWriter struct{}

func (combinedBufferWriter) WriteAndFlush(ctx *channel.HandlerContext, message any) error {
	out, ok := message.(buffer.ByteBuf)
	if !ok {
		return ctx.WriteAndFlush(message)
	}
	return WriteOutboundBufferAndFlush(ctx, out)
}

func (s *combinedWriteSink) Write(any) error { return errors.New("unexpected split write") }
func (s *combinedWriteSink) Flush() error    { return errors.New("unexpected split flush") }
func (s *combinedWriteSink) Close() error    { return nil }

func (s *combinedWriteSink) WriteAndFlush(message any) error {
	s.calls++
	s.message = message
	return s.err
}

func TestWriteOutboundBufferAndFlushUsesCombinedSink(t *testing.T) {
	sink := &combinedWriteSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("writer", combinedBufferWriter{}); err != nil {
		t.Fatal(err)
	}
	out := buffer.NewHeapBuffer(1)

	if err := ch.WriteAndFlush(out); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 1 || sink.message != out {
		t.Fatalf("calls=%d message=%T, want one combined write", sink.calls, sink.message)
	}
	out.Release()
}

func TestWriteOutboundBufferAndFlushReleasesOnFailure(t *testing.T) {
	wantErr := errors.New("write failed")
	sink := &combinedWriteSink{err: wantErr}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("writer", combinedBufferWriter{}); err != nil {
		t.Fatal(err)
	}
	out := buffer.NewHeapBuffer(1)

	if err := ch.WriteAndFlush(out); !errors.Is(err, wantErr) {
		t.Fatalf("err=%v, want %v", err, wantErr)
	}
	if refs := out.RefCnt(); refs != 0 {
		t.Fatalf("ref=%d, want 0", refs)
	}
}
