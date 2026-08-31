package codec

import (
	"errors"
	"testing"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

func TestChunkedWriteHandlerWritesChunksAndFlushes(t *testing.T) {
	sink := &codecOutboundSink{}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("chunked", NewChunkedWriteHandler()); err != nil {
		t.Fatal(err)
	}

	source := testBuf([]byte("abcdef"))
	input, err := NewChunkedByteBufInput(source, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Write(input); err != nil {
		t.Fatal(err)
	}
	if !input.closed {
		t.Fatal("chunked input should be closed after write")
	}
	if len(sink.writes) != 3 {
		t.Fatalf("writes=%d, want 3", len(sink.writes))
	}
	for idx, want := range []string{"ab", "cd", "ef"} {
		buf := sink.writes[idx].(buffer.ByteBuf)
		if got := string(buf.Bytes()); got != want {
			t.Fatalf("chunk[%d]=%q, want %q", idx, got, want)
		}
	}
	if sink.flushes != 1 {
		t.Fatalf("flushes=%d, want 1", sink.flushes)
	}
	if source.RefCnt() != 3 {
		t.Fatalf("source ref after write=%d, want 3 retained chunks", source.RefCnt())
	}
	sink.release()
	if source.RefCnt() != 0 {
		t.Fatalf("source ref after sink release=%d, want 0", source.RefCnt())
	}
}

func TestChunkedByteBufInputRejectsInvalidChunkSizeAndReleasesInput(t *testing.T) {
	source := testBuf([]byte("abc"))
	input, err := NewChunkedByteBufInput(source, 0)
	if err != ErrInvalidFrameLength {
		t.Fatalf("err=%v, want %v", err, ErrInvalidFrameLength)
	}
	if input != nil {
		t.Fatalf("input=%v, want nil", input)
	}
	if source.RefCnt() != 0 {
		t.Fatalf("source ref=%d, want 0", source.RefCnt())
	}
}

func TestChunkedWriteHandlerReleasesChunkWhenWriteFails(t *testing.T) {
	writeErr := errors.New("write failed")
	sink := &codecOutboundSink{writeAt: 1, writeErr: writeErr}
	ch := channel.NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("chunked", NewChunkedWriteHandler()); err != nil {
		t.Fatal(err)
	}

	source := testBuf([]byte("abcd"))
	input, err := NewChunkedByteBufInput(source, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.Write(input); !errors.Is(err, writeErr) {
		t.Fatalf("err=%v, want writeErr", err)
	}
	if !input.closed {
		t.Fatal("chunked input should be closed after write error")
	}
	if source.RefCnt() != 0 {
		t.Fatalf("source ref=%d, want 0", source.RefCnt())
	}
	if sink.flushes != 0 {
		t.Fatalf("flushes=%d, want 0 after write error", sink.flushes)
	}
}
