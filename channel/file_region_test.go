package channel

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"gnalloy.org/gnalloy/buffer"
)

func TestFileRegionReadsConfiguredRange(t *testing.T) {
	region, err := NewFileRegion(strings.NewReader("0123456789"), 2, 4)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 3)
	n, err := region.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || string(buf[:n]) != "234" {
		t.Fatalf("n=%d data=%q", n, buf[:n])
	}
	n, err = region.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || string(buf[:n]) != "5" {
		t.Fatalf("n=%d data=%q", n, buf[:n])
	}
	n, err = region.Read(buf)
	if !errors.Is(err, io.EOF) || n != 0 {
		t.Fatalf("n=%d err=%v, want EOF", n, err)
	}
	if region.Transferred() != 4 {
		t.Fatalf("transferred=%d, want 4", region.Transferred())
	}
	if err := region.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := region.Read(buf); !errors.Is(err, ErrFileRegionClosed) {
		t.Fatalf("err=%v, want %v", err, ErrFileRegionClosed)
	}
}

func TestFileRegionExposesNativeSourceAndAdvance(t *testing.T) {
	reader := strings.NewReader("0123456789")
	region, err := NewFileRegion(reader, 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if region.ReaderAt() != reader || region.Offset() != 3 {
		t.Fatalf("reader=%v offset=%d", region.ReaderAt(), region.Offset())
	}
	if err := region.Advance(2); err != nil {
		t.Fatal(err)
	}
	if region.Transferred() != 2 {
		t.Fatalf("transferred=%d, want 2", region.Transferred())
	}
	if err := region.Advance(3); !errors.Is(err, ErrInvalidFileRegion) {
		t.Fatalf("err=%v, want invalid region", err)
	}
	if err := region.Close(); err != nil {
		t.Fatal(err)
	}
	if err := region.Advance(1); !errors.Is(err, ErrFileRegionClosed) {
		t.Fatalf("err=%v, want closed region", err)
	}
}

func TestFileRegionEncoderWritesChunks(t *testing.T) {
	region, err := NewFileRegion(strings.NewReader("abcdefgh"), 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewFileRegionEncoder(3)
	if err != nil {
		t.Fatal(err)
	}
	sink := &fileRegionSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("file", encoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Write(region); err != nil {
		t.Fatal(err)
	}
	got := sink.strings()
	want := []string{"abc", "def", "gh"}
	if len(got) != len(want) {
		t.Fatalf("chunks=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("chunks=%v want=%v", got, want)
		}
	}
	if region.Transferred() != 8 {
		t.Fatalf("transferred=%d, want 8", region.Transferred())
	}
	if _, err := region.Read(make([]byte, 1)); !errors.Is(err, ErrFileRegionClosed) {
		t.Fatalf("err=%v, want closed region", err)
	}
	sink.release()
}

func TestFileRegionEncoderIgnoresEmptyRegion(t *testing.T) {
	region, err := NewFileRegion(strings.NewReader("abc"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	encoder, err := NewFileRegionEncoder(3)
	if err != nil {
		t.Fatal(err)
	}
	sink := &fileRegionSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("file", encoder); err != nil {
		t.Fatal(err)
	}
	if err := ch.Write(region); err != nil {
		t.Fatal(err)
	}
	if len(sink.writes) != 0 {
		t.Fatalf("writes=%d, want 0", len(sink.writes))
	}
}

func BenchmarkFileRegionEncoderChunks(b *testing.B) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 4096)
	encoder, err := NewFileRegionEncoder(32 << 10)
	if err != nil {
		b.Fatal(err)
	}
	sink := &benchmarkFileRegionSink{}
	ch := NewLocalChannel(1, buffer.NewHeapAllocator(), sink)
	if err := ch.Pipeline().AddLast("file", encoder); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		region, err := NewFileRegion(bytes.NewReader(payload), 0, int64(len(payload)))
		if err != nil {
			b.Fatal(err)
		}
		if err := ch.Write(region); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	want := int64(len(payload)) * int64(b.N)
	if sink.bytes != want {
		b.Fatalf("bytes=%d, want %d", sink.bytes, want)
	}
}

type fileRegionSink struct {
	writes []buffer.ByteBuf
}

func (s *fileRegionSink) Write(msg any) error {
	buf, ok := msg.(buffer.ByteBuf)
	if !ok {
		return ErrInvalidMessage
	}
	s.writes = append(s.writes, buf)
	return nil
}

func (s *fileRegionSink) Flush() error {
	return nil
}

func (s *fileRegionSink) Close() error {
	return nil
}

func (s *fileRegionSink) strings() []string {
	out := make([]string, 0, len(s.writes))
	for _, buf := range s.writes {
		out = append(out, string(buf.Bytes()))
	}
	return out
}

func (s *fileRegionSink) release() {
	for _, buf := range s.writes {
		if buf != nil {
			buf.Release()
		}
	}
}

type benchmarkFileRegionSink struct {
	bytes int64
}

func (s *benchmarkFileRegionSink) Write(msg any) error {
	buf, ok := msg.(buffer.ByteBuf)
	if !ok {
		return ErrInvalidMessage
	}
	s.bytes += int64(buf.ReadableBytes())
	buf.Release()
	return nil
}

func (s *benchmarkFileRegionSink) Flush() error {
	return nil
}

func (s *benchmarkFileRegionSink) Close() error {
	return nil
}
