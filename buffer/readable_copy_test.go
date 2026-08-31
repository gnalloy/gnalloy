package buffer

import "testing"

func TestCopyReadableBytesCopiesDirectWithoutAdvancing(t *testing.T) {
	buf := NewHeapBuffer(8)
	_, _ = buf.WriteBytes([]byte("abcdef"))
	if err := buf.SkipBytes(2); err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	dst := make([]byte, buf.ReadableBytes())
	n := CopyReadableBytes(dst, buf)
	if n != 4 || string(dst) != "cdef" {
		t.Fatalf("n=%d dst=%q, want cdef", n, dst)
	}
	if got := buf.ReaderIndex(); got != 2 {
		t.Fatalf("readerIndex=%d, want 2", got)
	}
}

func TestCopyReadableBytesCopiesCompositeWithoutAdvancing(t *testing.T) {
	first := NewHeapBuffer(4)
	second := NewHeapBuffer(4)
	_, _ = first.WriteBytes([]byte("abcd"))
	_, _ = second.WriteBytes([]byte("efgh"))
	composite := NewCompositeByteBuf()
	composite.Append(first)
	composite.Append(second)
	if err := composite.SkipBytes(3); err != nil {
		t.Fatal(err)
	}
	defer composite.Release()

	dst := make([]byte, composite.ReadableBytes())
	n := CopyReadableBytes(dst, composite)
	if n != 5 || string(dst) != "defgh" {
		t.Fatalf("n=%d dst=%q, want defgh", n, dst)
	}
	if got := composite.ReaderIndex(); got != 3 {
		t.Fatalf("readerIndex=%d, want 3", got)
	}
}

func TestContiguousReadableBytesDetectsDirectAndSingleComposite(t *testing.T) {
	direct := NewHeapBuffer(8)
	_, _ = direct.WriteBytes([]byte("abcd"))
	defer direct.Release()

	if data, ok := ContiguousReadableBytes(direct); !ok || string(data) != "abcd" {
		t.Fatalf("direct=%q ok=%v, want abcd,true", data, ok)
	}

	composite := NewCompositeByteBuf()
	composite.AppendRetained(direct)
	defer composite.Release()
	if data, ok := ContiguousReadableBytes(composite); !ok || string(data) != "abcd" {
		t.Fatalf("single composite=%q ok=%v, want abcd,true", data, ok)
	}

	fragmented := NewCompositeByteBuf()
	fragmented.Append(testBuffer("ab"))
	fragmented.Append(testBuffer("cd"))
	defer fragmented.Release()
	if _, ok := ContiguousReadableBytes(fragmented); ok {
		t.Fatal("fragmented composite should not expose a contiguous view")
	}
}

func TestWriteReadableBytesCopiesCompositeWithoutAdvancing(t *testing.T) {
	src := NewCompositeByteBuf()
	src.Append(testBuffer("ab"))
	src.Append(testBuffer("cd"))
	defer src.Release()

	dst := NewHeapBuffer(4)
	defer dst.Release()
	if err := WriteReadableBytes(dst, src); err != nil {
		t.Fatal(err)
	}
	if got := string(dst.Bytes()); got != "abcd" {
		t.Fatalf("dst=%q, want abcd", got)
	}
	if got := src.ReaderIndex(); got != 0 {
		t.Fatalf("src readerIndex=%d, want 0", got)
	}
}

func TestReadableStringAtCopiesCompositeRangeWithoutAdvancing(t *testing.T) {
	src := NewCompositeByteBuf()
	src.Append(testBuffer("ab"))
	src.Append(testBuffer("cdef"))
	src.Append(testBuffer("gh"))
	defer src.Release()

	value, err := ReadableStringAt(src, 1, 6)
	if err != nil {
		t.Fatal(err)
	}
	if value != "bcdefg" {
		t.Fatalf("value=%q, want bcdefg", value)
	}
	if src.ReaderIndex() != 0 {
		t.Fatalf("readerIndex=%d, want 0", src.ReaderIndex())
	}
	if _, err := ReadableStringAt(src, 7, 2); err != ErrInvalidIndex {
		t.Fatalf("err=%v, want ErrInvalidIndex", err)
	}
	if _, err := ReadableStringAt(src, 0, -1); err != ErrInvalidIndex {
		t.Fatalf("negative length err=%v, want ErrInvalidIndex", err)
	}
	if _, err := ReadableStringAt(src, src.WriterIndex()+1, 0); err != ErrInvalidIndex {
		t.Fatalf("empty out-of-range err=%v, want ErrInvalidIndex", err)
	}
}

func TestForEachReadableSliceVisitsCompositeWithoutAdvancing(t *testing.T) {
	src := NewCompositeByteBuf()
	src.Append(testBuffer("ab"))
	src.Append(testBuffer("cd"))
	defer src.Release()

	var got string
	if !ForEachReadableSlice(src, func(data []byte) bool {
		got += string(data)
		return true
	}) {
		t.Fatal("iteration should complete")
	}
	if got != "abcd" {
		t.Fatalf("got=%q, want abcd", got)
	}
	if src.ReaderIndex() != 0 {
		t.Fatalf("readerIndex=%d, want 0", src.ReaderIndex())
	}
}

func BenchmarkCopyReadableBytesComposite(b *testing.B) {
	src := NewCompositeByteBuf()
	for i := 0; i < 32; i++ {
		src.Append(testBuffer("abcdefghijklmnop"))
	}
	defer src.Release()
	dst := make([]byte, src.ReadableBytes())

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := CopyReadableBytes(dst, src); n != len(dst) {
			b.Fatalf("n=%d, want %d", n, len(dst))
		}
	}
}

func BenchmarkWriteReadableBytesComposite(b *testing.B) {
	src := NewCompositeByteBuf()
	for i := 0; i < 32; i++ {
		src.Append(testBuffer("abcdefghijklmnop"))
	}
	defer src.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := NewHeapBuffer(src.ReadableBytes())
		if err := WriteReadableBytes(dst, src); err != nil {
			b.Fatal(err)
		}
		dst.Release()
	}
}
