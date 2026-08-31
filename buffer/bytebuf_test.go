package buffer

import "testing"

func TestDirectByteBufSliceRetainsParent(t *testing.T) {
	buf := NewHeapBuffer(16)
	if _, err := buf.WriteBytes([]byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	slice, err := buf.Slice(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if buf.RefCnt() != 2 {
		t.Fatalf("parent ref=%d, want 2", buf.RefCnt())
	}
	if string(slice.Bytes()) != "bcd" {
		t.Fatalf("slice=%q", slice.Bytes())
	}
	if buf.Release() {
		t.Fatal("parent should still be retained by slice")
	}
	if string(slice.Bytes()) != "bcd" {
		t.Fatalf("slice after parent release=%q", slice.Bytes())
	}
	if !slice.Release() {
		t.Fatal("slice release should drop slice itself")
	}
}

func TestCompositeSliceAcrossComponents(t *testing.T) {
	a := NewHeapBuffer(8)
	b := NewHeapBuffer(8)
	_, _ = a.WriteBytes([]byte("abc"))
	_, _ = b.WriteBytes([]byte("def"))

	c := NewCompositeByteBuf()
	c.Append(a)
	c.Append(b)

	frame, err := c.Slice(2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if string(frame.Bytes()) != "cde" {
		t.Fatalf("frame=%q", frame.Bytes())
	}
	if err := c.SkipBytes(3); err != nil {
		t.Fatal(err)
	}
	c.DiscardReadComponents()
	if c.ReaderIndex() != 0 || c.ReadableBytes() != 3 {
		t.Fatalf("reader=%d readable=%d", c.ReaderIndex(), c.ReadableBytes())
	}
	frame.Release()
	c.Release()
}

func TestReadUnsignedWithoutCopy(t *testing.T) {
	a := NewHeapBuffer(2)
	b := NewHeapBuffer(2)
	_, _ = a.WriteBytes([]byte{0x01, 0x02})
	_, _ = b.WriteBytes([]byte{0x03, 0x04})

	c := NewCompositeByteBuf()
	c.Append(a)
	c.Append(b)
	got, err := c.ReadUnsigned(1, 3, BigEndian)
	if err != nil {
		t.Fatal(err)
	}
	if got != 0x020304 {
		t.Fatalf("got=%x", got)
	}
	c.Release()
}

func TestCompositeAppendRetainedKeepsCallerOwnership(t *testing.T) {
	buf := NewHeapBuffer(8)
	_, _ = buf.WriteBytes([]byte("abc"))

	c := NewCompositeByteBuf()
	c.AppendRetained(buf)
	if buf.RefCnt() != 2 {
		t.Fatalf("ref=%d, want 2", buf.RefCnt())
	}
	if c.ComponentCount() != 1 || !c.IsContiguous() {
		t.Fatalf("components=%d contiguous=%v", c.ComponentCount(), c.IsContiguous())
	}
	c.Release()
	if buf.RefCnt() != 1 {
		t.Fatalf("ref after composite release=%d, want 1", buf.RefCnt())
	}
	buf.Release()
}

func TestCompositeReadableSlicesAreViews(t *testing.T) {
	a := NewHeapBuffer(8)
	b := NewHeapBuffer(8)
	_, _ = a.WriteBytes([]byte("ab"))
	_, _ = b.WriteBytes([]byte("cd"))

	c := NewCompositeByteBuf()
	c.Append(a)
	c.Append(b)
	slices := c.ReadableSlices(nil)
	if len(slices) != 2 {
		t.Fatalf("slices=%d, want 2", len(slices))
	}
	slices[0][0] = 'A'
	slices[1][1] = 'D'
	if got := string(c.Bytes()); got != "AbcD" {
		t.Fatalf("bytes=%q", got)
	}
	c.Release()
}

func TestCompositeReadableSlicesPreservePartialViews(t *testing.T) {
	a := NewHeapBuffer(8)
	b := NewHeapBuffer(8)
	_, _ = a.WriteBytes([]byte("ab"))
	_, _ = b.WriteBytes([]byte("cd"))

	c := NewCompositeByteBuf()
	c.Append(a)
	c.Append(b)
	if err := c.SkipBytes(1); err != nil {
		t.Fatal(err)
	}
	slices := c.ReadableSlices(nil)
	if len(slices) != 2 || string(slices[0]) != "b" || string(slices[1]) != "cd" {
		t.Fatalf("slices=%q", slices)
	}
	slices[0][0] = 'B'
	if got := string(c.Bytes()); got != "Bcd" {
		t.Fatalf("bytes=%q, want Bcd", got)
	}
	c.Release()
}

func TestCompositeReadableSpan(t *testing.T) {
	c := NewCompositeByteBuf()
	c.Append(testBuffer("ping"))
	c.Append(testBuffer("pong"))
	defer c.Release()

	span, ok := c.ReadableSpan(1, 2)
	if !ok || string(span) != "in" {
		t.Fatalf("span=%q ok=%v", span, ok)
	}
	if _, ok := c.ReadableSpan(3, 2); ok {
		t.Fatal("cross-component span unexpectedly succeeded")
	}
	if err := c.SkipBytes(4); err != nil {
		t.Fatal(err)
	}
	span, ok = c.ReadableSpan(4, 4)
	if !ok || string(span) != "pong" {
		t.Fatalf("span=%q ok=%v", span, ok)
	}
}

func BenchmarkCompositeReadableSlicesFullComponents(b *testing.B) {
	a := NewHeapBuffer(8)
	c := NewCompositeByteBuf()
	_, _ = a.WriteBytes([]byte("abcd"))
	c.Append(a)
	var stack [4][]byte

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		slices := c.ReadableSlices(stack[:0])
		if len(slices) != 1 || len(slices[0]) != 4 {
			b.Fatalf("slices=%d", len(slices))
		}
	}
	c.Release()
}

func BenchmarkCompositeReadableSlicesPartialComponents(b *testing.B) {
	c := NewCompositeByteBuf()
	for i := 0; i < 8; i++ {
		c.Append(testBuffer("abcdefghijklmnop"))
	}
	if err := c.SkipBytes(1); err != nil {
		b.Fatal(err)
	}
	var stack [8][]byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slices := c.ReadableSlices(stack[:0])
		if len(slices) != 8 || len(slices[0]) != 15 || len(slices[7]) != 16 {
			b.Fatalf("slices=%d first=%d last=%d", len(slices), len(slices[0]), len(slices[7]))
		}
	}
	c.Release()
}

func TestCompositeIndexFindsValuesAcrossComponents(t *testing.T) {
	c := NewCompositeByteBuf()
	c.Append(testBuffer("ab"))
	c.Append(testBuffer("cd"))
	c.Append(testBuffer("ef"))
	defer c.Release()

	if index, ok := c.IndexByte(0, 'd'); !ok || index != 3 {
		t.Fatalf("IndexByte=%d,%t, want 3,true", index, ok)
	}
	if index, ok := c.Index(0, []byte("cde")); !ok || index != 2 {
		t.Fatalf("Index=%d,%t, want 2,true", index, ok)
	}
	if _, ok := c.Index(0, []byte("xyz")); ok {
		t.Fatal("Index should not find missing delimiter")
	}
}

func BenchmarkCompositeGetByteFragmented(b *testing.B) {
	c := NewCompositeByteBuf()
	for i := 0; i < 32; i++ {
		c.Append(testBuffer("abcdefghijklmnop"))
	}
	defer c.Release()

	b.ReportAllocs()
	b.ResetTimer()
	var v byte
	for i := 0; i < b.N; i++ {
		pos := i & (c.ReadableBytes() - 1)
		value, ok := c.GetByte(pos)
		if !ok {
			b.Fatal("missing byte")
		}
		v ^= value
	}
	_ = v
}

func BenchmarkCompositeIndexByteFragmented(b *testing.B) {
	c := NewCompositeByteBuf()
	for i := 0; i < 31; i++ {
		c.Append(testBuffer("aaaaaaaaaaaaaaaa"))
	}
	c.Append(testBuffer("aaaaaaaazaaaaaaa"))
	defer c.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if index, ok := c.IndexByte(0, 'z'); !ok || index != 504 {
			b.Fatalf("IndexByte=%d,%t, want 504,true", index, ok)
		}
	}
}

func TestCompositeAppendAfterReleaseDropsInput(t *testing.T) {
	c := NewCompositeByteBuf()
	c.Release()

	buf := NewHeapBuffer(4)
	_, _ = buf.WriteBytes([]byte("x"))
	c.Append(buf)
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want 0", buf.RefCnt())
	}
}

func testBuffer(value string) ByteBuf {
	buf := NewHeapBuffer(len(value))
	_, _ = buf.WriteBytes([]byte(value))
	return buf
}
