package buffer

import "testing"

func TestHexDumpUsesReadableSlices(t *testing.T) {
	a := NewHeapBuffer(4)
	b := NewHeapBuffer(4)
	_, _ = a.WriteBytes([]byte{0x00, 0x0f})
	_, _ = b.WriteBytes([]byte{0x10, 0xff})

	c := NewCompositeByteBuf()
	c.Append(a)
	c.Append(b)
	defer c.Release()

	if got := HexDump(c); got != "000f10ff" {
		t.Fatalf("hex=%q", got)
	}
	if got := HexDumpRange(c, 1, 2); got != "0f10" {
		t.Fatalf("range hex=%q", got)
	}
}

func TestByteBufEqualAndCompare(t *testing.T) {
	a := NewHeapBuffer(8)
	b := NewHeapBuffer(8)
	c := NewHeapBuffer(8)
	_, _ = a.WriteBytes([]byte("abc"))
	_, _ = b.WriteBytes([]byte("abc"))
	_, _ = c.WriteBytes([]byte("abd"))
	defer a.Release()
	defer b.Release()
	defer c.Release()

	if !ByteBufEqual(a, b) {
		t.Fatal("a and b should be equal")
	}
	if ByteBufEqual(a, c) {
		t.Fatal("a and c should not be equal")
	}
	if got := ByteBufCompare(a, b); got != 0 {
		t.Fatalf("compare equal=%d, want 0", got)
	}
	if got := ByteBufCompare(a, c); got >= 0 {
		t.Fatalf("compare a<c=%d, want negative", got)
	}
	if got := ByteBufCompare(c, a); got <= 0 {
		t.Fatalf("compare c>a=%d, want positive", got)
	}
	if ByteBufHashCode(a) != ByteBufHashCode(b) {
		t.Fatal("equal buffers should have equal hash")
	}
}

func TestIndexOfByteForwardAndReverse(t *testing.T) {
	buf := NewHeapBuffer(16)
	_, _ = buf.WriteBytes([]byte("abacad"))
	defer buf.Release()

	if got := IndexOfByte(buf, buf.ReaderIndex(), buf.WriterIndex(), 'a'); got != 0 {
		t.Fatalf("forward=%d, want 0", got)
	}
	if got := IndexOfByte(buf, buf.WriterIndex(), buf.ReaderIndex(), 'a'); got != 4 {
		t.Fatalf("reverse=%d, want 4", got)
	}
	if got := IndexOfByte(buf, 0, buf.WriterIndex(), 'z'); got != -1 {
		t.Fatalf("missing=%d, want -1", got)
	}
}

func TestIndexOfBytesAndBytesBefore(t *testing.T) {
	buf := NewHeapBuffer(16)
	_, _ = buf.WriteBytes([]byte("abc<END>def"))
	defer buf.Release()

	if got := IndexOfBytes(buf, buf.ReaderIndex(), buf.WriterIndex(), []byte("<END>")); got != 3 {
		t.Fatalf("index=%d, want 3", got)
	}
	if got := BytesBefore(buf, buf.ReaderIndex(), buf.WriterIndex(), []byte("<END>")); got != 3 {
		t.Fatalf("before=%d, want 3", got)
	}
	if got := IndexOfBytes(buf, buf.ReaderIndex(), buf.WriterIndex(), []byte("<MISS>")); got != -1 {
		t.Fatalf("missing=%d, want -1", got)
	}
}
