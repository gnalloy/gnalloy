package buffer

import (
	"bytes"
	"errors"
	"testing"
)

func TestNewSharedBufferReadsWithoutCopying(t *testing.T) {
	data := []byte("shared")
	buf := NewSharedBuffer(data)
	defer buf.Release()

	if got := string(buf.Bytes()); got != "shared" {
		t.Fatalf("bytes=%q, want shared", got)
	}
	data[0] = 'S'
	if got := string(buf.Bytes()); got != "Shared" {
		t.Fatalf("bytes=%q, want Shared", got)
	}
}

func TestSharedBufferRejectsWrites(t *testing.T) {
	buf := NewSharedBuffer([]byte("readonly"))
	defer buf.Release()

	if _, err := buf.WriteBytes([]byte("x")); !errors.Is(err, ErrNoWritableBytes) {
		t.Fatalf("WriteBytes err=%v, want ErrNoWritableBytes", err)
	}
	if _, err := buf.ReadFrom(bytes.NewReader([]byte("x"))); !errors.Is(err, ErrNoWritableBytes) {
		t.Fatalf("ReadFrom err=%v, want ErrNoWritableBytes", err)
	}
	if err := buf.AdvanceWriter(1); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("AdvanceWriter err=%v, want ErrInvalidIndex", err)
	}
}

func TestSharedBufferSliceHasIndependentIndexes(t *testing.T) {
	parent := NewSharedBuffer([]byte("abcdef"))
	part, err := parent.Slice(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Release()
	defer part.Release()

	if got := string(part.Bytes()); got != "bcd" {
		t.Fatalf("slice=%q, want bcd", got)
	}
	if err := part.SkipBytes(1); err != nil {
		t.Fatal(err)
	}
	if got := string(part.Bytes()); got != "cd" {
		t.Fatalf("slice after skip=%q, want cd", got)
	}
	if got := string(parent.Bytes()); got != "abcdef" {
		t.Fatalf("parent=%q, want abcdef", got)
	}
}

func TestSharedBufferContiguousReadableBytes(t *testing.T) {
	buf := NewSharedBuffer([]byte("abcdef"))
	defer buf.Release()

	if err := buf.SkipBytes(2); err != nil {
		t.Fatal(err)
	}
	data, ok := ContiguousReadableBytes(buf)
	if !ok {
		t.Fatal("shared buffer should expose contiguous readable bytes")
	}
	if got := string(data); got != "cdef" {
		t.Fatalf("contiguous=%q, want cdef", got)
	}
}
