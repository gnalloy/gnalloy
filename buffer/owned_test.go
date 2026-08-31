package buffer

import "testing"

func TestNewOwnedBufferReleasesOwnerOnce(t *testing.T) {
	released := 0
	owner := []byte("owned")
	buf := NewOwnedBuffer(owner, func(data []byte) {
		released++
		if string(data) != "owned" {
			t.Fatalf("released data=%q, want owned", data)
		}
	})
	if got := string(buf.Bytes()); got != "owned" {
		t.Fatalf("bytes=%q, want owned", got)
	}
	if buf.Release() {
		if released != 1 {
			t.Fatalf("released=%d, want 1", released)
		}
		return
	}
	t.Fatal("owned buffer release did not drop final reference")
}

func TestNewOwnedBufferRetainedSliceKeepsOwner(t *testing.T) {
	released := 0
	buf := NewOwnedBuffer([]byte("abcdef"), func([]byte) {
		released++
	})
	part, err := buf.Slice(1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(part.Bytes()); got != "bcd" {
		t.Fatalf("slice=%q, want bcd", got)
	}
	if buf.Release() {
		t.Fatal("parent should stay alive while slice is retained")
	}
	if released != 0 {
		t.Fatalf("released=%d before slice release", released)
	}
	if !part.Release() {
		t.Fatal("slice release did not drop final reference")
	}
	if released != 1 {
		t.Fatalf("released=%d, want 1", released)
	}
}
