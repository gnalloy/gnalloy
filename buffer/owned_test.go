package buffer

import "testing"

type trackingOwnedBufferReleaser struct {
	released int
	data     []byte
}

func (r *trackingOwnedBufferReleaser) Release(data []byte) {
	r.released++
	r.data = data
}

type discardOwnedBufferReleaser struct{}

func (discardOwnedBufferReleaser) Release([]byte) {}

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

func TestNewOwnedBufferWithReleaserReleasesOwnerOnce(t *testing.T) {
	releaser := &trackingOwnedBufferReleaser{}
	owner := []byte("owned")
	buf := NewOwnedBufferWithReleaser(owner, releaser)
	if got := string(buf.Bytes()); got != "owned" {
		t.Fatalf("bytes=%q, want owned", got)
	}
	if !buf.Release() {
		t.Fatal("owned buffer release did not drop final reference")
	}
	if releaser.released != 1 {
		t.Fatalf("released=%d, want 1", releaser.released)
	}
	if string(releaser.data) != "owned" {
		t.Fatalf("released data=%q, want owned", releaser.data)
	}
}

func TestNewOwnedBufferWithReleaserRoundTripDoesNotAllocate(t *testing.T) {
	releaser := discardOwnedBufferReleaser{}
	buf := NewOwnedBufferWithReleaser([]byte("warmup"), releaser)
	buf.Release()
	data := []byte("payload")

	allocs := testing.AllocsPerRun(1000, func() {
		buf := NewOwnedBufferWithReleaser(data, releaser)
		buf.Release()
	})
	if allocs != 0 {
		t.Fatalf("allocs=%f, want 0", allocs)
	}
}

func BenchmarkNewOwnedBufferWithReleaserRoundTrip(b *testing.B) {
	releaser := discardOwnedBufferReleaser{}
	data := []byte("payload")
	buf := NewOwnedBufferWithReleaser(data, releaser)
	buf.Release()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		buf := NewOwnedBufferWithReleaser(data, releaser)
		buf.Release()
	}
}
