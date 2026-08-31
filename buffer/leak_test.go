package buffer

import "testing"

func TestLeakDetectorTracksUnreleasedDirectBuffer(t *testing.T) {
	ResetLeakDetection()
	EnableLeakDetection(true)
	defer func() {
		EnableLeakDetection(false)
		ResetLeakDetection()
	}()

	buf := NewHeapBuffer(8)
	if ActiveLeakCount() != 1 {
		t.Fatalf("leaks=%d, want 1", ActiveLeakCount())
	}
	leaks := ActiveLeaks()
	if len(leaks) != 1 || leaks[0].Kind != "direct" || len(leaks[0].Stack) == 0 {
		t.Fatalf("leaks=%+v", leaks)
	}
	buf.Release()
	if ActiveLeakCount() != 0 {
		t.Fatalf("leaks=%d, want 0", ActiveLeakCount())
	}
}

func TestLeakDetectorTracksSliceAndCompositeOwnership(t *testing.T) {
	ResetLeakDetection()
	EnableLeakDetection(true)
	defer func() {
		EnableLeakDetection(false)
		ResetLeakDetection()
	}()

	buf := NewHeapBuffer(8)
	_, _ = buf.WriteBytes([]byte("abcdef"))
	slice, err := buf.Slice(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	composite := NewCompositeByteBuf()
	composite.Append(slice)
	if ActiveLeakCount() != 3 {
		t.Fatalf("leaks=%d, want 3", ActiveLeakCount())
	}
	composite.Release()
	buf.Release()
	if ActiveLeakCount() != 0 {
		t.Fatalf("leaks=%d, want 0", ActiveLeakCount())
	}
}
