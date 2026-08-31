package message

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
)

func TestReleaseHandlesByteBuf(t *testing.T) {
	buf := buffer.NewHeapBuffer(4)
	Release(buf)
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want released", buf.RefCnt())
	}
}

func TestReleaseHandlesBoolReturningRelease(t *testing.T) {
	recorder := &boolReleaseRecorder{}
	Release(recorder)
	if recorder.releases != 1 {
		t.Fatalf("releases=%d, want 1", recorder.releases)
	}
}

func TestReleaseHandlesPlainRelease(t *testing.T) {
	recorder := &plainReleaseRecorder{}
	Release(recorder)
	if recorder.releases != 1 {
		t.Fatalf("releases=%d, want 1", recorder.releases)
	}
}

type boolReleaseRecorder struct {
	releases int
}

func (r *boolReleaseRecorder) Release() bool {
	r.releases++
	return true
}

type plainReleaseRecorder struct {
	releases int
}

func (r *plainReleaseRecorder) Release() {
	r.releases++
}
