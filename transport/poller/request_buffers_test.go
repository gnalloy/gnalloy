package poller

import (
	"testing"

	"gnalloy.org/gnalloy/buffer"
)

func TestIORequestRetainBuffersRespectsOwnershipTransfer(t *testing.T) {
	buf := buffer.NewHeapBuffer(8)
	req := IORequest{Buf: buf, TransferBufferOwnership: true}
	if req.RetainBuffers() {
		t.Fatal("ownership-transfer request must not retain buffer")
	}
	if buf.RefCnt() != 1 {
		t.Fatalf("ref=%d, want original owner reference only", buf.RefCnt())
	}
	req.ReleaseBuffers()
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want released transfer owner reference", buf.RefCnt())
	}
}

func TestIORequestRetainBuffersKeepsDefaultRetainSemantics(t *testing.T) {
	buf := buffer.NewHeapBuffer(8)
	req := IORequest{Buf: buf}
	if !req.RetainBuffers() {
		t.Fatal("default request should retain buffer")
	}
	if buf.RefCnt() != 2 {
		t.Fatalf("ref=%d, want retained request reference", buf.RefCnt())
	}
	req.ReleaseBuffers()
	buf.Release()
	if buf.RefCnt() != 0 {
		t.Fatalf("ref=%d, want both references released", buf.RefCnt())
	}
}
