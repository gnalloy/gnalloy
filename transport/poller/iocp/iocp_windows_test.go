//go:build windows

package iocp

import (
	"errors"
	"testing"
	"unsafe"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/windows"
)

func TestSubmitAcceptClosesAcceptedSocketOnImmediateFailure(t *testing.T) {
	if err := ensureWSAStartup(); err != nil {
		t.Fatal(err)
	}
	p, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	accepted, err := windows.WSASocket(windows.AF_INET, windows.SOCK_STREAM, windows.IPPROTO_TCP, nil, 0, windows.WSA_FLAG_OVERLAPPED)
	if err != nil {
		t.Fatal(err)
	}
	err = p.Submit(poller.IORequest{
		Op:         poller.OpAccept,
		FD:         poller.FDRef{FD: 1},
		AcceptedFD: poller.FDRef{FD: int(accepted)},
	})
	if err == nil {
		_ = windows.Closesocket(accepted)
		t.Fatal("Submit unexpectedly succeeded")
	}
	if closeErr := windows.Closesocket(accepted); !errors.Is(closeErr, windows.WSAENOTSOCK) {
		t.Fatalf("accepted socket was not closed, close err=%v", closeErr)
	}
}

func TestMakeWriteBuffersUsesInlineStorageForSingleBuffer(t *testing.T) {
	buf := buffer.NewHeapBuffer(8)
	defer buf.Release()
	if _, err := buf.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	var inline [1]windows.WSABuf
	wsabufs, err := makeWriteBuffers(poller.IORequest{Buf: buf}, inline[:0])
	if err != nil {
		t.Fatal(err)
	}
	if len(wsabufs) != 1 || wsabufs[0].Len != 4 {
		t.Fatalf("wsabufs=%+v, want one 4-byte buffer", wsabufs)
	}
	if &wsabufs[0] != &inline[0] {
		t.Fatal("single-buffer write should reuse inline WSABuf storage")
	}
}

func TestMakeWriteBuffersExpandsCompositeWithoutCopy(t *testing.T) {
	buf := fragmentedWriteBuffer("ab", "cd")
	defer buf.Release()
	var inline [8]windows.WSABuf
	wsabufs, err := makeWriteBuffers(poller.IORequest{Buf: buf}, inline[:0])
	if err != nil {
		t.Fatal(err)
	}
	if len(wsabufs) != 2 || wsabufs[0].Len != 2 || wsabufs[1].Len != 2 {
		t.Fatalf("wsabufs=%+v, want two 2-byte vectors", wsabufs)
	}
	if &wsabufs[0] != &inline[0] {
		t.Fatal("composite write should reuse inline WSABuf storage")
	}
}

func BenchmarkMakeWriteBuffersComposite(b *testing.B) {
	buf := fragmentedWriteBuffer("abcd", "efgh", "ijkl", "mnop")
	defer buf.Release()
	req := poller.IORequest{Buf: buf}
	var inline [8]windows.WSABuf

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wsabufs, err := makeWriteBuffers(req, inline[:0])
		if err != nil {
			b.Fatal(err)
		}
		if writeBufferBytes(wsabufs) != 16 {
			b.Fatalf("wsabufs=%+v", wsabufs)
		}
	}
}

func TestPendingRequestOverlappedRoundTrip(t *testing.T) {
	if offset := unsafe.Offsetof(pendingRequest{}.ov); offset != 0 {
		t.Fatalf("overlapped offset=%d, want zero", offset)
	}
	pending := &pendingRequest{}
	if got := pendingFromOverlapped(&pending.ov); got != pending {
		t.Fatalf("pendingFromOverlapped returned %p, want %p", got, pending)
	}
}

func TestPendingRequestFreelistResetsState(t *testing.T) {
	buf := buffer.NewHeapBuffer(8)
	defer buf.Release()
	if _, err := buf.WriteBytes([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	p := &Poller{}
	first := p.acquirePending(poller.IORequest{
		Op:  poller.OpWrite,
		FD:  poller.FDRef{FD: 10},
		Buf: buf,
	})
	first.accept = &acceptContext{}
	first.wsabufs = append(first.wsabufs, windows.WSABuf{Len: 4})
	first.fromLen = 8
	first.toLen = 8
	p.unlinkPending(first)
	p.releasePending(first)

	second := p.acquirePending(poller.IORequest{
		Op:  poller.OpRead,
		FD:  poller.FDRef{FD: 11},
		Buf: buf,
	})
	if second != first {
		t.Fatal("pending freelist did not reuse released state")
	}
	if second.req.Op != poller.OpRead || second.req.FD.FD != 11 {
		t.Fatalf("request=%+v, want fresh read request", second.req)
	}
	if second.accept != nil || len(second.wsabufs) != 0 || second.fromLen != 0 || second.toLen != 0 {
		t.Fatalf("pending was not reset: %+v", second)
	}
	if second.prev != nil || second.next != nil || p.active != second {
		t.Fatal("active list was not reset correctly")
	}
}

func TestFillCompletionEventRecyclesPendingState(t *testing.T) {
	buf := buffer.NewHeapBuffer(8)
	defer buf.Release()
	p := &Poller{}
	pending := p.acquirePending(poller.IORequest{
		Op:        poller.OpRead,
		FD:        poller.FDRef{FD: 10},
		ChannelID: 7,
		OpID:      9,
		Buf:       buf,
	})

	var ev poller.Event
	if !p.fillCompletionEvent(&ev, &pending.ov, uintptr(pending.req.ChannelID), 4, nil) {
		t.Fatal("completion event was not filled")
	}
	if ev.Op != poller.OpRead || ev.ChannelID != 7 || ev.OpID != 9 || ev.N != 4 {
		t.Fatalf("event=%+v, want read completion for channel 7 op 9", ev)
	}
	if ev.Buf != buf || ev.Buf.ReadableBytes() != 4 {
		t.Fatalf("event buffer readable=%d, want 4", ev.Buf.ReadableBytes())
	}
	if p.active != nil || p.free != pending {
		t.Fatal("completed pending state was not moved to freelist")
	}
}

func TestCompletionBatchHelpers(t *testing.T) {
	if completionTimeout(-1) != windows.INFINITE {
		t.Fatal("negative timeout must map to INFINITE")
	}
	if completionTimeout(25) != 25 {
		t.Fatal("positive timeout must pass through milliseconds")
	}
	if !unsupportedCompletionBatch(windows.ERROR_PROC_NOT_FOUND) {
		t.Fatal("missing completion batch proc should be treated as unsupported")
	}
}

func ensureWSAStartup() error {
	var data windows.WSAData
	return windows.WSAStartup(0x202, &data)
}

func writeBufferBytes(wsabufs []windows.WSABuf) uint32 {
	var total uint32
	for _, buf := range wsabufs {
		total += buf.Len
	}
	return total
}

func fragmentedWriteBuffer(parts ...string) buffer.ByteBuf {
	c := buffer.NewCompositeByteBuf()
	for _, part := range parts {
		buf := buffer.NewHeapBuffer(len(part))
		_, _ = buf.WriteBytes([]byte(part))
		c.Append(buf)
	}
	return c
}
