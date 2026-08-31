//go:build windows

package iocp

import (
	"errors"
	"sync/atomic"
	"unsafe"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/windows"
)

const acceptAddressLength = 128

type Poller struct {
	port windows.Handle

	closed  atomic.Bool
	entries map[poller.FDRef]poller.ChannelID
	active  *pendingRequest
	free    *pendingRequest

	completions          []completionEntry
	completionBatchAddr  uintptr
	singleCompletionPoll bool
}

func New() (poller.Poller, error) {
	port, err := windows.CreateIoCompletionPort(windows.InvalidHandle, 0, 0, 0)
	if err != nil {
		return nil, err
	}
	batchAddr, batchErr := completionBatchAddress()
	return &Poller{
		port:                 port,
		entries:              make(map[poller.FDRef]poller.ChannelID, 1024),
		completions:          make([]completionEntry, defaultCompletionBatch),
		completionBatchAddr:  batchAddr,
		singleCompletionPoll: batchErr != nil,
	}, nil
}

func (p *Poller) Model() poller.Model {
	return poller.Completion
}

func (p *Poller) Backend() poller.BackendKind {
	return poller.BackendIOCP
}

func (p *Poller) Register(fd poller.FDRef, ch poller.ChannelID, _ poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	_, err := windows.CreateIoCompletionPort(windows.Handle(uintptr(fd.FD)), p.port, uintptr(ch), 0)
	if err != nil {
		return err
	}
	p.entries[fd] = ch
	return nil
}

func (p *Poller) Modify(fd poller.FDRef, _ poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	return nil
}

func (p *Poller) Deregister(fd poller.FDRef) error {
	delete(p.entries, fd)
	return nil
}

func (p *Poller) Submit(req poller.IORequest) error {
	if req.Op == poller.OpWakeup {
		return p.Wakeup()
	}
	if !validRequest(req) {
		return poller.ErrInvalidIORequest
	}
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	retainedBuffers := req.RetainBuffers()
	pending := p.acquirePending(req)
	if req.Op == poller.OpAccept {
		pending.accept = &acceptContext{}
	}
	if req.Op == poller.OpRead && req.Datagram {
		pending.fromLen = int32(unsafe.Sizeof(pending.from))
	}
	if req.Op == poller.OpWrite {
		wsabufs, err := makeWriteBuffers(req, pending.wsabuf[:0])
		if err != nil {
			if retainedBuffers {
				req.ReleaseBuffers()
			}
			p.unlinkPending(pending)
			p.releasePending(pending)
			return err
		}
		pending.wsabufs = wsabufs
		if req.Datagram {
			to, toLen, err := makeRawSockaddr(req.Addr)
			if err != nil {
				if retainedBuffers {
					req.ReleaseBuffers()
				}
				p.unlinkPending(pending)
				p.releasePending(pending)
				return err
			}
			pending.to = to
			pending.toLen = toLen
		}
	}

	var err error
	switch req.Op {
	case poller.OpAccept:
		err = p.submitAccept(req, &pending.ov, pending.accept)
	case poller.OpRead:
		err = p.submitRead(req, &pending.ov, pending)
	case poller.OpWrite:
		err = p.submitWrite(req, &pending.ov, pending.wsabufs, pending)
	case poller.OpClose:
		err = p.submitClose(req, &pending.ov)
	default:
		err = poller.ErrInvalidIORequest
	}
	if err == nil || err == windows.ERROR_IO_PENDING {
		return nil
	}

	p.unlinkPending(pending)
	p.releasePending(pending)
	if retainedBuffers {
		req.ReleaseBuffers()
	}
	if req.Op == poller.OpAccept && req.AcceptedFD.Valid() {
		_ = windows.Closesocket(windows.Handle(uintptr(req.AcceptedFD.FD)))
	}
	return err
}

func (p *Poller) Poll(dst []poller.Event, timeoutMillis int) (int, error) {
	if p.closed.Load() {
		return 0, poller.ErrClosedPoller
	}
	if len(dst) == 0 {
		return 0, nil
	}
	if p.singleCompletionPoll {
		return p.pollSingle(dst, timeoutMillis)
	}
	return p.pollBatch(dst, timeoutMillis)
}

func (p *Poller) pollBatch(dst []poller.Event, timeoutMillis int) (int, error) {
	entries := p.completionEntries(len(dst))
	n, err := getQueuedCompletionStatusEx(p.completionBatchAddr, p.port, entries, completionTimeout(timeoutMillis))
	if err == windows.Errno(windows.WAIT_TIMEOUT) {
		return 0, nil
	}
	if err != nil {
		if unsupportedCompletionBatch(err) {
			p.singleCompletionPoll = true
			return p.pollSingle(dst, timeoutMillis)
		}
		if p.closed.Load() {
			return 0, poller.ErrClosedPoller
		}
		return 0, err
	}
	out := 0
	for i := 0; i < n && out < len(dst); i++ {
		entry := entries[i]
		if p.fillCompletionEvent(&dst[out], entry.overlapped, entry.key, entry.transferred, nil) {
			out++
		}
	}
	clear(entries[:n])
	return out, nil
}

func (p *Poller) pollSingle(dst []poller.Event, timeoutMillis int) (int, error) {
	timeout := completionTimeout(timeoutMillis)
	out := 0
	for out < len(dst) {
		var transferred uint32
		var key uintptr
		var ov *windows.Overlapped
		err := windows.GetQueuedCompletionStatus(p.port, &transferred, &key, &ov, timeout)
		timeout = 0
		if err == windows.Errno(windows.WAIT_TIMEOUT) {
			break
		}
		if ov == nil && key == 0 {
			if err != nil {
				return out, err
			}
			if p.fillCompletionEvent(&dst[out], ov, key, transferred, nil) {
				out++
			}
			continue
		}
		if p.fillCompletionEvent(&dst[out], ov, key, transferred, err) {
			out++
		}
	}
	return out, nil
}

func (p *Poller) fillCompletionEvent(dst *poller.Event, ov *windows.Overlapped, key uintptr, transferred uint32, err error) bool {
	if ov == nil && key == 0 {
		*dst = poller.Event{Model: poller.Completion, Op: poller.OpWakeup}
		return true
	}
	pending := pendingFromOverlapped(ov)
	if pending == nil || pending.owner != p {
		return false
	}
	p.unlinkPending(pending)
	req := pending.req
	err = completionEventError(req, &pending.ov, &transferred, err)
	if req.Buf != nil && req.Op == poller.OpRead && transferred > 0 {
		if advErr := req.Buf.AdvanceWriter(int(transferred)); advErr != nil && err == nil {
			err = advErr
		}
	}
	addr := req.Addr
	if req.Op == poller.OpRead && req.Datagram && err == nil {
		addr = socketAddressFromRaw(&pending.from, pending.fromLen)
	}
	*dst = poller.Event{
		Model:      poller.Completion,
		Op:         req.Op,
		Ready:      poller.CompletionReady(req.Op),
		FD:         req.FD,
		AcceptedFD: req.AcceptedFD,
		ChannelID:  req.ChannelID,
		OpID:       req.OpID,
		Buf:        req.Buf,
		Bufs:       req.Bufs,
		Addr:       addr,
		N:          int(transferred),
		Err:        err,
	}
	p.releasePending(pending)
	return true
}

func (p *Poller) completionEntries(n int) []completionEntry {
	if cap(p.completions) < n {
		p.completions = make([]completionEntry, n)
	}
	return p.completions[:n]
}

func (p *Poller) Wakeup() error {
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	return windows.PostQueuedCompletionStatus(p.port, 0, 0, nil)
}

func (p *Poller) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	for pending := p.active; pending != nil; {
		next := pending.next
		req := pending.req
		cancelPending(req, &pending.ov)
		req.ReleaseBuffers()
		if req.Op == poller.OpAccept && req.AcceptedFD.Valid() {
			_ = windows.Closesocket(windows.Handle(uintptr(req.AcceptedFD.FD)))
		}
		pending.prev = nil
		pending.next = nil
		pending = next
	}
	p.active = nil
	p.free = nil
	return windows.CloseHandle(p.port)
}

func cancelPending(req poller.IORequest, ov *windows.Overlapped) {
	if ov == nil || !req.FD.Valid() {
		return
	}
	err := windows.CancelIoEx(windows.Handle(uintptr(req.FD.FD)), ov)
	if err != nil && !errors.Is(err, windows.ERROR_NOT_FOUND) && !errors.Is(err, windows.ERROR_OPERATION_ABORTED) {
		return
	}
}

func (p *Poller) submitAccept(req poller.IORequest, ov *windows.Overlapped, ctx *acceptContext) error {
	if ctx == nil {
		return poller.ErrInvalidIORequest
	}
	var recvd uint32
	return windows.AcceptEx(
		windows.Handle(uintptr(req.FD.FD)),
		windows.Handle(uintptr(req.AcceptedFD.FD)),
		&ctx.buf[0],
		0,
		acceptAddressLength,
		acceptAddressLength,
		&recvd,
		ov,
	)
}

func (p *Poller) submitRead(req poller.IORequest, ov *windows.Overlapped, pending *pendingRequest) error {
	view := req.Buf.WritableBytesView()
	if len(view) == 0 {
		return buffer.ErrNoWritableBytes
	}
	wsabuf := windows.WSABuf{Len: uint32(len(view)), Buf: &view[0]}
	var flags uint32
	var recvd uint32
	if req.Datagram {
		if pending == nil {
			return poller.ErrInvalidIORequest
		}
		return windows.WSARecvFrom(windows.Handle(uintptr(req.FD.FD)), &wsabuf, 1, &recvd, &flags, &pending.from, &pending.fromLen, ov, nil)
	}
	return windows.WSARecv(windows.Handle(uintptr(req.FD.FD)), &wsabuf, 1, &recvd, &flags, ov, nil)
}

func (p *Poller) submitWrite(req poller.IORequest, ov *windows.Overlapped, wsabufs []windows.WSABuf, pending *pendingRequest) error {
	if len(wsabufs) == 0 {
		return poller.ErrInvalidIORequest
	}
	var sent uint32
	if req.Datagram {
		if pending == nil || pending.toLen == 0 {
			return poller.ErrInvalidIORequest
		}
		return windows.WSASendTo(windows.Handle(uintptr(req.FD.FD)), &wsabufs[0], uint32(len(wsabufs)), &sent, 0, &pending.to, pending.toLen, ov, nil)
	}
	return windows.WSASend(windows.Handle(uintptr(req.FD.FD)), &wsabufs[0], uint32(len(wsabufs)), &sent, 0, ov, nil)
}

func (p *Poller) submitClose(req poller.IORequest, ov *windows.Overlapped) error {
	err := windows.Closesocket(windows.Handle(uintptr(req.FD.FD)))
	if err != nil {
		return err
	}
	return windows.PostQueuedCompletionStatus(p.port, 0, uintptr(req.ChannelID), ov)
}

func validRequest(req poller.IORequest) bool {
	switch req.Op {
	case poller.OpAccept:
		return req.FD.Valid() && req.AcceptedFD.Valid()
	case poller.OpClose:
		return req.FD.Valid()
	case poller.OpRead, poller.OpWrite:
		if !req.FD.Valid() || (req.Buf == nil && len(req.Bufs) == 0) {
			return false
		}
		return !req.Datagram || req.Op == poller.OpRead || req.Addr.Valid()
	default:
		return false
	}
}

func makeRawSockaddr(addr poller.SocketAddress) (windows.RawSockaddrAny, int32, error) {
	var rsa windows.RawSockaddrAny
	switch addr.Family {
	case poller.SocketFamilyIPv4:
		raw := (*windows.RawSockaddrInet4)(unsafe.Pointer(&rsa))
		raw.Family = windows.AF_INET
		raw.Port = htons(uint16(addr.Port))
		copy(raw.Addr[:], addr.IP[:4])
		return rsa, int32(unsafe.Sizeof(*raw)), nil
	case poller.SocketFamilyIPv6:
		raw := (*windows.RawSockaddrInet6)(unsafe.Pointer(&rsa))
		raw.Family = windows.AF_INET6
		raw.Port = htons(uint16(addr.Port))
		raw.Scope_id = addr.ZoneID
		copy(raw.Addr[:], addr.IP[:])
		return rsa, int32(unsafe.Sizeof(*raw)), nil
	default:
		return rsa, 0, poller.ErrInvalidIORequest
	}
}

func socketAddressFromRaw(rsa *windows.RawSockaddrAny, n int32) poller.SocketAddress {
	if rsa == nil || n <= 0 {
		return poller.SocketAddress{}
	}
	switch rsa.Addr.Family {
	case windows.AF_INET:
		raw := (*windows.RawSockaddrInet4)(unsafe.Pointer(rsa))
		var addr poller.SocketAddress
		addr.Family = poller.SocketFamilyIPv4
		addr.Port = int(ntohs(raw.Port))
		copy(addr.IP[:4], raw.Addr[:])
		return addr
	case windows.AF_INET6:
		raw := (*windows.RawSockaddrInet6)(unsafe.Pointer(rsa))
		var addr poller.SocketAddress
		addr.Family = poller.SocketFamilyIPv6
		addr.Port = int(ntohs(raw.Port))
		addr.ZoneID = raw.Scope_id
		copy(addr.IP[:], raw.Addr[:])
		return addr
	default:
		return poller.SocketAddress{}
	}
}

func htons(v uint16) uint16 {
	return v<<8 | v>>8
}

func ntohs(v uint16) uint16 {
	return htons(v)
}
