//go:build windows

package iocp

import (
	"unsafe"

	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/windows"
)

type pendingRequest struct {
	ov      windows.Overlapped
	owner   *Poller
	req     poller.IORequest
	accept  *acceptContext
	wsabufs []windows.WSABuf
	wsabuf  [8]windows.WSABuf
	from    windows.RawSockaddrAny
	fromLen int32
	to      windows.RawSockaddrAny
	toLen   int32
	prev    *pendingRequest
	next    *pendingRequest
}

type acceptContext struct {
	buf [acceptAddressLength * 2]byte
}

func (p *Poller) acquirePending(req poller.IORequest) *pendingRequest {
	pending := p.free
	if pending == nil {
		pending = new(pendingRequest)
	} else {
		p.free = pending.next
	}
	pending.reset(req)
	pending.owner = p
	p.linkPending(pending)
	return pending
}

func (p *Poller) releasePending(pending *pendingRequest) {
	if pending == nil {
		return
	}
	pending.reset(poller.IORequest{})
	pending.next = p.free
	p.free = pending
}

func (p *Poller) linkPending(pending *pendingRequest) {
	pending.prev = nil
	pending.next = p.active
	if p.active != nil {
		p.active.prev = pending
	}
	p.active = pending
}

func (p *Poller) unlinkPending(pending *pendingRequest) {
	if pending == nil {
		return
	}
	if pending.prev != nil {
		pending.prev.next = pending.next
	} else if p.active == pending {
		p.active = pending.next
	}
	if pending.next != nil {
		pending.next.prev = pending.prev
	}
	pending.prev = nil
	pending.next = nil
}

func (pending *pendingRequest) reset(req poller.IORequest) {
	pending.ov = windows.Overlapped{}
	pending.owner = nil
	pending.req = req
	pending.accept = nil
	pending.wsabufs = pending.wsabufs[:0]
	pending.from = windows.RawSockaddrAny{}
	pending.fromLen = 0
	pending.to = windows.RawSockaddrAny{}
	pending.toLen = 0
	pending.prev = nil
	pending.next = nil
}

func pendingFromOverlapped(ov *windows.Overlapped) *pendingRequest {
	if ov == nil {
		return nil
	}
	return (*pendingRequest)(unsafe.Pointer(ov))
}
