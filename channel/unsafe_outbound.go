package channel

import "gnalloy.org/gnalloy/transport"

func (u *Unsafe) collectOutboundBatch() {
	u.writeBatch = u.writeBatch[:0]
	limit := maxGatheringBuffers
	if u.fixedBuffers && u.poller != nil && u.poller.Backend() == transport.BackendIOUring {
		limit = 1
	}
	for e := u.outHead; e != nil && len(u.writeBatch) < limit; e = e.next {
		if e.region != nil {
			break
		}
		if e.buf == nil || e.buf.ReadableBytes() == 0 {
			continue
		}
		u.writeBatch = append(u.writeBatch, e.buf)
	}
}

func (u *Unsafe) collectOutboundSlices(limit int) {
	for e := u.outHead; e != nil && len(u.writeSlices) < limit; e = e.next {
		if e.region != nil {
			break
		}
		if e.buf == nil || e.buf.ReadableBytes() == 0 {
			continue
		}
		before := len(u.writeSlices)
		u.writeSlices = e.buf.ReadableSlices(u.writeSlices)
		if len(u.writeSlices) > limit {
			u.writeSlices = u.writeSlices[:limit]
			return
		}
		if len(u.writeSlices) == before {
			return
		}
	}
}

func (u *Unsafe) enqueueOutboundMessage(msg outboundMessage, promise Promise) {
	e := u.acquireOutboundEntry(msg, promise)
	if u.outTail == nil {
		u.outHead = e
		u.outTail = e
	} else {
		u.outTail.next = e
		u.outTail = e
	}
	u.outboundBytes.Add(msg.bytes)
	u.updateWritability()
}

func (u *Unsafe) dequeueOutbound() {
	e := u.outHead
	if e == nil {
		return
	}
	u.outHead = e.next
	if u.outHead == nil {
		u.outTail = nil
	}
	e.releaseMessage()
	if e.promise != nil {
		e.promise.SetSuccess()
	}
	u.releaseOutboundEntry(e)
	u.updateWritability()
}

func (u *Unsafe) acquireOutboundEntry(msg outboundMessage, promise Promise) *outboundEntry {
	if u.outFree == nil {
		return &outboundEntry{buf: msg.buf, region: msg.region, promise: promise}
	}
	e := u.outFree
	u.outFree = e.next
	e.buf = msg.buf
	e.region = msg.region
	e.promise = promise
	e.next = nil
	return e
}

func (u *Unsafe) releaseOutboundEntry(e *outboundEntry) {
	e.buf = nil
	e.region = nil
	e.promise = nil
	e.next = u.outFree
	u.outFree = e
}

func (u *Unsafe) updateWritability() {
	if u.closed.Load() {
		u.closeWritability()
		return
	}
	pending := u.outboundBytes.Load()
	if u.writable.Load() && pending >= u.writeHighWatermark {
		u.writable.Store(false)
		u.ch.Pipeline().FireChannelWritabilityChanged()
		return
	}
	if !u.writable.Load() && pending <= u.writeLowWatermark {
		u.writable.Store(true)
		u.ch.Pipeline().FireChannelWritabilityChanged()
	}
}

func (u *Unsafe) closeWritability() {
	if u.writable.CompareAndSwap(true, false) {
		u.ch.Pipeline().FireChannelWritabilityChanged()
	}
}

func (u *Unsafe) enableWriteInterest() error {
	if u.poller == nil || u.writeInterest {
		return nil
	}
	u.writeInterest = true
	return u.poller.Modify(u.fd, u.readInterest()|transport.ReadyWrite)
}

func (u *Unsafe) disableWriteInterest() error {
	if u.poller == nil || !u.writeInterest {
		return nil
	}
	u.writeInterest = false
	return u.poller.Modify(u.fd, u.readInterest())
}

func (u *Unsafe) releaseOutbound() {
	for u.outHead != nil {
		e := u.outHead
		u.outHead = e.next
		if u.outHead == nil {
			u.outTail = nil
		}
		e.releaseMessage()
		if e.promise != nil {
			e.promise.SetFailure(ErrPromiseFailed)
		}
		u.releaseOutboundEntry(e)
	}
	u.outboundBytes.Store(0)
	u.completeFlushWaiters(ErrPromiseFailed)
	for u.outFree != nil {
		e := u.outFree
		u.outFree = e.next
		e.next = nil
	}
}

func (e *outboundEntry) releaseMessage() {
	if e.buf != nil {
		e.buf.Release()
	}
	if e.region != nil {
		_ = e.region.Close()
	}
}
