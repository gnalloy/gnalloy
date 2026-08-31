package channel

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

func (u *Unsafe) Write(msg any) error {
	if u.closed.Load() {
		releaseWriteMessage(msg)
		return ErrPromiseFailed
	}
	out, err := u.prepareOutboundMessage(msg)
	if err != nil {
		return err
	}
	if out.bytes == 0 {
		out.release()
		return nil
	}
	u.enqueueOutboundMessage(out, nil)
	return nil
}

func (u *Unsafe) WriteAndFlush(msg any) error {
	if u.closed.Load() {
		releaseWriteMessage(msg)
		return ErrPromiseFailed
	}
	out, err := u.prepareOutboundMessage(msg)
	if err != nil {
		return err
	}
	if out.bytes == 0 {
		out.release()
		u.ch.Pipeline().FireFlushComplete()
		return nil
	}
	if done, err := u.tryWriteAndFlushDirect(out); done {
		return err
	}
	u.enqueueOutboundMessage(out, nil)
	return u.flushOutbound()
}

func (u *Unsafe) WriteStaticBytesAndFlush(data []byte) error {
	if u.closed.Load() {
		return ErrPromiseFailed
	}
	if len(data) == 0 {
		u.ch.Pipeline().FireFlushComplete()
		return nil
	}
	if done, err := u.tryWriteStaticBytesAndFlushDirect(data); done {
		return err
	}
	return u.WriteAndFlush(buffer.NewSharedBuffer(data))
}

func (u *Unsafe) WriteFuture(msg any) Future {
	if u.closed.Load() {
		releaseWriteMessage(msg)
		return FailedFuture(ErrPromiseFailed)
	}
	out, err := u.prepareOutboundMessage(msg)
	if err != nil {
		return FailedFuture(err)
	}
	promise := u.newPromise()
	if out.bytes == 0 {
		out.release()
		promise.SetSuccess()
		return promise
	}
	u.enqueueOutboundMessage(out, promise)
	return promise
}

func (u *Unsafe) Flush() error {
	if u.outHead == nil {
		u.ch.Pipeline().FireFlushComplete()
		return nil
	}
	if err := u.flushOutbound(); err != nil {
		u.completeFlushWaiters(err)
		return err
	}
	return nil
}

func (u *Unsafe) FlushFuture() Future {
	promise := u.newPromise()
	if u.outHead == nil {
		promise.SetSuccess()
		u.ch.Pipeline().FireFlushComplete()
		return promise
	}
	u.flushWaiters = append(u.flushWaiters, promise)
	if err := u.flushOutbound(); err != nil {
		u.completeFlushWaiters(err)
		return promise
	}
	return promise
}

func (u *Unsafe) flushOutbound() error {
	if u.poller != nil && u.poller.Model() == transport.PollerCompletion {
		if u.readCallback {
			u.deferredFlush = true
			return nil
		}
		if u.hasFileRegionHead() {
			return u.flushReady()
		}
		return u.submitWrite()
	}
	return u.flushReady()
}

func (u *Unsafe) flushReady() error {
	return u.flushReadyWithSpinBudget(u.maxWriteSpinCount())
}

func (u *Unsafe) flushReadyWithSpinBudget(spinCount int) error {
	for spins := 0; u.outHead != nil && spins < spinCount; spins++ {
		n, again, err := u.writeReadyBatch()
		if n > 0 {
			u.completeWrite(n)
		}
		if err != nil {
			return err
		}
		if again || n == 0 {
			return u.enableWriteInterest()
		}
	}
	if u.outHead != nil {
		return u.enableWriteInterest()
	}
	return u.disableWriteInterest()
}

func (u *Unsafe) writeReadyBatch() (int64, bool, error) {
	if u.outHead.region != nil {
		return u.writeFileRegion(u.outHead.region)
	}
	if u.rw == nil {
		return 0, false, ErrNoOutboundSink
	}
	if u.outHead.next == nil {
		buf := u.outHead.buf
		if _, ok := buf.(*buffer.DirectByteBuf); ok {
			n, again, err := u.rw.Write(u.fd, readableWriteBytes(buf))
			return int64(n), again, err
		}
	}
	if vector := u.vectorWriter; vector != nil {
		u.writeSlices = u.writeSlices[:0]
		u.collectOutboundSlices(maxGatheringBuffers)
		if len(u.writeSlices) > 1 {
			n, again, err := vector.Writev(u.fd, u.writeSlices)
			return int64(n), again, err
		}
	}
	buf := u.outHead.buf
	n, again, err := u.rw.Write(u.fd, readableWriteBytes(buf))
	return int64(n), again, err
}

func readableWriteBytes(buf buffer.ByteBuf) []byte {
	if data, ok := buffer.ContiguousReadableBytes(buf); ok {
		return data
	}
	return buf.Bytes()
}

func (u *Unsafe) tryWriteAndFlushDirect(out outboundMessage) (bool, error) {
	if !u.canWriteAndFlushDirect(out) {
		return false, nil
	}
	data, ok := buffer.ContiguousReadableBytes(out.buf)
	if !ok {
		return false, nil
	}
	n, again, err := u.rw.Write(u.fd, data)
	if n < 0 {
		if again && err == nil {
			n = 0
		} else {
			out.release()
			return true, ErrInvalidMessage
		}
	}
	if n > len(data) {
		out.release()
		return true, ErrInvalidMessage
	}
	if n > 0 {
		_ = out.buf.SkipBytes(n)
	}
	if out.buf.ReadableBytes() == 0 {
		out.release()
		u.ch.Pipeline().FireFlushComplete()
		if err != nil {
			return true, err
		}
		if again {
			return true, u.enableWriteInterest()
		}
		if u.writeInterest {
			return true, u.disableWriteInterest()
		}
		return true, nil
	}
	out.bytes = int64(out.buf.ReadableBytes())
	u.enqueueOutboundMessage(out, nil)
	if err != nil {
		return true, err
	}
	if again || n == 0 {
		return true, u.enableWriteInterest()
	}
	return true, u.flushReadyWithSpinBudget(u.maxWriteSpinCount() - 1)
}

func (u *Unsafe) canWriteAndFlushDirect(out outboundMessage) bool {
	if u.outHead != nil || out.region != nil || out.buf == nil || out.bytes <= 0 || u.rw == nil {
		return false
	}
	if u.poller == nil || u.poller.Model() == transport.PollerCompletion {
		return false
	}
	return out.bytes < u.writeHighWatermark
}

func (u *Unsafe) tryWriteStaticBytesAndFlushDirect(data []byte) (bool, error) {
	if !u.canWriteStaticBytesAndFlushDirect(len(data)) {
		return false, nil
	}
	n, again, err := u.rw.Write(u.fd, data)
	if n < 0 {
		if again && err == nil {
			n = 0
		} else {
			return true, ErrInvalidMessage
		}
	}
	if n > len(data) {
		return true, ErrInvalidMessage
	}
	if n == len(data) {
		u.ch.Pipeline().FireFlushComplete()
		if err != nil {
			return true, err
		}
		if again {
			return true, u.enableWriteInterest()
		}
		if u.writeInterest {
			return true, u.disableWriteInterest()
		}
		return true, nil
	}
	out := outboundMessage{
		buf:   buffer.NewSharedBuffer(data[n:]),
		bytes: int64(len(data) - n),
	}
	u.enqueueOutboundMessage(out, nil)
	if err != nil {
		return true, err
	}
	if again || n == 0 {
		return true, u.enableWriteInterest()
	}
	return true, u.flushReadyWithSpinBudget(u.maxWriteSpinCount() - 1)
}

func (u *Unsafe) canWriteStaticBytesAndFlushDirect(size int) bool {
	if size <= 0 || u.outHead != nil || u.rw == nil {
		return false
	}
	if u.poller == nil || u.poller.Model() == transport.PollerCompletion {
		return false
	}
	return int64(size) < u.writeHighWatermark
}

func (u *Unsafe) submitWrite() error {
	req, ok := u.prepareWriteRequest()
	if !ok {
		return nil
	}
	u.writePending = true
	err := u.poller.Submit(req)
	if err != nil {
		u.writePending = false
	}
	return err
}

func (u *Unsafe) prepareWriteRequest() (transport.IORequest, bool) {
	if u.writePending || u.outHead == nil {
		return transport.IORequest{}, false
	}
	u.collectOutboundBatch()
	if len(u.writeBatch) == 0 {
		return transport.IORequest{}, false
	}
	req := transport.IORequest{
		Op:        transport.OpWrite,
		FD:        u.fd,
		ChannelID: u.ID(),
	}
	if len(u.writeBatch) == 1 {
		req.Buf = u.writeBatch[0]
	} else {
		req.Bufs = u.writeBatch
	}
	req = u.prepareIORequest(req)
	return req, true
}

func (u *Unsafe) completeWrite(n int64) {
	if u.outHead == nil {
		return
	}
	for n > 0 && u.outHead != nil {
		if u.outHead.region != nil {
			u.completeFileRegionWrite(n)
			n = 0
			continue
		}
		buf := u.outHead.buf
		readable := buf.ReadableBytes()
		if readable == 0 {
			u.dequeueOutbound()
			continue
		}
		consume := int64(readable)
		if n < consume {
			consume = n
		}
		_ = buf.SkipBytes(int(consume))
		u.outboundBytes.Add(-consume)
		n -= consume
		if buf.ReadableBytes() == 0 {
			u.dequeueOutbound()
		}
	}
	u.updateWritability()
	if u.outHead == nil {
		u.completeFlushWaiters(nil)
		u.ch.Pipeline().FireFlushComplete()
	}
}

func (u *Unsafe) prepareOutboundMessage(msg any) (outboundMessage, error) {
	out, err := newOutboundMessage(msg)
	if err != nil {
		return outboundMessage{}, err
	}
	if out.bytes == 0 || out.region == nil {
		return out, nil
	}
	if u.fileRegionWriter == nil {
		return outboundMessage{}, ErrNoOutboundSink
	}
	return out, nil
}

func (u *Unsafe) writeFileRegion(region FileRegion) (int64, bool, error) {
	if u.fileRegionWriter == nil {
		return 0, false, ErrNoOutboundSink
	}
	before := region.Transferred()
	remaining := region.Count() - before
	n, again, err := u.fileRegionWriter.WriteFileRegion(u.fd, region)
	if n < 0 || n > remaining {
		return 0, false, ErrInvalidFileRegion
	}
	if delta := region.Transferred() - before; delta != n {
		return 0, false, ErrInvalidFileRegion
	}
	return n, again, err
}

func (u *Unsafe) completeFileRegionWrite(n int64) {
	u.outboundBytes.Add(-n)
	region := u.outHead.region
	if region == nil {
		return
	}
	if region.Transferred() >= region.Count() {
		u.dequeueOutbound()
	}
}

func (u *Unsafe) hasFileRegionHead() bool {
	return u.outHead != nil && u.outHead.region != nil
}

func (u *Unsafe) maxWriteSpinCount() int {
	spinCount := int(u.writeSpinCount.Load())
	if spinCount <= 0 {
		return 1
	}
	return spinCount
}

func (u *Unsafe) newPromise() *DefaultPromise {
	if executor, ok := u.eventExecutor.Load().(FutureListenerExecutor); ok {
		return NewPromiseWithExecutor(executor)
	}
	return NewPromise()
}

func (u *Unsafe) completeFlushWaiters(err error) {
	if len(u.flushWaiters) == 0 {
		return
	}
	waiters := u.flushWaiters
	u.flushWaiters = nil
	for _, waiter := range waiters {
		if err != nil {
			waiter.SetFailure(err)
		} else {
			waiter.SetSuccess()
		}
	}
}
