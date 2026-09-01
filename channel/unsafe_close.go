package channel

import "gnalloy.org/gnalloy/transport"

func (u *Unsafe) Close() error {
	future := u.CloseFuture()
	if future.IsDone() {
		return future.Err()
	}
	return nil
}

func (u *Unsafe) CloseFuture() Future {
	if !u.closed.CompareAndSwap(false, true) {
		return u.closePromise
	}
	u.closeWritability()
	locked := u.lockOutboundIfConcurrent()
	u.releaseOutboundLocked()
	if locked {
		u.outboundMu.Unlock()
	}
	if u.registered.Load() && u.poller != nil && (u.poller.Backend() == transport.BackendIOUring || u.poller.Backend() == transport.BackendIOCP) {
		if err := u.poller.Submit(transport.IORequest{Op: transport.OpClose, FD: u.fd, ChannelID: u.ID()}); err == nil {
			return u.closePromise
		}
	}
	if u.rw != nil {
		err := u.rw.Close(u.fd)
		if err != nil {
			u.closePromise.SetFailure(err)
			return u.closePromise
		}
		u.fireInactiveOnce()
		return u.closePromise
	}
	u.fireInactiveOnce()
	return u.closePromise
}

func (u *Unsafe) finishClose() {
	if u.closed.CompareAndSwap(false, true) {
		locked := u.lockOutboundIfConcurrent()
		u.releaseOutboundLocked()
		if locked {
			u.outboundMu.Unlock()
		}
	}
	u.fireInactiveOnce()
}

func (u *Unsafe) fireInactiveOnce() {
	if !u.inactiveFired.CompareAndSwap(false, true) {
		return
	}
	if u.closeHook != nil {
		u.closeHook(u)
	}
	u.ch.Pipeline().FireChannelInactive()
	u.closePromise.SetSuccess()
}
