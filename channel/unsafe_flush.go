package channel

import "gnalloy.org/gnalloy/transport"

func (u *Unsafe) canTryImmediateWrite() bool {
	if u.readCallback && u.flushStrategy() != FlushImmediate {
		return false
	}
	if u.flushScheduler == nil {
		return true
	}
	return u.flushStrategy() == FlushImmediate
}

func (u *Unsafe) requestFlush() error {
	if u.outHead == nil {
		u.flushPending = false
		u.ch.Pipeline().FireFlushComplete()
		return nil
	}
	u.flushPending = true
	if u.poller != nil && u.poller.Model() == transport.PollerCompletion && u.readCallback {
		u.deferredFlush = true
		return nil
	}
	switch u.flushStrategy() {
	case FlushImmediate:
		return u.runPendingFlush()
	case FlushOnReadComplete:
		if u.readCallback {
			u.deferredFlush = true
			return nil
		}
		return u.runPendingFlush()
	case FlushOnEventLoopBatch:
		if u.flushScheduler != nil {
			return u.scheduleFlush()
		}
		if u.readCallback {
			u.deferredFlush = true
			return nil
		}
		return u.runPendingFlush()
	default:
		return u.runPendingFlush()
	}
}

func (u *Unsafe) scheduleFlush() error {
	if u.flushScheduled {
		return nil
	}
	u.flushScheduled = true
	err := u.flushScheduler.SubmitAfterBatch(u.flushTask)
	if err != nil {
		u.flushScheduled = false
		return err
	}
	return nil
}

func (u *Unsafe) executeScheduledFlush() {
	if err := u.runPendingFlush(); err != nil {
		u.failFlush(err)
	}
}

func (u *Unsafe) runPendingFlush() error {
	u.flushScheduled = false
	if !u.flushPending {
		return nil
	}
	if u.outHead == nil {
		u.flushPending = false
		u.ch.Pipeline().FireFlushComplete()
		return nil
	}
	if err := u.flushOutbound(); err != nil {
		return err
	}
	if u.outHead == nil {
		u.flushPending = false
	}
	return nil
}

func (u *Unsafe) flushStrategy() FlushStrategy {
	switch v := FlushStrategy(u.cachedFlushStrategy.Load()); v {
	case FlushImmediate, FlushOnReadComplete, FlushOnEventLoopBatch:
		return v
	default:
		return OptionFlushStrategy.Default()
	}
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

func (u *Unsafe) failFlush(err error) {
	u.completeFlushWaiters(err)
	u.fail(err)
}
