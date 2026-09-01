package channel

import "gnalloy.org/gnalloy/transport"

// BindEventExecutor 绑定 Channel 所属 EventLoop，保证 Future listener 回到 owner loop。
func (u *Unsafe) BindEventExecutor(executor interface{ Submit(transport.Task) error }) {
	if executor == nil {
		return
	}
	u.eventExecutor.Store(executor)
	if scheduler, ok := executor.(eventLoopTailSubmitter); ok {
		u.flushScheduler = scheduler
	}
	u.closePromise.SetListenerExecutor(executor)
	if u.ch != nil {
		u.ch.BindEventExecutor(executor)
	}
}

// MarkRegistered 由 EventLoop 在 fd 成功注册到底层 poller 后调用。
func (u *Unsafe) MarkRegistered() {
	u.registered.Store(true)
	u.ch.Pipeline().FireChannelRegistered()
}

// MarkDeregistered 由 EventLoop 在 fd 从底层 poller 移除后调用。
func (u *Unsafe) MarkDeregistered() {
	u.registered.Store(false)
	u.ch.Pipeline().FireChannelUnregistered()
}

func (u *Unsafe) HandleEvent(ev transport.PollEvent) {
	if u.closed.Load() {
		if ev.Buf != nil {
			ev.Buf.Release()
		}
		for _, buf := range ev.Bufs {
			buf.Release()
		}
		if ev.Op == transport.OpClose {
			u.fireInactiveOnce()
		}
		return
	}
	if ev.Model == transport.PollerCompletion {
		u.handleCompletion(ev)
		return
	}
	if ev.Err != nil {
		u.fail(ev.Err)
		return
	}
	u.handleReadiness(ev)
}

// Activate 在 Channel 归属 EventLoop 并注册到底层 Poller 后调用。
// readiness 后端等待可读事件，completion 后端需要主动提交首个 read。
func (u *Unsafe) Activate() error {
	u.ch.Pipeline().FireChannelActive()
	if err := u.BeginRead(); err != nil {
		u.fail(err)
		return err
	}
	return nil
}

func (u *Unsafe) handleReadiness(ev transport.PollEvent) {
	if ev.Ready&(transport.ReadyError|transport.ReadyHangup) != 0 {
		_ = u.Close()
		return
	}
	if ev.Ready&transport.ReadyRead != 0 {
		if u.AutoRead() {
			u.readReady()
		}
	}
	if ev.Ready&transport.ReadyWrite != 0 {
		if err := u.runPendingFlush(); err != nil {
			u.failFlush(err)
		}
	}
}

func (u *Unsafe) handleCompletion(ev transport.PollEvent) {
	switch ev.Op {
	case transport.OpRead:
		u.readPending = false
		u.recordReadBuffer(int(ev.N), readAttemptedSize(ev.Buf))
		if ev.Err != nil {
			if ev.Buf != nil {
				ev.Buf.Release()
			}
			u.fail(ev.Err)
			return
		}
		read := false
		u.readCallback.Store(true)
		if ev.N > 0 && ev.Buf != nil {
			u.ch.Pipeline().FireChannelRead(ev.Buf)
			read = true
		} else if ev.Buf != nil {
			ev.Buf.Release()
		}
		if read {
			u.ch.Pipeline().FireChannelReadComplete()
		}
		u.readCallback.Store(false)
		if ev.N == 0 {
			u.deferredFlush = false
			_ = u.Close()
			return
		}
		if u.closed.Load() {
			u.deferredFlush = false
			return
		}
		if err := u.submitAfterReadCompletion(); err != nil {
			u.failFlush(err)
		}
	case transport.OpWrite:
		if ev.Buf != nil {
			ev.Buf.Release()
		}
		for _, buf := range ev.Bufs {
			buf.Release()
		}
		u.writePending = false
		if ev.Err != nil {
			u.fail(ev.Err)
			return
		}
		u.completeWrite(int64(ev.N))
		if !u.closed.Load() {
			if err := u.runPendingFlush(); err != nil {
				u.failFlush(err)
			}
		}
	case transport.OpClose:
		u.finishClose()
	}
}

func (u *Unsafe) fail(err error) {
	u.ch.Pipeline().FireExceptionCaught(err)
	_ = u.Close()
}
