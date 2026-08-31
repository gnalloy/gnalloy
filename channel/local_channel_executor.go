package channel

import (
	"gnalloy.org/gnalloy/message"
	"gnalloy.org/gnalloy/transport"
)

type boundEventExecutor struct {
	executor FutureListenerExecutor
}

// BindEventExecutor 绑定 Channel 的 owner EventLoop。
//
// 绑定后，公开 Channel 出站和手动读操作都会先进入 owner loop，避免业务 goroutine
// 直接触碰传输层队列状态。
func (c *LocalChannel) BindEventExecutor(executor interface{ Submit(transport.Task) error }) {
	if c == nil || executor == nil {
		return
	}
	var listenerExecutor FutureListenerExecutor = executor
	c.eventExecutor.Store(boundEventExecutor{executor: listenerExecutor})
}

func (c *LocalChannel) ownerExecutor() FutureListenerExecutor {
	if c == nil {
		return nil
	}
	bound, ok := c.eventExecutor.Load().(boundEventExecutor)
	if !ok {
		return nil
	}
	return bound.executor
}

func (c *LocalChannel) submitOwnerFuture(executor FutureListenerExecutor, releaseOnReject any, op func() Future) Future {
	promise := NewPromiseWithExecutor(executor)
	if err := executor.Submit(func() {
		completePromiseFromFuture(promise, op())
	}); err != nil {
		releaseMessage(releaseOnReject)
		promise.SetFailure(err)
	}
	return promise
}

func completePromiseFromFuture(promise Promise, future Future) {
	if promise == nil {
		return
	}
	if future == nil {
		promise.SetSuccess()
		return
	}
	if future.IsDone() {
		completePromise(promise, future.Err())
		return
	}
	future.AddListener(func(done Future) {
		completePromise(promise, done.Err())
	})
}

func completePromise(promise Promise, err error) {
	if err != nil {
		promise.SetFailure(err)
		return
	}
	promise.SetSuccess()
}

func releaseMessage(msg any) {
	message.Release(msg)
}
