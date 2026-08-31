package channel

import (
	"sync"
	"sync/atomic"
	"time"

	"gnalloy.org/gnalloy/transport"
)

// Future 表示异步 Channel 操作的最终结果。
type Future interface {
	Done() <-chan struct{}
	IsDone() bool
	IsSuccess() bool
	Err() error
	Cause() error
	Await() error
	AwaitTimeout(timeout time.Duration) (bool, error)
	AddListener(func(Future)) Future
	AddListenerHandle(func(Future)) FutureListenerHandle
	RemoveListener(FutureListenerHandle) bool
}

// Promise 是框架内部可完成的 Future。
type Promise interface {
	Future
	SetSuccess() bool
	SetFailure(error) bool
}

// FutureListenerExecutor 抽象 Future listener 的执行器。
//
// Channel 相关 Future 应绑定到所属 EventLoop，避免 listener 在完成线程或调用线程中
// 直接运行，保持和 Netty EventExecutor 语义一致。
type FutureListenerExecutor interface {
	Submit(transport.Task) error
}

// DefaultPromise 是无外部依赖的 Future/Promise 实现。
type DefaultPromise struct {
	done      chan struct{}
	once      sync.Once
	mu        sync.Mutex
	err       error
	completed bool
	executor  FutureListenerExecutor
	nextID    atomic.Uint64
	listeners []futureListenerEntry
}

// FutureListenerHandle 是监听器注册句柄，用于在 Future 完成前移除监听器。
type FutureListenerHandle struct {
	id uint64
}

type futureListenerEntry struct {
	handle   FutureListenerHandle
	listener func(Future)
}

func NewPromise() *DefaultPromise {
	return &DefaultPromise{done: make(chan struct{})}
}

func NewPromiseWithExecutor(executor FutureListenerExecutor) *DefaultPromise {
	promise := NewPromise()
	promise.executor = executor
	return promise
}

func SucceededFuture() Future {
	p := NewPromise()
	p.SetSuccess()
	return p
}

func FailedFuture(err error) Future {
	p := NewPromise()
	p.SetFailure(err)
	return p
}

func (p *DefaultPromise) Done() <-chan struct{} {
	return p.done
}

func (p *DefaultPromise) IsDone() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *DefaultPromise) IsSuccess() bool {
	return p.IsDone() && p.Err() == nil
}

func (p *DefaultPromise) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *DefaultPromise) Cause() error {
	return p.Err()
}

func (p *DefaultPromise) Await() error {
	<-p.done
	return p.Err()
}

func (p *DefaultPromise) AwaitTimeout(timeout time.Duration) (bool, error) {
	if timeout <= 0 {
		if !p.IsDone() {
			return false, nil
		}
		return true, p.Err()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.done:
		return true, p.Err()
	case <-timer.C:
		return false, nil
	}
}

func (p *DefaultPromise) AddListener(listener func(Future)) Future {
	p.AddListenerHandle(listener)
	return p
}

func (p *DefaultPromise) AddListenerHandle(listener func(Future)) FutureListenerHandle {
	if listener == nil {
		return FutureListenerHandle{}
	}
	handle := FutureListenerHandle{id: p.nextID.Add(1)}
	callNow := false
	executor := p.listenerExecutor()
	p.mu.Lock()
	if p.completed {
		callNow = true
		executor = p.executor
	} else {
		p.listeners = append(p.listeners, futureListenerEntry{handle: handle, listener: listener})
	}
	p.mu.Unlock()
	if callNow {
		p.dispatchListener(executor, listener)
	}
	return handle
}

func (p *DefaultPromise) RemoveListener(handle FutureListenerHandle) bool {
	if handle.id == 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.completed {
		return false
	}
	for i, entry := range p.listeners {
		if entry.handle == handle {
			copy(p.listeners[i:], p.listeners[i+1:])
			p.listeners[len(p.listeners)-1] = futureListenerEntry{}
			p.listeners = p.listeners[:len(p.listeners)-1]
			return true
		}
	}
	return false
}

func (p *DefaultPromise) SetSuccess() bool {
	return p.complete(nil)
}

func (p *DefaultPromise) SetFailure(err error) bool {
	if err == nil {
		err = ErrPromiseFailed
	}
	return p.complete(err)
}

// SetListenerExecutor 设置后续 listener 的执行器。
//
// 该方法用于 Channel 注册到 EventLoop 后补齐 closeFuture 等早期创建 Promise 的
// 执行归属。已经派发出去的 listener 不会被迁移。
func (p *DefaultPromise) SetListenerExecutor(executor FutureListenerExecutor) {
	p.mu.Lock()
	p.executor = executor
	p.mu.Unlock()
}

func (p *DefaultPromise) complete(err error) bool {
	completed := false
	p.once.Do(func() {
		p.mu.Lock()
		p.err = err
		p.completed = true
		executor := p.executor
		listeners := append([]futureListenerEntry{}, p.listeners...)
		p.listeners = nil
		p.mu.Unlock()

		close(p.done)
		for _, listener := range listeners {
			p.dispatchListener(executor, listener.listener)
		}
		completed = true
	})
	return completed
}

func (p *DefaultPromise) listenerExecutor() FutureListenerExecutor {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.executor
}

func (p *DefaultPromise) dispatchListener(executor FutureListenerExecutor, listener func(Future)) {
	if executor == nil {
		listener(p)
		return
	}
	if err := executor.Submit(transport.Task(func() {
		listener(p)
	})); err != nil {
		// EventLoop 关闭或队列拒绝时不能丢失 listener；此时退回同步执行。
		listener(p)
	}
}
