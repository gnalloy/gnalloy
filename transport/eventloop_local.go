package transport

import (
	"errors"
	"io"
	"sort"
	"sync"
)

var (
	ErrInvalidEventLoopLocal = errors.New("gnalloy/transport: invalid event loop local")
	ErrEventLoopLocalClosed  = errors.New("gnalloy/transport: event loop local closed")
)

// EventLoopLocalFactory 为目标 EventLoop 创建独占本地资源。
// 工厂只运行在控制面路径；返回值会被缓存，热路径应复用 Get 返回的对象。
type EventLoopLocalFactory[T any] func(loop *EventLoop) (T, error)

// EventLoopLocalSnapshot 是 EventLoop-local 资源的只读观测快照。
type EventLoopLocalSnapshot[T any] struct {
	EventLoopID EventLoopID
	Value       T
}

type eventLoopLocalEntry[T any] struct {
	loop  *EventLoop
	value T
}

// EventLoopLocal 为每个 EventLoop 懒加载并缓存一个本地资源。
// 该类型用于 allocator、观测探针、协议私有缓存等必须跟 EventLoop 对齐的低共享状态。
type EventLoopLocal[T any] struct {
	mu      sync.Mutex
	factory EventLoopLocalFactory[T]
	values  map[*EventLoop]eventLoopLocalEntry[T]
	closed  bool
}

// NewEventLoopLocal 创建 EventLoop-local 资源容器。
func NewEventLoopLocal[T any](factory EventLoopLocalFactory[T]) (*EventLoopLocal[T], error) {
	if factory == nil {
		return nil, ErrInvalidEventLoopLocal
	}
	return &EventLoopLocal[T]{
		factory: factory,
		values:  make(map[*EventLoop]eventLoopLocalEntry[T]),
	}, nil
}

// Get 返回 loop 对应的本地资源，首次访问时调用工厂创建。
func (l *EventLoopLocal[T]) Get(loop *EventLoop) (T, error) {
	var zero T
	if l == nil || l.factory == nil {
		return zero, ErrInvalidEventLoopLocal
	}
	if loop == nil {
		return zero, ErrNoEventLoop
	}
	if loop.closed.Load() {
		return zero, ErrEventLoopClosed
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return zero, ErrEventLoopLocalClosed
	}
	if entry, ok := l.values[loop]; ok {
		return entry.value, nil
	}
	value, err := l.factory(loop)
	if err != nil {
		return zero, err
	}
	if err := loop.locals.add(loop, func() error {
		return l.closeLoop(loop)
	}); err != nil {
		_ = closeEventLoopLocalValue(value)
		return zero, err
	}
	l.values[loop] = eventLoopLocalEntry[T]{loop: loop, value: value}
	return value, nil
}

// Len 返回当前已创建的 EventLoop-local 资源数量。
func (l *EventLoopLocal[T]) Len() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.values)
}

// Snapshot 返回按 EventLoopID 排序的只读快照。
func (l *EventLoopLocal[T]) Snapshot() []EventLoopLocalSnapshot[T] {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]EventLoopLocalSnapshot[T], 0, len(l.values))
	for _, entry := range l.values {
		out = append(out, EventLoopLocalSnapshot[T]{
			EventLoopID: entry.loop.ID(),
			Value:       entry.value,
		})
	}
	sort.Slice(out, func(i int, j int) bool {
		return out[i].EventLoopID < out[j].EventLoopID
	})
	return out
}

// Close 关闭全部已创建资源；实现 io.Closer 的资源会被顺序释放。
func (l *EventLoopLocal[T]) Close() error {
	if l == nil {
		return nil
	}
	values := l.drain()
	var first error
	for _, entry := range values {
		if err := closeEventLoopLocalValue(entry.value); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (l *EventLoopLocal[T]) drain() []eventLoopLocalEntry[T] {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	values := make([]eventLoopLocalEntry[T], 0, len(l.values))
	for loop, entry := range l.values {
		delete(l.values, loop)
		values = append(values, entry)
	}
	return values
}

func (l *EventLoopLocal[T]) closeLoop(loop *EventLoop) error {
	if l == nil || loop == nil {
		return nil
	}
	l.mu.Lock()
	entry, ok := l.values[loop]
	if ok {
		delete(l.values, loop)
	}
	l.mu.Unlock()
	if !ok {
		return nil
	}
	return closeEventLoopLocalValue(entry.value)
}

func closeEventLoopLocalValue[T any](value T) error {
	closer, ok := any(value).(io.Closer)
	if !ok {
		return nil
	}
	return closer.Close()
}
