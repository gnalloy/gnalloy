package transport

import "sync"

type eventLoopLocalRegistry struct {
	mu      sync.Mutex
	closers []func() error
}

func (r *eventLoopLocalRegistry) add(loop *EventLoop, closer func() error) error {
	if closer == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if loop == nil || loop.closed.Load() {
		return ErrEventLoopClosed
	}
	r.closers = append(r.closers, closer)
	return nil
}

func (r *eventLoopLocalRegistry) closeAll() error {
	r.mu.Lock()
	closers := r.closers
	r.closers = nil
	r.mu.Unlock()

	var first error
	for _, closer := range closers {
		if err := closer(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
