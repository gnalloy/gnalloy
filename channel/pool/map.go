package pool

import (
	"context"
	"sync"

	"gnalloy.org/gnalloy/channel"
)

// ChannelPool 是 Pool、SimplePool、FixedPool 的公共最小契约。
type ChannelPool interface {
	Get(context.Context) (channel.Channel, error)
	Put(channel.Channel) error
	Discard(channel.Channel) error
	Close() error
	Len() int
}

// MapFactory 按 key 创建独立 ChannelPool。
type MapFactory[K comparable] func(K) (ChannelPool, error)

// Map 按 key 懒加载并复用 ChannelPool。
type Map[K comparable] struct {
	factory MapFactory[K]

	mu     sync.Mutex
	pools  map[K]ChannelPool
	closed bool
}

// NewMap 创建 ChannelPool 映射。
func NewMap[K comparable](factory MapFactory[K]) (*Map[K], error) {
	if factory == nil {
		return nil, ErrInvalidConfig
	}
	return &Map[K]{factory: factory, pools: make(map[K]ChannelPool, 8)}, nil
}

// Get 返回指定 key 对应的 ChannelPool，缺失时按 factory 创建。
func (m *Map[K]) Get(key K) (ChannelPool, error) {
	if m == nil {
		return nil, ErrClosedPool
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrClosedPool
	}
	if p := m.pools[key]; p != nil {
		m.mu.Unlock()
		return p, nil
	}
	m.mu.Unlock()

	created, err := m.factory(key)
	if err != nil {
		return nil, err
	}
	if created == nil {
		return nil, ErrInvalidConfig
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		_ = created.Close()
		return nil, ErrClosedPool
	}
	if existing := m.pools[key]; existing != nil {
		_ = created.Close()
		return existing, nil
	}
	m.pools[key] = created
	return created, nil
}

// Remove 移除并关闭指定 key 的 ChannelPool。
func (m *Map[K]) Remove(key K) error {
	if m == nil {
		return ErrClosedPool
	}
	m.mu.Lock()
	p := m.pools[key]
	delete(m.pools, key)
	m.mu.Unlock()
	if p == nil {
		return nil
	}
	return p.Close()
}

// Close 关闭全部子池。
func (m *Map[K]) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	pools := make([]ChannelPool, 0, len(m.pools))
	for key, p := range m.pools {
		delete(m.pools, key)
		pools = append(pools, p)
	}
	m.mu.Unlock()

	var first error
	for _, p := range pools {
		if err := p.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Len 返回当前已创建子池数量。
func (m *Map[K]) Len() int {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pools)
}
