package pool

import (
	"context"
	"sync"

	"gnalloy.org/gnalloy/channel"
)

const defaultMaxIdle = 16

type Factory func(context.Context) (channel.Channel, error)

type HealthCheck func(channel.Channel) bool

type Config struct {
	Factory     Factory
	HealthCheck HealthCheck
	MaxIdle     int
	// Handler 接收 Channel 创建、借出、归还生命周期回调。
	Handler any
}

// Pool 复用可安全再次借出的 Channel。
type Pool struct {
	factory Factory
	health  HealthCheck
	maxIdle int
	handler any

	mu     sync.Mutex
	idle   []channel.Channel
	closed bool
}

func New(cfg Config) (*Pool, error) {
	if cfg.Factory == nil || cfg.MaxIdle < 0 {
		return nil, ErrInvalidConfig
	}
	maxIdle := cfg.MaxIdle
	if maxIdle == 0 {
		maxIdle = defaultMaxIdle
	}
	health := cfg.HealthCheck
	if health == nil {
		health = func(channel.Channel) bool { return true }
	}
	return &Pool{factory: cfg.Factory, health: health, maxIdle: maxIdle, handler: cfg.Handler, idle: make([]channel.Channel, 0, maxIdle)}, nil
}

func (p *Pool) Get(ctx context.Context) (channel.Channel, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		ch, ok, err := p.popIdle()
		if err != nil {
			return nil, err
		}
		if !ok {
			return p.newChannel(ctx)
		}
		if p.health(ch) {
			if err := notifyAcquired(p.handler, ch); err != nil {
				_ = ch.Close()
				return nil, err
			}
			return ch, nil
		}
		_ = ch.Close()
	}
}

func (p *Pool) Put(ch channel.Channel) error {
	if ch == nil {
		return ErrInvalidChannel
	}
	if !p.health(ch) {
		return ch.Close()
	}
	if err := notifyReleased(p.handler, ch); err != nil {
		_ = ch.Close()
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		_ = ch.Close()
		return ErrClosedPool
	}
	if len(p.idle) >= p.maxIdle {
		_ = ch.Close()
		return nil
	}
	p.idle = append(p.idle, ch)
	return nil
}

func (p *Pool) Discard(ch channel.Channel) error {
	if ch == nil {
		return ErrInvalidChannel
	}
	return ch.Close()
}

func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	idle := append([]channel.Channel(nil), p.idle...)
	p.idle = nil
	p.mu.Unlock()

	var first error
	for _, ch := range idle {
		if err := ch.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (p *Pool) Len() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

func (p *Pool) popIdle() (channel.Channel, bool, error) {
	if p == nil {
		return nil, false, ErrClosedPool
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false, ErrClosedPool
	}
	n := len(p.idle)
	if n == 0 {
		return nil, false, nil
	}
	ch := p.idle[n-1]
	p.idle[n-1] = nil
	p.idle = p.idle[:n-1]
	return ch, true, nil
}

func (p *Pool) newChannel(ctx context.Context) (channel.Channel, error) {
	ch, err := p.factory(ctx)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		_ = ch.Close()
		return nil, ErrClosedPool
	}
	if err := notifyCreated(p.handler, ch); err != nil {
		_ = ch.Close()
		return nil, err
	}
	if err := notifyAcquired(p.handler, ch); err != nil {
		_ = ch.Close()
		return nil, err
	}
	return ch, nil
}
