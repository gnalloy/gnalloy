package pool

import (
	"context"
	"sync"
	"time"

	"gnalloy.org/gnalloy/channel"
)

const defaultMaxPendingAcquire = 1024

// FixedConfig 描述固定容量 ChannelPool。
type FixedConfig struct {
	// Factory 创建新 Channel。
	Factory Factory
	// HealthCheck 判断 Channel 是否可复用。
	HealthCheck HealthCheck
	// MaxConnections 限制 borrowed 与 idle Channel 总数。
	MaxConnections int
	// MaxIdle 限制空闲 Channel 数量，0 表示等于 MaxConnections。
	MaxIdle int
	// MaxPendingAcquire 限制等待队列长度，0 表示使用默认上限。
	MaxPendingAcquire int
	// AcquireTimeout 限制排队等待时间，0 表示只受调用方 context 控制。
	AcquireTimeout time.Duration
	// Handler 接收 Channel 创建、借出、归还生命周期回调。
	Handler any
}

// FixedStats 是 FixedPool 的低基数容量快照。
type FixedStats struct {
	Total          int
	Idle           int
	PendingAcquire int
	MaxConnections int
}

// FixedPool 限制可打开 Channel 总数，并为超限 acquire 提供有界等待队列。
type FixedPool struct {
	factory        Factory
	health         HealthCheck
	maxConnections int
	maxIdle        int
	maxPending     int
	acquireTimeout time.Duration
	handler        any

	mu      sync.Mutex
	total   int
	idle    []channel.Channel
	pending []*pendingAcquire
	closed  bool
}

type pendingAcquire struct {
	result chan acquireResult
	done   bool
}

type acquireResult struct {
	ch     channel.Channel
	create bool
	err    error
}

// NewFixed 创建固定容量 ChannelPool。
func NewFixed(cfg FixedConfig) (*FixedPool, error) {
	if cfg.Factory == nil || cfg.MaxConnections <= 0 || cfg.MaxIdle < 0 || cfg.MaxPendingAcquire < 0 {
		return nil, ErrInvalidConfig
	}
	maxIdle := cfg.MaxIdle
	if maxIdle == 0 || maxIdle > cfg.MaxConnections {
		maxIdle = cfg.MaxConnections
	}
	maxPending := cfg.MaxPendingAcquire
	if maxPending == 0 {
		maxPending = defaultMaxPendingAcquire
	}
	health := cfg.HealthCheck
	if health == nil {
		health = func(channel.Channel) bool { return true }
	}
	return &FixedPool{
		factory:        cfg.Factory,
		health:         health,
		maxConnections: cfg.MaxConnections,
		maxIdle:        maxIdle,
		maxPending:     maxPending,
		acquireTimeout: cfg.AcquireTimeout,
		handler:        cfg.Handler,
		idle:           make([]channel.Channel, 0, maxIdle),
	}, nil
}

// Get 获取一个可用 Channel，超过容量时进入有界等待队列。
func (p *FixedPool) Get(ctx context.Context) (channel.Channel, error) {
	if p == nil {
		return nil, ErrClosedPool
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if p.acquireTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.acquireTimeout)
		defer cancel()
	}
	for {
		ch, create, waiter, stale, err := p.reserveAcquire()
		closeChannels(stale)
		if err != nil {
			return nil, err
		}
		if ch != nil {
			if err := notifyAcquired(p.handler, ch); err != nil {
				_ = ch.Close()
				p.releaseCapacity()
				return nil, err
			}
			return ch, nil
		}
		if create {
			return p.createReserved(ctx)
		}
		acquired, err := p.awaitAcquire(ctx, waiter)
		if err != nil {
			return nil, err
		}
		if acquired != nil {
			if err := notifyAcquired(p.handler, acquired); err != nil {
				_ = acquired.Close()
				p.releaseCapacity()
				return nil, err
			}
			return acquired, nil
		}
	}
}

// Put 归还 Channel，优先交给等待中的 acquire。
func (p *FixedPool) Put(ch channel.Channel) error {
	if p == nil {
		return ErrClosedPool
	}
	if ch == nil {
		return ErrInvalidChannel
	}
	if !p.health(ch) {
		_ = ch.Close()
		p.releaseCapacity()
		return nil
	}
	if err := notifyReleased(p.handler, ch); err != nil {
		_ = ch.Close()
		p.releaseCapacity()
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.decrementTotalLocked()
		p.mu.Unlock()
		_ = ch.Close()
		return ErrClosedPool
	}
	if p.deliverChannelLocked(ch) {
		p.mu.Unlock()
		return nil
	}
	if len(p.idle) < p.maxIdle {
		p.idle = append(p.idle, ch)
		p.mu.Unlock()
		return nil
	}
	p.decrementTotalLocked()
	p.wakeCreatorLocked()
	p.mu.Unlock()
	return ch.Close()
}

// Discard 丢弃不可复用 Channel，并释放一个容量名额。
func (p *FixedPool) Discard(ch channel.Channel) error {
	if p == nil {
		return ErrClosedPool
	}
	if ch == nil {
		return ErrInvalidChannel
	}
	err := ch.Close()
	p.releaseCapacity()
	return err
}

// Close 关闭 idle Channel，并唤醒所有等待中的 acquire。
func (p *FixedPool) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	idle := append([]channel.Channel(nil), p.idle...)
	p.idle = nil
	p.decreaseTotalByLocked(len(idle))
	pending := append([]*pendingAcquire(nil), p.pending...)
	p.pending = nil
	for _, waiter := range pending {
		if waiter.done {
			continue
		}
		waiter.done = true
		waiter.result <- acquireResult{err: ErrClosedPool}
	}
	p.mu.Unlock()
	return closeChannels(idle)
}

// Len 返回当前 idle Channel 数量。
func (p *FixedPool) Len() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.idle)
}

// Stats 返回固定池容量快照。
func (p *FixedPool) Stats() FixedStats {
	if p == nil {
		return FixedStats{}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return FixedStats{
		Total:          p.total,
		Idle:           len(p.idle),
		PendingAcquire: len(p.pending),
		MaxConnections: p.maxConnections,
	}
}

func (p *FixedPool) reserveAcquire() (channel.Channel, bool, *pendingAcquire, []channel.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, false, nil, nil, ErrClosedPool
	}
	stale := make([]channel.Channel, 0)
	for len(p.idle) > 0 {
		ch := p.idle[len(p.idle)-1]
		p.idle[len(p.idle)-1] = nil
		p.idle = p.idle[:len(p.idle)-1]
		if p.health(ch) {
			return ch, false, nil, stale, nil
		}
		stale = append(stale, ch)
		p.decrementTotalLocked()
	}
	if p.total < p.maxConnections {
		p.total++
		return nil, true, nil, stale, nil
	}
	if len(p.pending) >= p.maxPending {
		return nil, false, nil, stale, ErrAcquireQueueFull
	}
	waiter := &pendingAcquire{result: make(chan acquireResult, 1)}
	p.pending = append(p.pending, waiter)
	return nil, false, waiter, stale, nil
}

func (p *FixedPool) createReserved(ctx context.Context) (channel.Channel, error) {
	ch, err := p.factory(ctx)
	if err != nil {
		p.releaseCapacity()
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.decrementTotalLocked()
		p.mu.Unlock()
		_ = ch.Close()
		return nil, ErrClosedPool
	}
	p.mu.Unlock()
	if err := notifyCreated(p.handler, ch); err != nil {
		_ = ch.Close()
		p.releaseCapacity()
		return nil, err
	}
	if err := notifyAcquired(p.handler, ch); err != nil {
		_ = ch.Close()
		p.releaseCapacity()
		return nil, err
	}
	return ch, nil
}

func (p *FixedPool) awaitAcquire(ctx context.Context, waiter *pendingAcquire) (channel.Channel, error) {
	select {
	case result := <-waiter.result:
		return p.resolveAcquire(ctx, result)
	case <-ctx.Done():
		select {
		case result := <-waiter.result:
			return p.resolveAcquire(ctx, result)
		default:
		}
		if p.cancelAcquire(waiter) {
			return nil, ctx.Err()
		}
		result := <-waiter.result
		return p.resolveAcquire(ctx, result)
	}
}

func (p *FixedPool) resolveAcquire(ctx context.Context, result acquireResult) (channel.Channel, error) {
	if result.err != nil {
		return nil, result.err
	}
	if result.ch != nil {
		return result.ch, nil
	}
	if result.create {
		return p.createReserved(ctx)
	}
	return nil, ErrClosedPool
}

func (p *FixedPool) cancelAcquire(waiter *pendingAcquire) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if waiter.done {
		return false
	}
	for i, pending := range p.pending {
		if pending != waiter {
			continue
		}
		waiter.done = true
		copy(p.pending[i:], p.pending[i+1:])
		p.pending[len(p.pending)-1] = nil
		p.pending = p.pending[:len(p.pending)-1]
		return true
	}
	return false
}

func (p *FixedPool) deliverChannelLocked(ch channel.Channel) bool {
	for len(p.pending) > 0 {
		waiter := p.pending[0]
		copy(p.pending[0:], p.pending[1:])
		p.pending[len(p.pending)-1] = nil
		p.pending = p.pending[:len(p.pending)-1]
		if waiter.done {
			continue
		}
		waiter.done = true
		waiter.result <- acquireResult{ch: ch}
		return true
	}
	return false
}

func (p *FixedPool) releaseCapacity() {
	p.mu.Lock()
	p.decrementTotalLocked()
	p.wakeCreatorLocked()
	p.mu.Unlock()
}

func (p *FixedPool) wakeCreatorLocked() {
	if p.closed || p.total >= p.maxConnections {
		return
	}
	for len(p.pending) > 0 {
		waiter := p.pending[0]
		copy(p.pending[0:], p.pending[1:])
		p.pending[len(p.pending)-1] = nil
		p.pending = p.pending[:len(p.pending)-1]
		if waiter.done {
			continue
		}
		p.total++
		waiter.done = true
		waiter.result <- acquireResult{create: true}
		return
	}
}

func (p *FixedPool) decrementTotalLocked() {
	p.decreaseTotalByLocked(1)
}

func (p *FixedPool) decreaseTotalByLocked(n int) {
	p.total -= n
	if p.total < 0 {
		p.total = 0
	}
}

func closeChannels(channels []channel.Channel) error {
	var first error
	for _, ch := range channels {
		if ch == nil {
			continue
		}
		if err := ch.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
