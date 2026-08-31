package transport

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrInvalidEventLoopGroup = errors.New("gnalloy/transport: invalid event loop group")
	ErrEventLoopGroupClosed  = errors.New("gnalloy/transport: event loop group closed")
	ErrEventLoopGroupRunning = errors.New("gnalloy/transport: event loop group already running")
	ErrNoEventLoop           = errors.New("gnalloy/transport: no event loop available")
	ErrUnsupportedAffinity   = errors.New("gnalloy/transport: cpu affinity is unsupported on this platform")
)

// ClockMillis 返回当前毫秒时间，EventLoopGroup 用它驱动本地时间轮。
type ClockMillis func() int64

// PollerFactory 为每个 EventLoop 创建独占 Poller。
type PollerFactory func(index int) (Poller, error)

// RegisterHook 在 fd 注册到目标 EventLoop 后执行，通常用于触发 ChannelActive 和预读。
type RegisterHook func(loop *EventLoop, handler EventHandler) error

// RegisterErrorHandler 处理异步注册失败，避免错误在跨线程投递后丢失。
type RegisterErrorHandler func(loop *EventLoop, handler EventHandler, err error)

type EventLoopGroupConfig struct {
	ID EventLoopGroupID

	Size          int
	PollerFactory PollerFactory
	PollerConfig  Config

	TaskQueueSize   uint64
	TimerTickMillis int64
	TimerWheelSize  uint64
	StartMillis     int64
	EventBatchSize  int

	Clock ClockMillis

	// CPUAffinity 按 EventLoop 下标循环绑定 CPU。为空时不绑核。
	CPUAffinity []int
}

// EventLoopGroup 管理一组固定 EventLoop，并提供 Round-Robin 分配。
type EventLoopGroup struct {
	id    EventLoopGroupID
	loops []*EventLoop
	clock ClockMillis

	next    atomic.Uint64
	running atomic.Bool
	closed  atomic.Bool

	mu      sync.Mutex
	runCtx  context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	first   error
	waiters sync.WaitGroup
}

func NewEventLoopGroup(cfg EventLoopGroupConfig) (*EventLoopGroup, error) {
	if cfg.Size <= 0 {
		return nil, ErrInvalidEventLoopGroup
	}
	for _, cpu := range cfg.CPUAffinity {
		if cpu < 0 {
			return nil, ErrInvalidEventLoopGroup
		}
	}
	clock := cfg.Clock
	if clock == nil {
		clock = func() int64 { return time.Now().UnixMilli() }
	}
	pollerFactory := cfg.PollerFactory
	if pollerFactory == nil {
		pollerConfig := cfg.PollerConfig
		pollerFactory = func(int) (Poller, error) {
			return NewPoller(pollerConfig)
		}
	}

	startMillis := cfg.StartMillis
	if startMillis == 0 {
		startMillis = clock()
	}
	loops := make([]*EventLoop, 0, cfg.Size)
	for i := 0; i < cfg.Size; i++ {
		p, err := pollerFactory(i)
		if err != nil {
			closeEventLoops(loops)
			return nil, err
		}
		loop, err := NewEventLoop(EventLoopConfig{
			ID:              EventLoopID(i),
			Poller:          p,
			TaskQueueSize:   cfg.TaskQueueSize,
			TimerTickMillis: cfg.TimerTickMillis,
			TimerWheelSize:  cfg.TimerWheelSize,
			StartMillis:     startMillis,
			EventBatchSize:  cfg.EventBatchSize,
			CPUAffinity:     cpuAffinityAt(cfg.CPUAffinity, i),
			PinCPU:          len(cfg.CPUAffinity) > 0,
		})
		if err != nil {
			_ = p.Close()
			closeEventLoops(loops)
			return nil, err
		}
		loops = append(loops, loop)
	}

	return &EventLoopGroup{id: cfg.ID, loops: loops, clock: clock}, nil
}

func (g *EventLoopGroup) ID() EventLoopGroupID {
	if g == nil {
		return 0
	}
	return g.id
}

func (g *EventLoopGroup) Size() int {
	if g == nil {
		return 0
	}
	return len(g.loops)
}

func (g *EventLoopGroup) Loops() []*EventLoop {
	if g == nil {
		return nil
	}
	out := make([]*EventLoop, len(g.loops))
	copy(out, g.loops)
	return out
}

func (g *EventLoopGroup) Next() (*EventLoop, error) {
	if g == nil || len(g.loops) == 0 {
		return nil, ErrNoEventLoop
	}
	idx := g.next.Add(1) - 1
	return g.loops[idx%uint64(len(g.loops))], nil
}

func (g *EventLoopGroup) IsRunning() bool {
	return g != nil && g.running.Load()
}

func (g *EventLoopGroup) IsClosed() bool {
	return g == nil || g.closed.Load()
}

func (g *EventLoopGroup) Start(ctx context.Context) error {
	if g == nil || len(g.loops) == 0 {
		return ErrNoEventLoop
	}
	if g.closed.Load() {
		return ErrEventLoopGroupClosed
	}
	if !g.running.CompareAndSwap(false, true) {
		return ErrEventLoopGroupRunning
	}
	if ctx == nil {
		ctx = context.Background()
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})

	g.mu.Lock()
	g.runCtx = runCtx
	g.cancel = cancel
	g.done = done
	g.first = nil
	g.mu.Unlock()

	for _, loop := range g.loops {
		loop := loop
		g.waiters.Add(1)
		go func() {
			defer g.waiters.Done()
			if err := loop.Run(runCtx, g.clock); err != nil && !isExpectedLoopStop(err) {
				g.storeFirstError(err)
				cancel()
			}
		}()
	}

	go func() {
		g.waiters.Wait()
		g.running.Store(false)
		close(done)
	}()
	return nil
}

func (g *EventLoopGroup) Submit(task Task) (*EventLoop, error) {
	loop, err := g.Next()
	if err != nil {
		return nil, err
	}
	return loop, loop.Submit(task)
}

func (g *EventLoopGroup) Invoke(ctx context.Context, task func() error) (*EventLoop, error) {
	loop, err := g.Next()
	if err != nil {
		return nil, err
	}
	return loop, loop.Invoke(ctx, task)
}

func (g *EventLoopGroup) RegisterNext(ctx context.Context, handler EventHandler, interest ReadyMask, after RegisterHook) (*EventLoop, error) {
	if handler == nil {
		return nil, ErrInvalidIORequest
	}
	loop, err := g.Next()
	if err != nil {
		return nil, err
	}
	err = loop.Invoke(ctx, func() error {
		if err := loop.Register(handler, interest); err != nil {
			return err
		}
		if after != nil {
			return after(loop, handler)
		}
		return nil
	})
	return loop, err
}

func (g *EventLoopGroup) ScheduleRegisterNext(handler EventHandler, interest ReadyMask, after RegisterHook, onError RegisterErrorHandler) (*EventLoop, error) {
	if handler == nil {
		return nil, ErrInvalidIORequest
	}
	loop, err := g.Next()
	if err != nil {
		return nil, err
	}
	err = loop.Submit(func() {
		err := loop.Register(handler, interest)
		if err == nil && after != nil {
			err = after(loop, handler)
		}
		if err != nil {
			if onError != nil {
				onError(loop, handler, err)
				return
			}
			handler.HandleEvent(PollEvent{Model: loop.Poller().Model(), Op: OpClose, FD: handler.FD(), ChannelID: handler.ID(), Err: err})
		}
	})
	return loop, err
}

func (g *EventLoopGroup) Shutdown(ctx context.Context) error {
	if g == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	g.mu.Lock()
	cancel := g.cancel
	done := g.done
	g.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, loop := range g.loops {
		_ = loop.Poller().Wakeup()
	}

	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	err := closeEventLoops(g.loops)
	g.closed.Store(true)
	if first := g.firstError(); first != nil && err == nil {
		return first
	}
	return err
}

func (g *EventLoopGroup) Close() error {
	return g.Shutdown(context.Background())
}

func (g *EventLoopGroup) storeFirstError(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.first == nil {
		g.first = err
	}
}

func (g *EventLoopGroup) firstError() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.first
}

func closeEventLoops(loops []*EventLoop) error {
	var first error
	for _, loop := range loops {
		if loop == nil {
			continue
		}
		if err := loop.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func cpuAffinityAt(cpus []int, index int) int {
	if len(cpus) == 0 {
		return 0
	}
	return cpus[index%len(cpus)]
}

func isExpectedLoopStop(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrEventLoopClosed) ||
		errors.Is(err, ErrClosedPoller)
}
