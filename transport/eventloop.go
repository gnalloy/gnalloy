package transport

import (
	"context"
	"errors"
	"sync/atomic"

	"gnalloy.org/gnalloy/queue"
	"gnalloy.org/gnalloy/timer"
)

var (
	ErrEventLoopClosed = errors.New("gnalloy/transport: event loop closed")
	ErrTaskQueueFull   = errors.New("gnalloy/transport: task queue full")
)

type Task func()

type EventHandler interface {
	ID() ChannelID
	FD() FDRef
	HandleEvent(ev PollEvent)
	Close() error
}

type registrationAware interface {
	MarkRegistered()
	MarkDeregistered()
}

type eventExecutorAware interface {
	BindEventExecutor(interface{ Submit(Task) error })
}

type EventLoopConfig struct {
	ID              EventLoopID
	Poller          Poller
	TaskQueueSize   uint64
	TimerTickMillis int64
	TimerWheelSize  uint64
	StartMillis     int64
	EventBatchSize  int
	CPUAffinity     int
	PinCPU          bool
}

type EventLoop struct {
	id          EventLoopID
	poller      Poller
	tasks       *queue.MPSC[Task]
	tailTasks   *queue.MPSC[Task]
	timer       *timer.Wheel
	events      []PollEvent
	cpuAffinity int
	pinCPU      bool

	channels map[ChannelID]EventHandler
	locals   eventLoopLocalRegistry
	closed   atomic.Bool

	taskWakeupPending atomic.Bool
	executing         atomic.Bool
}

func NewEventLoop(cfg EventLoopConfig) (*EventLoop, error) {
	if cfg.Poller == nil {
		return nil, ErrUnsupportedPoller
	}
	taskQueueSize := cfg.TaskQueueSize
	if taskQueueSize == 0 {
		taskQueueSize = 1024
	}
	eventBatchSize := cfg.EventBatchSize
	if eventBatchSize <= 0 {
		eventBatchSize = 1024
	}
	tickMillis := cfg.TimerTickMillis
	if tickMillis <= 0 {
		tickMillis = 100
	}
	wheelSize := cfg.TimerWheelSize
	if wheelSize == 0 {
		wheelSize = 1024
	}
	tw, err := timer.NewWheel(tickMillis, wheelSize, cfg.StartMillis)
	if err != nil {
		return nil, err
	}
	return &EventLoop{
		id:          cfg.ID,
		poller:      cfg.Poller,
		tasks:       queue.NewMPSC[Task](taskQueueSize),
		tailTasks:   queue.NewMPSC[Task](taskQueueSize),
		timer:       tw,
		events:      make([]PollEvent, eventBatchSize),
		cpuAffinity: cfg.CPUAffinity,
		pinCPU:      cfg.PinCPU,
		channels:    make(map[ChannelID]EventHandler, 1024),
	}, nil
}

func (l *EventLoop) ID() EventLoopID {
	return l.id
}

func (l *EventLoop) Poller() Poller {
	return l.poller
}

func (l *EventLoop) Timer() *timer.Wheel {
	return l.timer
}

func (l *EventLoop) Register(ch EventHandler, interest ReadyMask) error {
	if l.closed.Load() {
		return ErrEventLoopClosed
	}
	if ch == nil {
		return ErrInvalidIORequest
	}
	if err := l.poller.Register(ch.FD(), ch.ID(), interest); err != nil {
		return err
	}
	l.channels[ch.ID()] = ch
	if aware, ok := ch.(eventExecutorAware); ok {
		aware.BindEventExecutor(l)
	}
	if aware, ok := ch.(registrationAware); ok {
		aware.MarkRegistered()
	}
	return nil
}

func (l *EventLoop) Deregister(chID ChannelID) error {
	ch := l.channels[chID]
	if ch == nil {
		return nil
	}
	delete(l.channels, chID)
	if aware, ok := ch.(registrationAware); ok {
		aware.MarkDeregistered()
	}
	return l.poller.Deregister(ch.FD())
}

func (l *EventLoop) Submit(task Task) error {
	if task == nil {
		return nil
	}
	if l.closed.Load() {
		return ErrEventLoopClosed
	}
	if !l.tasks.Offer(task) {
		return ErrTaskQueueFull
	}
	return l.signalTaskWakeup()
}

// SubmitAfterBatch 把任务放到当前 EventLoop 批次尾部执行。
//
// 该队列用于 I/O owner 线程内的安全点，例如批量 flush 出站缓冲。调用方不应在
// 这里放入阻塞业务逻辑，否则会直接拖慢 I/O 周期。
func (l *EventLoop) SubmitAfterBatch(task Task) error {
	if task == nil {
		return nil
	}
	if l.closed.Load() {
		return ErrEventLoopClosed
	}
	if !l.tailTasks.Offer(task) {
		return ErrTaskQueueFull
	}
	if l.executing.Load() {
		return nil
	}
	return l.signalTaskWakeup()
}

// Invoke 将控制面任务投递到 EventLoop 并等待执行结果。
// 它用于启动、注册、测试等低频路径，不参与 I/O 热路径。
func (l *EventLoop) Invoke(ctx context.Context, task func() error) error {
	if task == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	if err := l.Submit(func() {
		done <- task()
	}); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *EventLoop) Run(ctx context.Context, nowMillis func() int64) error {
	if l.pinCPU {
		unlock, err := bindOSThreadToCPU(l.cpuAffinity)
		if err != nil {
			return err
		}
		defer unlock()
	}
	if nowMillis == nil {
		nowMillis = func() int64 { return 0 }
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := l.RunOnce(nowMillis()); err != nil && !errors.Is(err, ErrClosedPoller) {
			return err
		}
		if l.closed.Load() {
			return ErrEventLoopClosed
		}
	}
}

func (l *EventLoop) RunOnce(nowMillis int64) error {
	if l.closed.Load() {
		return ErrEventLoopClosed
	}
	l.executing.Store(true)
	l.drainTasks()
	l.drainTailTasks()
	l.timer.Advance(nowMillis, 0)
	timeout := l.timer.NextDelayMillis(nowMillis)
	l.executing.Store(false)
	n, err := l.poller.Poll(l.events, timeout)
	if err != nil {
		return err
	}
	l.executing.Store(true)
	defer l.executing.Store(false)
	for i := 0; i < n; i++ {
		ev := l.events[i]
		if ev.Op == OpWakeup {
			l.drainTasks()
			l.drainTailTasks()
			continue
		}
		if ch := l.channels[ev.ChannelID]; ch != nil {
			ch.HandleEvent(ev)
		}
	}
	l.drainTailTasks()
	l.drainTasks()
	l.drainTailTasks()
	return nil
}

func (l *EventLoop) Close() error {
	if l.closed.Swap(true) {
		return nil
	}
	for id, ch := range l.channels {
		delete(l.channels, id)
		if aware, ok := ch.(registrationAware); ok {
			aware.MarkDeregistered()
		}
		_ = ch.Close()
	}
	var first error
	if err := l.locals.closeAll(); err != nil && first == nil {
		first = err
	}
	l.timer.Close()
	if err := l.poller.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

func (l *EventLoop) drainTasks() {
	l.taskWakeupPending.Store(false)
	for {
		task, ok := l.tasks.Poll()
		if !ok {
			return
		}
		task()
	}
}

func (l *EventLoop) drainTailTasks() {
	for {
		task, ok := l.tailTasks.Poll()
		if !ok {
			return
		}
		task()
	}
}

func (l *EventLoop) signalTaskWakeup() error {
	if !l.taskWakeupPending.CompareAndSwap(false, true) {
		return nil
	}
	if err := l.poller.Wakeup(); err != nil {
		l.taskWakeupPending.Store(false)
		return err
	}
	return nil
}
