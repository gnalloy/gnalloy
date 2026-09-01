package transport

import (
	"sync/atomic"
	"testing"

	"gnalloy.org/gnalloy/transport/poller/memory"
)

type testEventHandler struct {
	id     ChannelID
	fd     FDRef
	events []PollEvent
	closed bool
}

func (h *testEventHandler) ID() ChannelID {
	return h.id
}

func (h *testEventHandler) FD() FDRef {
	return h.fd
}

func (h *testEventHandler) HandleEvent(ev PollEvent) {
	h.events = append(h.events, ev)
}

func (h *testEventHandler) Close() error {
	h.closed = true
	return nil
}

func TestEventLoopSubmitTask(t *testing.T) {
	p := memory.New()
	loop, err := NewEventLoop(EventLoopConfig{ID: 1, Poller: p, StartMillis: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()

	ran := false
	if err := loop.Submit(func() { ran = true }); err != nil {
		t.Fatal(err)
	}
	if err := loop.RunOnce(0); err != nil {
		t.Fatal(err)
	}
	if !ran {
		t.Fatal("task was not executed")
	}
}

func TestEventLoopDispatchesPollEvent(t *testing.T) {
	p := memory.New()
	loop, err := NewEventLoop(EventLoopConfig{ID: 1, Poller: p, StartMillis: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()

	h := &testEventHandler{id: 7, fd: FDRef{FD: 123}}
	if err := loop.Register(h, ReadyRead); err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(IORequest{Op: OpRead, FD: h.fd, ChannelID: h.id}); err != nil {
		t.Fatal(err)
	}
	if err := loop.RunOnce(0); err != nil {
		t.Fatal(err)
	}
	if len(h.events) != 1 || h.events[0].Op != OpRead {
		t.Fatalf("events=%+v", h.events)
	}
}

func TestEventLoopCoalescesTaskWakeups(t *testing.T) {
	p := &wakeupCountingPoller{}
	loop, err := NewEventLoop(EventLoopConfig{ID: 1, Poller: p, TaskQueueSize: 64, StartMillis: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()

	ran := 0
	for i := 0; i < 8; i++ {
		if err := loop.Submit(func() { ran++ }); err != nil {
			t.Fatal(err)
		}
	}
	if got := p.wakeups.Load(); got != 1 {
		t.Fatalf("wakeups=%d, want 1", got)
	}
	if err := loop.RunOnce(0); err != nil {
		t.Fatal(err)
	}
	if ran != 8 {
		t.Fatalf("ran=%d, want 8", ran)
	}

	if err := loop.Submit(func() { ran++ }); err != nil {
		t.Fatal(err)
	}
	if got := p.wakeups.Load(); got != 2 {
		t.Fatalf("wakeups=%d, want 2 after drain", got)
	}
}

func TestEventLoopRunsTailTasksAfterSubmittedTasks(t *testing.T) {
	p := memory.New()
	loop, err := NewEventLoop(EventLoopConfig{ID: 1, Poller: p, StartMillis: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()

	var order []string
	if err := loop.Submit(func() {
		order = append(order, "task")
		if err := loop.SubmitAfterBatch(func() {
			order = append(order, "tail")
		}); err != nil {
			t.Fatal(err)
		}
	}); err != nil {
		t.Fatal(err)
	}

	if err := loop.RunOnce(0); err != nil {
		t.Fatal(err)
	}
	want := []string{"task", "tail"}
	if len(order) != len(want) {
		t.Fatalf("order=%v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v, want %v", order, want)
		}
	}
}

func TestEventLoopRunsTailTasksAfterPollBatch(t *testing.T) {
	p := &scriptedEventPoller{
		events: []PollEvent{
			{Model: PollerReadiness, Ready: ReadyRead, FD: FDRef{FD: 1}, ChannelID: 1},
			{Model: PollerReadiness, Ready: ReadyRead, FD: FDRef{FD: 2}, ChannelID: 2},
		},
	}
	loop, err := NewEventLoop(EventLoopConfig{ID: 1, Poller: p, StartMillis: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()

	var order []string
	first := &callbackEventHandler{id: 1, fd: FDRef{FD: 1}, fn: func(PollEvent) {
		order = append(order, "event1")
		if err := loop.SubmitAfterBatch(func() {
			order = append(order, "tail")
		}); err != nil {
			t.Fatal(err)
		}
	}}
	second := &callbackEventHandler{id: 2, fd: FDRef{FD: 2}, fn: func(PollEvent) {
		order = append(order, "event2")
	}}
	if err := loop.Register(first, ReadyRead); err != nil {
		t.Fatal(err)
	}
	if err := loop.Register(second, ReadyRead); err != nil {
		t.Fatal(err)
	}

	if err := loop.RunOnce(0); err != nil {
		t.Fatal(err)
	}
	want := []string{"event1", "event2", "tail"}
	if len(order) != len(want) {
		t.Fatalf("order=%v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v, want %v", order, want)
		}
	}
}

type wakeupCountingPoller struct {
	wakeups atomic.Uint64
	closed  atomic.Bool
}

func (p *wakeupCountingPoller) Model() PollerModel {
	return PollerCompletion
}

func (p *wakeupCountingPoller) Backend() BackendKind {
	return BackendMemory
}

func (p *wakeupCountingPoller) Register(FDRef, ChannelID, ReadyMask) error {
	if p.closed.Load() {
		return ErrClosedPoller
	}
	return nil
}

func (p *wakeupCountingPoller) Modify(FDRef, ReadyMask) error {
	if p.closed.Load() {
		return ErrClosedPoller
	}
	return nil
}

func (p *wakeupCountingPoller) Deregister(FDRef) error {
	return nil
}

func (p *wakeupCountingPoller) Submit(req IORequest) error {
	if req.Op == OpWakeup {
		return p.Wakeup()
	}
	if p.closed.Load() {
		return ErrClosedPoller
	}
	return nil
}

func (p *wakeupCountingPoller) Poll([]PollEvent, int) (int, error) {
	if p.closed.Load() {
		return 0, ErrClosedPoller
	}
	return 0, nil
}

func (p *wakeupCountingPoller) Wakeup() error {
	if p.closed.Load() {
		return ErrClosedPoller
	}
	p.wakeups.Add(1)
	return nil
}

func (p *wakeupCountingPoller) Close() error {
	p.closed.Store(true)
	return nil
}

type callbackEventHandler struct {
	id     ChannelID
	fd     FDRef
	fn     func(PollEvent)
	closed bool
}

func (h *callbackEventHandler) ID() ChannelID {
	return h.id
}

func (h *callbackEventHandler) FD() FDRef {
	return h.fd
}

func (h *callbackEventHandler) HandleEvent(ev PollEvent) {
	if h.fn != nil {
		h.fn(ev)
	}
}

func (h *callbackEventHandler) Close() error {
	h.closed = true
	return nil
}

type scriptedEventPoller struct {
	events []PollEvent
	closed atomic.Bool
}

func (p *scriptedEventPoller) Model() PollerModel {
	return PollerReadiness
}

func (p *scriptedEventPoller) Backend() BackendKind {
	return BackendMemory
}

func (p *scriptedEventPoller) Register(FDRef, ChannelID, ReadyMask) error {
	if p.closed.Load() {
		return ErrClosedPoller
	}
	return nil
}

func (p *scriptedEventPoller) Modify(FDRef, ReadyMask) error {
	if p.closed.Load() {
		return ErrClosedPoller
	}
	return nil
}

func (p *scriptedEventPoller) Deregister(FDRef) error {
	return nil
}

func (p *scriptedEventPoller) Submit(IORequest) error {
	if p.closed.Load() {
		return ErrClosedPoller
	}
	return nil
}

func (p *scriptedEventPoller) Poll(dst []PollEvent, _ int) (int, error) {
	if p.closed.Load() {
		return 0, ErrClosedPoller
	}
	n := copy(dst, p.events)
	copy(p.events, p.events[n:])
	p.events = p.events[:len(p.events)-n]
	return n, nil
}

func (p *scriptedEventPoller) Wakeup() error {
	return nil
}

func (p *scriptedEventPoller) Close() error {
	p.closed.Store(true)
	return nil
}
