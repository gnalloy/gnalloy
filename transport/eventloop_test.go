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
