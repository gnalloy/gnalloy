package timer

import "sync/atomic"

type Callback interface {
	OnTimeout(ctx Context, task *Task)
}

type CallbackFunc func(ctx Context, task *Task)

func (f CallbackFunc) OnTimeout(ctx Context, task *Task) {
	f(ctx, task)
}

type Context interface {
	NowTick() uint64
}

type context struct {
	nowTick uint64
}

func (c context) NowTick() uint64 {
	return c.nowTick
}

type State uint8

const (
	Pending State = iota
	Cancelled
	Expired
)

type Task struct {
	ID uint64

	deadline uint64
	state    State
	cb       Callback
	slot     *slot
	prev     *Task
	next     *Task
	owner    *Wheel
}

func (t *Task) State() State {
	if t == nil {
		return Cancelled
	}
	return t.state
}

func (t *Task) DeadlineTick() uint64 {
	if t == nil {
		return 0
	}
	return t.deadline
}

type Wheel struct {
	tickMillis  int64
	startMillis int64
	currentTick uint64

	mask   uint64
	slots  []slot
	free   []*Task
	nextID uint64

	closed atomic.Bool
}

type slot struct {
	head *Task
	tail *Task
}

func NewWheel(tickMillis int64, wheelSize uint64, startMillis int64) (*Wheel, error) {
	if tickMillis <= 0 {
		return nil, ErrInvalidTick
	}
	if wheelSize < 2 || wheelSize&(wheelSize-1) != 0 {
		return nil, ErrInvalidWheelSize
	}
	return &Wheel{
		tickMillis:  tickMillis,
		startMillis: startMillis,
		mask:        wheelSize - 1,
		slots:       make([]slot, wheelSize),
	}, nil
}

func (w *Wheel) Schedule(delayMillis int64, cb Callback) (*Task, error) {
	if w.closed.Load() {
		return nil, ErrTimerWheelClosed
	}
	if cb == nil {
		return nil, ErrNilTimerCallback
	}
	if delayMillis < w.tickMillis {
		delayMillis = w.tickMillis
	}
	ticks := uint64((delayMillis + w.tickMillis - 1) / w.tickMillis)
	deadline := w.currentTick + ticks

	task := w.acquire()
	task.ID = w.nextID
	w.nextID++
	task.deadline = deadline
	task.state = Pending
	task.cb = cb
	task.owner = w

	s := &w.slots[deadline&w.mask]
	w.add(s, task)
	return task, nil
}

func (w *Wheel) Cancel(task *Task) bool {
	if task == nil || task.owner != w || task.state != Pending {
		return false
	}
	w.remove(task.slot, task)
	task.state = Cancelled
	w.release(task)
	return true
}

func (w *Wheel) Advance(nowMillis int64, budget int) int {
	if w.closed.Load() {
		return 0
	}
	target := uint64(0)
	if nowMillis > w.startMillis {
		target = uint64((nowMillis - w.startMillis) / w.tickMillis)
	}
	executed := 0

	for w.currentTick < target {
		w.currentTick++
		s := &w.slots[w.currentTick&w.mask]
		for t := s.head; t != nil; {
			next := t.next
			if t.deadline <= w.currentTick && t.state == Pending {
				w.remove(s, t)
				t.state = Expired
				cb := t.cb
				cb.OnTimeout(context{nowTick: w.currentTick}, t)
				w.release(t)
				executed++
				if budget > 0 && executed >= budget {
					return executed
				}
			}
			t = next
		}
	}
	return executed
}

func (w *Wheel) NextDelayMillis(nowMillis int64) int {
	if w.closed.Load() {
		return -1
	}
	target := w.startMillis + int64(w.currentTick+1)*w.tickMillis
	if target <= nowMillis {
		return 0
	}
	return int(target - nowMillis)
}

func (w *Wheel) CurrentTick() uint64 {
	return w.currentTick
}

func (w *Wheel) Close() {
	if w.closed.Swap(true) {
		return
	}
	for i := range w.slots {
		for t := w.slots[i].head; t != nil; {
			next := t.next
			t.state = Cancelled
			w.release(t)
			t = next
		}
		w.slots[i] = slot{}
	}
	w.free = nil
}

func (w *Wheel) acquire() *Task {
	n := len(w.free)
	if n == 0 {
		return &Task{}
	}
	t := w.free[n-1]
	w.free = w.free[:n-1]
	return t
}

func (w *Wheel) release(task *Task) {
	task.cb = nil
	task.slot = nil
	task.prev = nil
	task.next = nil
	task.owner = nil
	w.free = append(w.free, task)
}

func (w *Wheel) add(s *slot, task *Task) {
	task.slot = s
	if s.tail == nil {
		s.head = task
		s.tail = task
		return
	}
	s.tail.next = task
	task.prev = s.tail
	s.tail = task
}

func (w *Wheel) remove(s *slot, task *Task) {
	if s == nil || task == nil {
		return
	}
	if task.prev != nil {
		task.prev.next = task.next
	} else {
		s.head = task.next
	}
	if task.next != nil {
		task.next.prev = task.prev
	} else {
		s.tail = task.prev
	}
	task.prev = nil
	task.next = nil
	task.slot = nil
}
