package std

import (
	"sync"
	"time"

	"gnalloy.org/gnalloy/transport/poller"
)

const defaultPollIntervalMillis = 10

type entry struct {
	fd       poller.FDRef
	ch       poller.ChannelID
	interest poller.ReadyMask
}

// Poller 是不依赖平台事件接口的 readiness fallback。
type Poller struct {
	mu      sync.Mutex
	cond    *sync.Cond
	closed  bool
	events  []poller.Event
	entries map[poller.FDRef]entry
}

func New() *Poller {
	p := &Poller{entries: make(map[poller.FDRef]entry)}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *Poller) Model() poller.Model {
	return poller.Readiness
}

func (p *Poller) Backend() poller.BackendKind {
	return poller.BackendStd
}

func (p *Poller) Register(fd poller.FDRef, ch poller.ChannelID, interest poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return poller.ErrClosedPoller
	}
	p.entries[fd] = entry{fd: fd, ch: ch, interest: interest}
	p.cond.Broadcast()
	return nil
}

func (p *Poller) Modify(fd poller.FDRef, interest poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return poller.ErrClosedPoller
	}
	e, ok := p.entries[fd]
	if !ok {
		return nil
	}
	e.interest = interest
	p.entries[fd] = e
	p.cond.Broadcast()
	return nil
}

func (p *Poller) Deregister(fd poller.FDRef) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.entries, fd)
	return nil
}

func (p *Poller) Submit(req poller.IORequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return poller.ErrClosedPoller
	}
	if req.Buf != nil {
		req.Buf.Retain()
	}
	for _, buf := range req.Bufs {
		buf.Retain()
	}
	p.events = append(p.events, poller.Event{
		Model:      poller.Readiness,
		Op:         req.Op,
		Ready:      req.Ready,
		FD:         req.FD,
		AcceptedFD: req.AcceptedFD,
		ChannelID:  req.ChannelID,
		OpID:       req.OpID,
		Buf:        req.Buf,
		Bufs:       req.Bufs,
		Addr:       req.Addr,
	})
	p.cond.Signal()
	return nil
}

func (p *Poller) Poll(dst []poller.Event, timeoutMillis int) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	for !p.closed && len(p.events) == 0 && len(p.entries) == 0 {
		if !p.waitLocked(timeoutMillis) {
			return 0, nil
		}
	}
	if p.closed {
		return 0, poller.ErrClosedPoller
	}
	if len(p.events) > 0 {
		return p.popEventsLocked(dst), nil
	}
	if timeoutMillis != 0 {
		_ = p.waitLocked(pollInterval(timeoutMillis))
		if p.closed {
			return 0, poller.ErrClosedPoller
		}
		if len(p.events) > 0 {
			return p.popEventsLocked(dst), nil
		}
	}
	return p.copyReadinessLocked(dst), nil
}

func (p *Poller) Wakeup() error {
	return p.Submit(poller.IORequest{Op: poller.OpWakeup})
}

func (p *Poller) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	for _, ev := range p.events {
		if ev.Buf != nil {
			ev.Buf.Release()
		}
		for _, buf := range ev.Bufs {
			buf.Release()
		}
	}
	p.events = nil
	p.entries = nil
	p.cond.Broadcast()
	return nil
}

func (p *Poller) popEventsLocked(dst []poller.Event) int {
	n := copy(dst, p.events)
	copy(p.events, p.events[n:])
	p.events = p.events[:len(p.events)-n]
	return n
}

func (p *Poller) copyReadinessLocked(dst []poller.Event) int {
	n := 0
	for _, e := range p.entries {
		if e.interest == 0 {
			continue
		}
		dst[n] = poller.Event{
			Model:     poller.Readiness,
			Ready:     e.interest,
			FD:        e.fd,
			ChannelID: e.ch,
		}
		n++
		if n == len(dst) {
			break
		}
	}
	return n
}

func (p *Poller) waitLocked(timeoutMillis int) bool {
	if timeoutMillis == 0 {
		return false
	}
	if timeoutMillis < 0 {
		p.cond.Wait()
		return true
	}
	timer := time.AfterFunc(time.Duration(timeoutMillis)*time.Millisecond, func() {
		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
	})
	p.cond.Wait()
	timer.Stop()
	return true
}

func pollInterval(timeoutMillis int) int {
	if timeoutMillis < 0 || timeoutMillis > defaultPollIntervalMillis {
		return defaultPollIntervalMillis
	}
	return timeoutMillis
}
