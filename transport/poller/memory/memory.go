package memory

import (
	"sync"
	"time"

	"gnalloy.org/gnalloy/transport/poller"
)

// Poller 是测试用 completion 后端，不接触操作系统 fd。
type Poller struct {
	mu      sync.Mutex
	cond    *sync.Cond
	closed  bool
	events  []poller.Event
	entries map[poller.FDRef]poller.ChannelID
}

func New() *Poller {
	p := &Poller{entries: make(map[poller.FDRef]poller.ChannelID)}
	p.cond = sync.NewCond(&p.mu)
	return p
}

func (p *Poller) Model() poller.Model {
	return poller.Completion
}

func (p *Poller) Backend() poller.BackendKind {
	return poller.BackendMemory
}

func (p *Poller) Register(fd poller.FDRef, ch poller.ChannelID, _ poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return poller.ErrClosedPoller
	}
	p.entries[fd] = ch
	return nil
}

func (p *Poller) Modify(fd poller.FDRef, _ poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return poller.ErrClosedPoller
	}
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
	req.RetainBuffers()
	p.events = append(p.events, poller.Event{
		Model:      poller.Completion,
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
	deadline := time.Time{}
	if timeoutMillis >= 0 {
		deadline = time.Now().Add(time.Duration(timeoutMillis) * time.Millisecond)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for !p.closed && len(p.events) == 0 {
		if timeoutMillis == 0 {
			return 0, nil
		}
		if timeoutMillis < 0 {
			p.cond.Wait()
			continue
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0, nil
		}
		timer := time.AfterFunc(remaining, func() {
			p.mu.Lock()
			p.cond.Broadcast()
			p.mu.Unlock()
		})
		p.cond.Wait()
		timer.Stop()
	}
	if p.closed {
		return 0, poller.ErrClosedPoller
	}
	n := copy(dst, p.events)
	copy(p.events, p.events[n:])
	p.events = p.events[:len(p.events)-n]
	return n, nil
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
	p.cond.Broadcast()
	return nil
}
