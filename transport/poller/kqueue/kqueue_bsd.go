//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package kqueue

import (
	"errors"

	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/unix"
)

type Poller struct {
	kq       int
	wakeRead int
	wakeWrit int
	events   []unix.Kevent_t

	entries map[int]poller.ChannelID
	closed  bool
}

func New() (poller.Poller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	var pipe [2]int
	if err := unix.Pipe(pipe[:]); err != nil {
		_ = unix.Close(kq)
		return nil, err
	}
	p := &Poller{
		kq:       kq,
		wakeRead: pipe[0],
		wakeWrit: pipe[1],
		events:   make([]unix.Kevent_t, 1024),
		entries:  make(map[int]poller.ChannelID, 1024),
	}
	_ = unix.SetNonblock(p.wakeRead, true)
	_ = unix.SetNonblock(p.wakeWrit, true)
	if err := p.Register(poller.FDRef{FD: p.wakeRead}, 0, poller.ReadyRead); err != nil {
		_ = unix.Close(p.wakeRead)
		_ = unix.Close(p.wakeWrit)
		_ = unix.Close(kq)
		return nil, err
	}
	return p, nil
}

func (p *Poller) Model() poller.Model {
	return poller.Readiness
}

func (p *Poller) Backend() poller.BackendKind {
	return poller.BackendKqueue
}

func (p *Poller) Register(fd poller.FDRef, ch poller.ChannelID, interest poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	if p.closed {
		return poller.ErrClosedPoller
	}
	if err := unix.SetNonblock(fd.FD, true); err != nil {
		return err
	}
	changes := kqueueChanges(fd.FD, interest, unix.EV_ADD|unix.EV_CLEAR)
	if _, err := unix.Kevent(p.kq, changes, nil, nil); err != nil {
		return err
	}
	p.entries[fd.FD] = ch
	return nil
}

func (p *Poller) Modify(fd poller.FDRef, interest poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	if p.closed {
		return poller.ErrClosedPoller
	}
	var changes []unix.Kevent_t
	changes = append(changes, kqueueChanges(fd.FD, poller.ReadyRead|poller.ReadyWrite, unix.EV_DELETE)...)
	changes = append(changes, kqueueChanges(fd.FD, interest, unix.EV_ADD|unix.EV_CLEAR)...)
	_, err := unix.Kevent(p.kq, changes, nil, nil)
	return err
}

func (p *Poller) Deregister(fd poller.FDRef) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	delete(p.entries, fd.FD)
	_, err := unix.Kevent(p.kq, kqueueChanges(fd.FD, poller.ReadyRead|poller.ReadyWrite, unix.EV_DELETE), nil, nil)
	if errors.Is(err, unix.ENOENT) {
		return nil
	}
	return err
}

func (p *Poller) Submit(req poller.IORequest) error {
	if req.Op == poller.OpWakeup {
		return p.Wakeup()
	}
	return poller.ErrInvalidIORequest
}

func (p *Poller) Poll(dst []poller.Event, timeoutMillis int) (int, error) {
	if p.closed {
		return 0, poller.ErrClosedPoller
	}
	if len(dst) == 0 {
		return 0, nil
	}
	timeout := kqueueTimeout(timeoutMillis)
	n, err := unix.Kevent(p.kq, nil, p.events, timeout)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return 0, nil
		}
		return 0, err
	}
	out := 0
	for i := 0; i < n && out < len(dst); i++ {
		ev := p.events[i]
		fd := int(ev.Ident)
		if fd == p.wakeRead {
			p.drainWakeup()
			dst[out] = poller.Event{Model: poller.Readiness, Op: poller.OpWakeup, FD: poller.FDRef{FD: fd}, Ready: poller.ReadyRead}
			out++
			continue
		}
		ready := kqueueReady(ev)
		dst[out] = poller.Event{
			Model:     poller.Readiness,
			Op:        poller.ReadinessOp(ready),
			Ready:     ready,
			FD:        poller.FDRef{FD: fd},
			ChannelID: p.entries[fd],
		}
		out++
	}
	return out, nil
}

func (p *Poller) Wakeup() error {
	var b [1]byte
	b[0] = 1
	_, err := unix.Write(p.wakeWrit, b[:])
	if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
		return nil
	}
	return err
}

func (p *Poller) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	err1 := unix.Close(p.wakeRead)
	err2 := unix.Close(p.wakeWrit)
	err3 := unix.Close(p.kq)
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return err3
}

func (p *Poller) drainWakeup() {
	var buf [64]byte
	for {
		_, err := unix.Read(p.wakeRead, buf[:])
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return
		}
		if err != nil {
			return
		}
	}
}

func kqueueChanges(fd int, mask poller.ReadyMask, flags int) []unix.Kevent_t {
	changes := make([]unix.Kevent_t, 0, 2)
	if mask&poller.ReadyRead != 0 {
		var ev unix.Kevent_t
		unix.SetKevent(&ev, fd, unix.EVFILT_READ, flags)
		changes = append(changes, ev)
	}
	if mask&poller.ReadyWrite != 0 {
		var ev unix.Kevent_t
		unix.SetKevent(&ev, fd, unix.EVFILT_WRITE, flags)
		changes = append(changes, ev)
	}
	return changes
}

func kqueueReady(ev unix.Kevent_t) poller.ReadyMask {
	var ready poller.ReadyMask
	if ev.Filter == unix.EVFILT_READ {
		ready |= poller.ReadyRead
	}
	if ev.Filter == unix.EVFILT_WRITE {
		ready |= poller.ReadyWrite
	}
	if ev.Flags&unix.EV_EOF != 0 {
		ready |= poller.ReadyHangup
	}
	if ev.Flags&unix.EV_ERROR != 0 {
		ready |= poller.ReadyError
	}
	return ready
}

func kqueueTimeout(timeoutMillis int) *unix.Timespec {
	if timeoutMillis < 0 {
		return nil
	}
	return &unix.Timespec{
		Sec:  int64(timeoutMillis / 1000),
		Nsec: int64(timeoutMillis%1000) * 1000 * 1000,
	}
}
