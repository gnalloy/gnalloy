//go:build linux

package epoll

import (
	"encoding/binary"
	"errors"

	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/unix"
)

type Poller struct {
	epfd   int
	wakefd int
	events []unix.EpollEvent

	entries map[int]poller.ChannelID
	closed  bool
}

func New() (poller.Poller, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	wakefd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		_ = unix.Close(epfd)
		return nil, err
	}
	p := &Poller{
		epfd:    epfd,
		wakefd:  wakefd,
		events:  make([]unix.EpollEvent, 1024),
		entries: make(map[int]poller.ChannelID, 1024),
	}
	if err := p.addFD(wakefd, 0, poller.ReadyRead); err != nil {
		_ = unix.Close(wakefd)
		_ = unix.Close(epfd)
		return nil, err
	}
	return p, nil
}

func (p *Poller) Model() poller.Model {
	return poller.Readiness
}

func (p *Poller) Backend() poller.BackendKind {
	return poller.BackendEpoll
}

func (p *Poller) Register(fd poller.FDRef, ch poller.ChannelID, interest poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	return p.addFD(fd.FD, ch, interest)
}

func (p *Poller) Modify(fd poller.FDRef, interest poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	event := unix.EpollEvent{Events: epollEvents(interest), Fd: int32(fd.FD)}
	return unix.EpollCtl(p.epfd, unix.EPOLL_CTL_MOD, fd.FD, &event)
}

func (p *Poller) Deregister(fd poller.FDRef) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	delete(p.entries, fd.FD)
	err := unix.EpollCtl(p.epfd, unix.EPOLL_CTL_DEL, fd.FD, nil)
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
	n, err := unix.EpollWait(p.epfd, p.events, timeoutMillis)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return 0, nil
		}
		return 0, err
	}
	out := 0
	for i := 0; i < n && out < len(dst); i++ {
		fd := int(p.events[i].Fd)
		if fd == p.wakefd {
			p.drainWakeup()
			dst[out] = poller.Event{Model: poller.Readiness, Op: poller.OpWakeup, FD: poller.FDRef{FD: fd}, Ready: poller.ReadyRead}
			out++
			continue
		}
		ready := epollReady(p.events[i].Events)
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
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)
	_, err := unix.Write(p.wakefd, buf[:])
	if errors.Is(err, unix.EAGAIN) {
		return nil
	}
	return err
}

func (p *Poller) Close() error {
	if p.closed {
		return nil
	}
	p.closed = true
	err1 := unix.Close(p.wakefd)
	err2 := unix.Close(p.epfd)
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *Poller) addFD(fd int, ch poller.ChannelID, interest poller.ReadyMask) error {
	if p.closed {
		return poller.ErrClosedPoller
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		return err
	}
	event := unix.EpollEvent{Events: epollEvents(interest), Fd: int32(fd)}
	if err := unix.EpollCtl(p.epfd, unix.EPOLL_CTL_ADD, fd, &event); err != nil {
		return err
	}
	p.entries[fd] = ch
	return nil
}

func (p *Poller) drainWakeup() {
	var buf [8]byte
	for {
		_, err := unix.Read(p.wakefd, buf[:])
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return
		}
		if err != nil {
			return
		}
	}
}

func epollEvents(mask poller.ReadyMask) uint32 {
	events := uint32(unix.EPOLLET | unix.EPOLLERR | unix.EPOLLHUP | unix.EPOLLRDHUP)
	if mask&poller.ReadyRead != 0 {
		events |= unix.EPOLLIN
	}
	if mask&poller.ReadyWrite != 0 {
		events |= unix.EPOLLOUT
	}
	return events
}

func epollReady(events uint32) poller.ReadyMask {
	var ready poller.ReadyMask
	if events&(unix.EPOLLIN|unix.EPOLLPRI) != 0 {
		ready |= poller.ReadyRead
	}
	if events&unix.EPOLLOUT != 0 {
		ready |= poller.ReadyWrite
	}
	if events&(unix.EPOLLHUP|unix.EPOLLRDHUP) != 0 {
		ready |= poller.ReadyHangup
	}
	if events&unix.EPOLLERR != 0 {
		ready |= poller.ReadyError
	}
	return ready
}
