//go:build linux

package iouring

import (
	"encoding/binary"
	"errors"

	"golang.org/x/sys/unix"
)

func (p *Poller) waitReadable(timeoutMillis int) (bool, error) {
	pollfds := []unix.PollFd{
		{Fd: int32(p.fd), Events: unix.POLLIN},
		{Fd: int32(p.wakefd), Events: unix.POLLIN},
	}
	ready, err := unix.Poll(pollfds, timeoutMillis)
	if err != nil {
		if errors.Is(err, unix.EINTR) {
			return false, nil
		}
		return false, err
	}
	if ready == 0 {
		return false, nil
	}
	return pollfds[1].Revents&(unix.POLLIN|unix.POLLHUP|unix.POLLERR) != 0, nil
}

func (p *Poller) signalWakeup() error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], 1)
	_, err := unix.Write(p.wakefd, buf[:])
	if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
		return nil
	}
	return err
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

func (p *Poller) closeWakeup() error {
	if p.wakefd < 0 {
		return nil
	}
	fd := p.wakefd
	p.wakefd = -1
	return unix.Close(fd)
}
