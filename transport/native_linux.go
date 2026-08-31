//go:build linux

package transport

import (
	"gnalloy.org/gnalloy/transport/poller"
	"gnalloy.org/gnalloy/transport/poller/epoll"
	"gnalloy.org/gnalloy/transport/poller/iouring"
)

func newNativePoller(cfg poller.Config) (Poller, error) {
	switch cfg.Backend {
	case BackendEpoll:
		return epoll.New()
	case BackendIOUring:
		return iouring.NewWithConfig(iouring.Config{
			Entries:          cfg.Entries,
			SQPoll:           cfg.SQPoll,
			SQPollAffinity:   cfg.SQPollAffinity,
			SQPollCPU:        cfg.SQPollCPU,
			SQPollIdleMillis: cfg.SQPollIdleMillis,
			MultishotAccept:  cfg.MultishotAccept,
		})
	default:
		return nil, ErrUnsupportedPoller
	}
}
