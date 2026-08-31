//go:build windows

package transport

import (
	"gnalloy.org/gnalloy/transport/poller"
	"gnalloy.org/gnalloy/transport/poller/iocp"
)

func newNativePoller(cfg poller.Config) (Poller, error) {
	if cfg.Backend == BackendIOCP {
		return iocp.New()
	}
	return nil, ErrUnsupportedPoller
}
