package transport

import (
	"gnalloy.org/gnalloy/transport/poller"
	"gnalloy.org/gnalloy/transport/poller/memory"
	"gnalloy.org/gnalloy/transport/poller/std"
)

func NewPoller(cfg Config) (Poller, error) {
	switch cfg.Backend {
	case BackendMemory:
		return memory.New(), nil
	case BackendStd:
		return std.New(), nil
	default:
		return newNativePoller(poller.Config(cfg))
	}
}
