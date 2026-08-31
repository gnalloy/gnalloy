package pool

import "gnalloy.org/gnalloy/channel"

// ChannelCreatedHandler 在池创建新 Channel 后被调用。
type ChannelCreatedHandler interface {
	ChannelCreated(channel.Channel) error
}

// ChannelAcquiredHandler 在 Channel 交给调用方前被调用。
type ChannelAcquiredHandler interface {
	ChannelAcquired(channel.Channel) error
}

// ChannelReleasedHandler 在 Channel 归还到池前被调用。
type ChannelReleasedHandler interface {
	ChannelReleased(channel.Channel) error
}

// LifecycleHandler 聚合 ChannelPool 生命周期回调。
type LifecycleHandler interface {
	ChannelCreatedHandler
	ChannelAcquiredHandler
	ChannelReleasedHandler
}

func notifyCreated(handler any, ch channel.Channel) error {
	if h, ok := handler.(ChannelCreatedHandler); ok {
		return h.ChannelCreated(ch)
	}
	return nil
}

func notifyAcquired(handler any, ch channel.Channel) error {
	if h, ok := handler.(ChannelAcquiredHandler); ok {
		return h.ChannelAcquired(ch)
	}
	return nil
}

func notifyReleased(handler any, ch channel.Channel) error {
	if h, ok := handler.(ChannelReleasedHandler); ok {
		return h.ChannelReleased(ch)
	}
	return nil
}
