package bootstrap

import (
	"gnalloy.org/gnalloy/channel"
)

// ChannelFactory 抽象底层传输创建 Channel 的步骤。
// 自定义工厂必须保留 UnsafeConfig 内的 fd、poller、allocator 和 close hook。
type ChannelFactory func(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error)

// DefaultChannelFactory 使用 gnalloy 标准 Unsafe/LocalChannel 组合。
func DefaultChannelFactory(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error) {
	ch, unsafeCh := channel.NewUnsafeChannel(cfg)
	return ch, unsafeCh, nil
}

func channelFactoryOrDefault(factory ChannelFactory) ChannelFactory {
	if factory != nil {
		return factory
	}
	return DefaultChannelFactory
}

// Apply 把客户端 Bootstrap 配置落到已创建的 Channel。
func (c ClientConfig) Apply(ch channel.Channel) {
	applyChannelConfig(ch, c.Options, c.Attributes)
}

// NewChannel 用客户端配置中的工厂创建底层 Channel。
func (c ClientConfig) NewChannel(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error) {
	return channelFactoryOrDefault(c.ChannelFactory)(cfg)
}

// ApplyChild 把服务端子 Channel 配置落到已创建的 Channel。
func (c ServerConfig) ApplyChild(ch channel.Channel) {
	applyChannelConfig(ch, c.ChildOptions, c.ChildAttributes)
}

// NewChildChannel 用服务端配置中的子 Channel 工厂创建连接 Channel。
func (c ServerConfig) NewChildChannel(cfg channel.UnsafeConfig) (channel.Channel, *channel.Unsafe, error) {
	return channelFactoryOrDefault(c.ChildChannelFactory)(cfg)
}

func applyChannelConfig(ch channel.Channel, options *channel.ChannelOptions, attrs *channel.AttributeMap) {
	if ch == nil {
		return
	}
	options.CopyTo(ch.Options())
	attrs.CopyTo(ch.Attributes())
}
