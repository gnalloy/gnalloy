package bootstrap

import (
	"context"

	"gnalloy.org/gnalloy/channel"
	"gnalloy.org/gnalloy/transport"
)

// ChannelInitializer 初始化新建 Channel 的 Pipeline。
type ChannelInitializer = ChildInitializer

// ClientTransport 是具体传输实现需要满足的客户端连接契约。
type ClientTransport interface {
	Dial(ctx context.Context, cfg ClientConfig) (channel.Channel, error)
}

type ClientConfig struct {
	Address string

	Group *transport.EventLoopGroup

	Options        *channel.ChannelOptions
	Attributes     *channel.AttributeMap
	Initializer    ChannelInitializer
	ChannelFactory ChannelFactory
}

// Dialer 提供 Go 化客户端连接装配 API。
type Dialer struct {
	group       *transport.EventLoopGroup
	options     *channel.ChannelOptions
	attrs       *channel.AttributeMap
	initializer ChannelInitializer
	factory     ChannelFactory
	transport   ClientTransport
}

func NewDialer() *Dialer {
	return &Dialer{}
}

func (d *Dialer) Group(group *transport.EventLoopGroup) *Dialer {
	d.group = group
	return d
}

// Option 配置客户端 Channel 选项。
func (d *Dialer) Option(assignments ...channel.ChannelOptionAssignment) *Dialer {
	if d.options == nil {
		d.options = channel.NewChannelOptions()
	}
	d.options.Apply(assignments...)
	return d
}

// Attr 配置客户端 Channel 属性。
func (d *Dialer) Attr(assignments ...channel.AttributeAssignment) *Dialer {
	if d.attrs == nil {
		d.attrs = channel.NewAttributeMap()
	}
	d.attrs.Apply(assignments...)
	return d
}

func (d *Dialer) Handler(handler func(ch channel.Channel)) *Dialer {
	if handler == nil {
		d.initializer = nil
		return d
	}
	d.initializer = func(ch channel.Channel) error {
		handler(ch)
		return nil
	}
	return d
}

func (d *Dialer) Initializer(initializer ChannelInitializer) *Dialer {
	d.initializer = initializer
	return d
}

func (d *Dialer) ChannelFactory(factory ChannelFactory) *Dialer {
	d.factory = factory
	return d
}

func (d *Dialer) Transport(transport ClientTransport) *Dialer {
	d.transport = transport
	return d
}

func (d *Dialer) Dial(address string) (channel.Channel, error) {
	return d.DialContext(context.Background(), address)
}

func (d *Dialer) DialContext(ctx context.Context, address string) (channel.Channel, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := d.validate(address); err != nil {
		return nil, err
	}
	started, err := ensureGroupStarted(ctx, d.group)
	if err != nil {
		return nil, err
	}
	ch, err := d.transport.Dial(ctx, ClientConfig{
		Address:        address,
		Group:          d.group,
		Options:        d.options.Clone(),
		Attributes:     d.attrs.Clone(),
		Initializer:    d.initializerOrNoop(),
		ChannelFactory: d.factory,
	})
	if err != nil {
		if started {
			_ = d.group.Shutdown(context.Background())
		}
		return nil, err
	}
	return ch, nil
}

func (d *Dialer) validate(address string) error {
	if address == "" {
		return ErrEmptyAddress
	}
	if d.group == nil {
		return ErrMissingGroup
	}
	if d.transport == nil {
		return ErrMissingDialTransport
	}
	return nil
}

func (d *Dialer) initializerOrNoop() ChannelInitializer {
	if d.initializer != nil {
		return d.initializer
	}
	return func(channel.Channel) error { return nil }
}
