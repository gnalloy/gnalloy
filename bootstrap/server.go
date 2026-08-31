package bootstrap

import (
	"context"
	"errors"

	"gnalloy.org/gnalloy/channel"
	"gnalloy.org/gnalloy/transport"
)

// ChildInitializer 初始化新建 Channel 的 Pipeline。
type ChildInitializer func(ch channel.Channel) error

// Server 是 Bind 后返回的服务端句柄。
type Server interface {
	Addr() string
	Close() error
}

// ServerTransport 是具体传输实现需要满足的服务端绑定契约。
type ServerTransport interface {
	Bind(ctx context.Context, cfg ServerConfig) (Server, error)
}

type ServerConfig struct {
	Address string

	BossGroup   *transport.EventLoopGroup
	WorkerGroup *transport.EventLoopGroup

	Options             *channel.ChannelOptions
	Attributes          *channel.AttributeMap
	ChildOptions        *channel.ChannelOptions
	ChildAttributes     *channel.AttributeMap
	ChildInitializer    ChildInitializer
	ChildChannelFactory ChannelFactory
}

type ServerBootstrap struct {
	bossGroup   *transport.EventLoopGroup
	workerGroup *transport.EventLoopGroup

	options             *channel.ChannelOptions
	attrs               *channel.AttributeMap
	childOptions        *channel.ChannelOptions
	childAttrs          *channel.AttributeMap
	childInitializer    ChildInitializer
	childChannelFactory ChannelFactory
	serverTransport     ServerTransport
}

func NewServerBootstrap() *ServerBootstrap {
	return &ServerBootstrap{}
}

func (b *ServerBootstrap) Group(bossGroup *transport.EventLoopGroup, workerGroup *transport.EventLoopGroup) *ServerBootstrap {
	b.bossGroup = bossGroup
	b.workerGroup = workerGroup
	return b
}

// Option 配置服务端监听 Channel 选项。当前传输可按需消费该快照。
func (b *ServerBootstrap) Option(assignments ...channel.ChannelOptionAssignment) *ServerBootstrap {
	if b.options == nil {
		b.options = channel.NewChannelOptions()
	}
	b.options.Apply(assignments...)
	return b
}

// Attr 配置服务端监听 Channel 属性。当前传输可按需消费该快照。
func (b *ServerBootstrap) Attr(assignments ...channel.AttributeAssignment) *ServerBootstrap {
	if b.attrs == nil {
		b.attrs = channel.NewAttributeMap()
	}
	b.attrs.Apply(assignments...)
	return b
}

// ChildOption 配置服务端接受到的子 Channel 选项。
func (b *ServerBootstrap) ChildOption(assignments ...channel.ChannelOptionAssignment) *ServerBootstrap {
	if b.childOptions == nil {
		b.childOptions = channel.NewChannelOptions()
	}
	b.childOptions.Apply(assignments...)
	return b
}

// ChildAttr 配置服务端接受到的子 Channel 属性。
func (b *ServerBootstrap) ChildAttr(assignments ...channel.AttributeAssignment) *ServerBootstrap {
	if b.childAttrs == nil {
		b.childAttrs = channel.NewAttributeMap()
	}
	b.childAttrs.Apply(assignments...)
	return b
}

func (b *ServerBootstrap) ChildHandler(handler func(ch channel.Channel)) *ServerBootstrap {
	if handler == nil {
		b.childInitializer = nil
		return b
	}
	b.childInitializer = func(ch channel.Channel) error {
		handler(ch)
		return nil
	}
	return b
}

func (b *ServerBootstrap) ChildInitializer(initializer ChildInitializer) *ServerBootstrap {
	b.childInitializer = initializer
	return b
}

func (b *ServerBootstrap) ChildChannelFactory(factory ChannelFactory) *ServerBootstrap {
	b.childChannelFactory = factory
	return b
}

func (b *ServerBootstrap) Transport(serverTransport ServerTransport) *ServerBootstrap {
	b.serverTransport = serverTransport
	return b
}

func (b *ServerBootstrap) Bind(address string) (Server, error) {
	return b.BindContext(context.Background(), address)
}

func (b *ServerBootstrap) BindContext(ctx context.Context, address string) (Server, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := b.validate(address); err != nil {
		return nil, err
	}

	startedBoss, err := ensureGroupStarted(ctx, b.bossGroup)
	if err != nil {
		return nil, err
	}
	startedWorker := false
	if b.workerGroup != b.bossGroup {
		startedWorker, err = ensureGroupStarted(ctx, b.workerGroup)
		if err != nil {
			if startedBoss {
				_ = b.bossGroup.Shutdown(context.Background())
			}
			return nil, err
		}
	}

	server, err := b.serverTransport.Bind(ctx, ServerConfig{
		Address:             address,
		BossGroup:           b.bossGroup,
		WorkerGroup:         b.workerGroup,
		Options:             b.options.Clone(),
		Attributes:          b.attrs.Clone(),
		ChildOptions:        b.childOptions.Clone(),
		ChildAttributes:     b.childAttrs.Clone(),
		ChildInitializer:    b.childInitializer,
		ChildChannelFactory: b.childChannelFactory,
	})
	if err != nil {
		if startedWorker {
			_ = b.workerGroup.Shutdown(context.Background())
		}
		if startedBoss {
			_ = b.bossGroup.Shutdown(context.Background())
		}
		return nil, err
	}
	return server, nil
}

func (b *ServerBootstrap) validate(address string) error {
	if address == "" {
		return ErrEmptyAddress
	}
	if b.bossGroup == nil || b.workerGroup == nil {
		return ErrMissingGroup
	}
	if b.childInitializer == nil {
		return ErrMissingChildHandler
	}
	if b.serverTransport == nil {
		return ErrMissingTransport
	}
	return nil
}

func ensureGroupStarted(ctx context.Context, group *transport.EventLoopGroup) (bool, error) {
	if group == nil {
		return false, ErrMissingGroup
	}
	if group.IsRunning() {
		return false, nil
	}
	err := group.Start(ctx)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, transport.ErrEventLoopGroupRunning) {
		return false, nil
	}
	return false, err
}
