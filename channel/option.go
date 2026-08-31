package channel

import "gnalloy.org/gnalloy/transport"

type optionKeyID string

// ChannelOption 是 Netty ChannelOption 的 Go 化类型安全版本。
type ChannelOption[T any] struct {
	name         optionKeyID
	defaultValue T
}

var (
	// OptionAutoRead 控制 Channel 激活后是否自动注册读兴趣。
	OptionAutoRead = NewChannelOption("AUTO_READ", true)
	// OptionReadBufferSize 控制单次底层读缓冲区大小，0 表示使用传输默认值。
	OptionReadBufferSize = NewChannelOption("READ_BUFFER_SIZE", 0)
	// OptionWriteBufferWatermark 控制出站缓冲区的高低水位线。
	OptionWriteBufferWatermark = NewChannelOption("WRITE_BUFFER_WATERMARK", transport.DefaultWriteBufferWatermark())
	// OptionConnectTimeoutMillis 控制客户端 TCP connect 超时时间，0 表示不设置超时。
	OptionConnectTimeoutMillis = NewChannelOption("CONNECT_TIMEOUT_MILLIS", 30000)
	// OptionSoBacklog 控制监听 socket 的 accept backlog。
	OptionSoBacklog = NewChannelOption("SO_BACKLOG", 1024)
	// OptionSoReuseAddr 控制监听 socket 的 SO_REUSEADDR。
	OptionSoReuseAddr = NewChannelOption("SO_REUSEADDR", true)
	// OptionSoReusePort 控制监听 socket 的 SO_REUSEPORT。
	OptionSoReusePort = NewChannelOption("SO_REUSEPORT", false)
	// OptionTcpNoDelay 控制 TCP_NODELAY，默认关闭 Nagle 以降低交互延迟。
	OptionTcpNoDelay = NewChannelOption("TCP_NODELAY", true)
	// OptionSoKeepAlive 控制连接 socket 的 SO_KEEPALIVE。
	OptionSoKeepAlive = NewChannelOption("SO_KEEPALIVE", false)
	// OptionSoSndBuf 控制 socket 发送缓冲区大小，0 表示使用系统默认值。
	OptionSoSndBuf = NewChannelOption("SO_SNDBUF", 0)
	// OptionSoRcvBuf 控制 socket 接收缓冲区大小，0 表示使用系统默认值。
	OptionSoRcvBuf = NewChannelOption("SO_RCVBUF", 0)
	// OptionSoLinger 控制连接 socket 的 SO_LINGER，-1 表示禁用 linger。
	OptionSoLinger = NewChannelOption("SO_LINGER", -1)
	// OptionWriteSpinCount 控制单次可写事件内的出站写重试次数。
	OptionWriteSpinCount = NewChannelOption("WRITE_SPIN_COUNT", 16)
	// OptionMaxMessagesPerRead 控制单次可读事件最多连续读取的消息数。
	OptionMaxMessagesPerRead = NewChannelOption("MAX_MESSAGES_PER_READ", 16)
)

func NewChannelOption[T any](name string, defaultValue T) ChannelOption[T] {
	return ChannelOption[T]{name: optionKeyID(name), defaultValue: defaultValue}
}

func (o ChannelOption[T]) Name() string {
	return string(o.name)
}

func (o ChannelOption[T]) Default() T {
	return o.defaultValue
}

func (o ChannelOption[T]) Get(options *ChannelOptions) T {
	if options == nil {
		return o.defaultValue
	}
	value, ok := options.get(o.name)
	if !ok {
		return o.defaultValue
	}
	typed, ok := value.(T)
	if !ok {
		return o.defaultValue
	}
	return typed
}

// GetIfSet 只在调用方显式配置该选项时返回值，用于区分零值和默认值。
func (o ChannelOption[T]) GetIfSet(options *ChannelOptions) (T, bool) {
	var zero T
	if options == nil {
		return zero, false
	}
	value, ok := options.get(o.name)
	if !ok {
		return zero, false
	}
	typed, ok := value.(T)
	if !ok {
		return zero, false
	}
	return typed, true
}

// IsSet 判断调用方是否显式配置了该选项。
func (o ChannelOption[T]) IsSet(options *ChannelOptions) bool {
	if options == nil {
		return false
	}
	_, ok := options.get(o.name)
	return ok
}

func (o ChannelOption[T]) Set(options *ChannelOptions, value T) {
	if options == nil {
		return
	}
	options.set(o.name, value)
}

func (o ChannelOption[T]) Remove(options *ChannelOptions) {
	if options == nil {
		return
	}
	options.remove(o.name)
}

// ChannelOptionAssignment 保存一个类型安全 ChannelOption 赋值。
type ChannelOptionAssignment struct {
	name  optionKeyID
	value any
}

// Assignment 把选项和值绑定成可复用的装配项。
func (o ChannelOption[T]) Assignment(value T) ChannelOptionAssignment {
	return ChannelOptionAssignment{name: o.name, value: value}
}

type ChannelOptions struct {
	attrs    AttributeMap
	observer optionObserver
}

type optionObserver func(key optionKeyID, value any, present bool)

func NewChannelOptions() *ChannelOptions {
	return &ChannelOptions{}
}

// Clone 复制当前选项集合。选项值按接口引用复制，不深拷贝业务对象。
func (o *ChannelOptions) Clone() *ChannelOptions {
	out := NewChannelOptions()
	o.CopyTo(out)
	return out
}

// Apply 写入一组选项装配项，后出现的同名选项覆盖前值。
func (o *ChannelOptions) Apply(assignments ...ChannelOptionAssignment) {
	if o == nil {
		return
	}
	for _, assignment := range assignments {
		if assignment.name == "" {
			continue
		}
		o.set(assignment.name, assignment.value)
	}
}

// CopyTo 把当前选项复制到目标集合，用于 Bootstrap 配置快照落到 Channel。
func (o *ChannelOptions) CopyTo(dst *ChannelOptions) {
	if o == nil || dst == nil {
		return
	}
	values := o.attrs.snapshot()
	for key, value := range values {
		dst.set(optionKeyID(key), value)
	}
}

func (o *ChannelOptions) get(key optionKeyID) (any, bool) {
	return o.attrs.get(attributeKeyID(key))
}

func (o *ChannelOptions) set(key optionKeyID, value any) {
	o.attrs.set(attributeKeyID(key), value)
	if o.observer != nil {
		o.observer(key, value, true)
	}
}

func (o *ChannelOptions) remove(key optionKeyID) {
	if _, ok := o.attrs.remove(attributeKeyID(key)); ok && o.observer != nil {
		o.observer(key, nil, false)
	}
}

func (o *ChannelOptions) observe(observer optionObserver) {
	if o == nil {
		return
	}
	o.observer = observer
}
