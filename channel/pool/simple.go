package pool

// SimpleConfig 是无总连接数限制、仅限制 idle 数量的轻量池配置。
type SimpleConfig = Config

// SimplePool 对齐 Netty SimpleChannelPool，底层复用现有轻量 Pool 实现。
type SimplePool = Pool

// NewSimple 创建轻量 ChannelPool。
func NewSimple(cfg SimpleConfig) (*SimplePool, error) {
	return New(cfg)
}
