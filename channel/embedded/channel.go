package embedded

import (
	"fmt"
	"sync"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
	"gnalloy.org/gnalloy/message"
	"gnalloy.org/gnalloy/transport"
)

// HandlerSpec 描述需要安装到 EmbeddedChannel 的处理器。
type HandlerSpec struct {
	Name    string
	Handler channel.Handler
}

// Config 描述 EmbeddedChannel 的测试态配置。
type Config struct {
	ID            transport.ChannelID
	Allocator     buffer.Allocator
	Handlers      []HandlerSpec
	SkipLifecycle bool
}

// EmbeddedChannel 是无 socket 的 Pipeline 驱动器。
type EmbeddedChannel struct {
	ch        *channel.LocalChannel
	sink      *sink
	collector *collector
}

// New 使用自动命名的 handler 创建 EmbeddedChannel。
func New(handlers ...channel.Handler) (*EmbeddedChannel, error) {
	specs := make([]HandlerSpec, 0, len(handlers))
	for i, handler := range handlers {
		specs = append(specs, HandlerSpec{Name: fmt.Sprintf("handler-%d", i), Handler: handler})
	}
	return NewWithConfig(Config{Handlers: specs})
}

// NewWithConfig 创建 EmbeddedChannel，并按需触发 registered/active 生命周期事件。
func NewWithConfig(cfg Config) (*EmbeddedChannel, error) {
	alloc := cfg.Allocator
	if alloc == nil {
		alloc = buffer.NewHeapAllocator()
	}
	id := cfg.ID
	if id == 0 {
		id = 1
	}
	out := &sink{}
	ch := channel.NewLocalChannel(id, alloc, out)
	ec := &EmbeddedChannel{ch: ch, sink: out, collector: &collector{}}
	for _, spec := range cfg.Handlers {
		if err := ch.Pipeline().AddLast(spec.Name, spec.Handler); err != nil {
			ec.ReleaseAll()
			return nil, err
		}
	}
	if err := ch.Pipeline().AddLast("$embeddedCollector", ec.collector); err != nil {
		ec.ReleaseAll()
		return nil, err
	}
	if !cfg.SkipLifecycle {
		ch.Pipeline().FireChannelRegistered()
		ch.Pipeline().FireChannelActive()
	}
	return ec, nil
}

// Channel 返回底层 LocalChannel。
func (c *EmbeddedChannel) Channel() channel.Channel {
	return c.ch
}

// Pipeline 返回底层 Pipeline，便于测试代码继续动态调整 handler。
func (c *EmbeddedChannel) Pipeline() *channel.Pipeline {
	return c.ch.Pipeline()
}

// WriteInbound 向 Pipeline 注入一个 inbound 消息。
func (c *EmbeddedChannel) WriteInbound(msg any) (bool, error) {
	if c == nil || c.sink.closed {
		releaseMessage(msg)
		return false, ErrClosed
	}
	before := c.collector.Len()
	c.ch.Pipeline().FireChannelRead(msg)
	c.ch.Pipeline().FireChannelReadComplete()
	return c.collector.Len() > before, nil
}

// ReadInbound 读取一个已捕获的 inbound 消息，返回后由调用方负责释放。
func (c *EmbeddedChannel) ReadInbound() (any, bool) {
	if c == nil {
		return nil, false
	}
	return c.collector.Pop()
}

// WriteOutbound 把消息写入 outbound 路径并立即 flush。
func (c *EmbeddedChannel) WriteOutbound(msg any) (bool, error) {
	if c == nil || c.sink.closed {
		releaseMessage(msg)
		return false, ErrClosed
	}
	before := c.sink.Len()
	if err := c.ch.WriteAndFlush(msg); err != nil {
		return c.sink.Len() > before, err
	}
	return c.sink.Len() > before, nil
}

// ReadOutbound 读取一个已捕获的 outbound 消息，返回后由调用方负责释放。
func (c *EmbeddedChannel) ReadOutbound() (any, bool) {
	if c == nil {
		return nil, false
	}
	return c.sink.Pop()
}

// Flushes 返回 outbound sink 收到的 flush 次数。
func (c *EmbeddedChannel) Flushes() int {
	if c == nil {
		return 0
	}
	return c.sink.Flushes()
}

// Finish 关闭 EmbeddedChannel 并返回是否仍有可读取消息。
func (c *EmbeddedChannel) Finish() bool {
	if c == nil {
		return false
	}
	_ = c.Close()
	return c.collector.Len()+c.sink.Len() > 0
}

// FinishAndReleaseAll 关闭 Channel 并释放所有未读取消息。
func (c *EmbeddedChannel) FinishAndReleaseAll() bool {
	hasMessages := c.Finish()
	c.ReleaseAll()
	return hasMessages
}

// Close 关闭 EmbeddedChannel，并触发 inactive/unregistered 生命周期。
func (c *EmbeddedChannel) Close() error {
	if c == nil {
		return ErrClosed
	}
	if err := c.sink.Close(); err != nil {
		return err
	}
	c.ch.Pipeline().FireChannelInactive()
	c.ch.Pipeline().FireChannelUnregistered()
	return nil
}

// ReleaseAll 释放所有未被测试代码读取的消息。
func (c *EmbeddedChannel) ReleaseAll() {
	if c == nil {
		return
	}
	c.collector.ReleaseAll()
	c.sink.ReleaseAll()
}

type collector struct {
	mu   sync.Mutex
	msgs []any
}

func (c *collector) ChannelRead(_ *channel.HandlerContext, msg any) {
	c.mu.Lock()
	c.msgs = append(c.msgs, msg)
	c.mu.Unlock()
}

func (c *collector) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.msgs)
}

func (c *collector) Pop() (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.msgs) == 0 {
		return nil, false
	}
	msg := c.msgs[0]
	copy(c.msgs, c.msgs[1:])
	c.msgs[len(c.msgs)-1] = nil
	c.msgs = c.msgs[:len(c.msgs)-1]
	return msg, true
}

func (c *collector) ReleaseAll() {
	c.mu.Lock()
	msgs := c.msgs
	c.msgs = nil
	c.mu.Unlock()
	releaseMessages(msgs)
}

type sink struct {
	mu      sync.Mutex
	msgs    []any
	flushes int
	closed  bool
}

func (s *sink) Write(msg any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		releaseMessage(msg)
		return ErrClosed
	}
	s.msgs = append(s.msgs, msg)
	return nil
}

func (s *sink) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.flushes++
	return nil
}

func (s *sink) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}

func (s *sink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

func (s *sink) Flushes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushes
}

func (s *sink) Pop() (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.msgs) == 0 {
		return nil, false
	}
	msg := s.msgs[0]
	copy(s.msgs, s.msgs[1:])
	s.msgs[len(s.msgs)-1] = nil
	s.msgs = s.msgs[:len(s.msgs)-1]
	return msg, true
}

func (s *sink) ReleaseAll() {
	s.mu.Lock()
	msgs := s.msgs
	s.msgs = nil
	s.mu.Unlock()
	releaseMessages(msgs)
}

func releaseMessages(msgs []any) {
	message.ReleaseAll(msgs)
}

func releaseMessage(msg any) {
	message.Release(msg)
}
