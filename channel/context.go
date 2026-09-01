package channel

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport"
)

type writeAndFlushSink interface {
	WriteAndFlush(msg any) error
}

type staticBytesWriteAndFlushSink interface {
	WriteStaticBytesAndFlush(data []byte) error
}

type HandlerContext struct {
	pipeline *Pipeline
	name     string
	handler  Handler
	prev     *HandlerContext
	next     *HandlerContext

	channelRegistered         ChannelRegisteredHandler
	channelUnregistered       ChannelUnregisteredHandler
	channelActive             ChannelActiveHandler
	channelRead               ChannelReadHandler
	channelReadComplete       ChannelReadCompleteHandler
	channelInactive           ChannelInactiveHandler
	channelWritabilityChanged ChannelWritabilityChangedHandler
	userEventTriggered        UserEventTriggeredHandler
	exceptionCaught           ExceptionCaughtHandler
	write                     WriteHandler
	writeFuture               WriteFutureHandler
	flush                     FlushHandler
	flushFuture               FlushFutureHandler
	flushComplete             FlushCompleteHandler
	close                     CloseHandler
	closeFuture               CloseFutureHandler
}

func (c *HandlerContext) Name() string {
	return c.name
}

func (c *HandlerContext) Channel() Channel {
	return c.pipeline.ch
}

func (c *HandlerContext) Pipeline() *Pipeline {
	return c.pipeline
}

func (c *HandlerContext) Handler() Handler {
	return c.handler
}

// EventExecutor 返回当前 Channel 绑定的 owner EventLoop 执行器。
func (c *HandlerContext) EventExecutor() FutureListenerExecutor {
	if c == nil || c.pipeline == nil {
		return nil
	}
	ch, ok := c.pipeline.ch.(*LocalChannel)
	if !ok {
		return nil
	}
	return ch.ownerExecutor()
}

// Execute 在 owner EventLoop 上执行任务；未绑定 EventLoop 的本地 Channel 直接执行。
func (c *HandlerContext) Execute(task transport.Task) error {
	if task == nil {
		return nil
	}
	if executor := c.EventExecutor(); executor != nil {
		return executor.Submit(task)
	}
	task()
	return nil
}

func (c *HandlerContext) FireChannelRegistered() {
	for n := c.next; n != nil; n = n.next {
		if n.channelRegistered != nil {
			n.channelRegistered.ChannelRegistered(n)
			return
		}
	}
}

func (c *HandlerContext) FireChannelUnregistered() {
	for n := c.next; n != nil; n = n.next {
		if n.channelUnregistered != nil {
			n.channelUnregistered.ChannelUnregistered(n)
			return
		}
	}
}

func (c *HandlerContext) FireChannelActive() {
	for n := c.next; n != nil; n = n.next {
		if n.channelActive != nil {
			n.channelActive.ChannelActive(n)
			return
		}
	}
}

func (c *HandlerContext) FireChannelRead(msg any) {
	for n := c.next; n != nil; n = n.next {
		if n.channelRead != nil {
			n.channelRead.ChannelRead(n, msg)
			return
		}
	}
}

func (c *HandlerContext) FireChannelReadComplete() {
	for n := c.next; n != nil; n = n.next {
		if n.channelReadComplete != nil {
			n.channelReadComplete.ChannelReadComplete(n)
			return
		}
	}
}

func (c *HandlerContext) FireChannelInactive() {
	for n := c.next; n != nil; n = n.next {
		if n.channelInactive != nil {
			n.channelInactive.ChannelInactive(n)
			return
		}
	}
}

func (c *HandlerContext) FireChannelWritabilityChanged() {
	for n := c.next; n != nil; n = n.next {
		if n.channelWritabilityChanged != nil {
			n.channelWritabilityChanged.ChannelWritabilityChanged(n)
			return
		}
	}
}

func (c *HandlerContext) FireUserEventTriggered(event any) {
	for n := c.next; n != nil; n = n.next {
		if n.userEventTriggered != nil {
			n.userEventTriggered.UserEventTriggered(n, event)
			return
		}
	}
}

func (c *HandlerContext) FireFlushComplete() {
	for n := c.next; n != nil; n = n.next {
		if n.flushComplete != nil {
			n.flushComplete.FlushComplete(n)
			return
		}
	}
}

func (c *HandlerContext) FireExceptionCaught(err error) {
	for n := c.next; n != nil; n = n.next {
		if n.exceptionCaught != nil {
			n.exceptionCaught.ExceptionCaught(n, err)
			return
		}
	}
}

func (c *HandlerContext) WriteFuture(msg any) Future {
	return writeFutureFrom(c, msg)
}

func (c *HandlerContext) Write(msg any) error {
	for n := c.prev; n != nil; n = n.prev {
		if n.write != nil {
			return n.write.Write(n, msg)
		}
	}
	if c.pipeline.sink == nil {
		return ErrNoOutboundSink
	}
	return c.pipeline.sink.Write(msg)
}

func (c *HandlerContext) FlushFuture() Future {
	return flushFutureFrom(c)
}

func (c *HandlerContext) Flush() error {
	for n := c.prev; n != nil; n = n.prev {
		if n.flush != nil {
			return n.flush.Flush(n)
		}
	}
	if c.pipeline.sink == nil {
		return ErrNoOutboundSink
	}
	return c.pipeline.sink.Flush()
}

func (c *HandlerContext) WriteAndFlush(msg any) error {
	if sink, ok := c.directWriteAndFlushSink(); ok {
		return sink.WriteAndFlush(msg)
	}
	if err := c.Write(msg); err != nil {
		return err
	}
	return c.Flush()
}

// WriteStaticBytesAndFlush 写出不可变静态字节并立即 flush。
//
// 调用方必须保证 data 在写出完成或连接关闭前不被修改；该方法用于协议常量帧、
// 固定响应体等高频路径。有出站 handler 时仍走常规 ByteBuf pipeline。
func (c *HandlerContext) WriteStaticBytesAndFlush(data []byte) error {
	if len(data) == 0 {
		return c.Flush()
	}
	if sink, ok := c.directStaticBytesWriteAndFlushSink(); ok {
		return sink.WriteStaticBytesAndFlush(data)
	}
	return c.WriteAndFlush(buffer.NewSharedBuffer(data))
}

func (c *HandlerContext) directWriteAndFlushSink() (writeAndFlushSink, bool) {
	if c == nil || c.pipeline == nil || c.pipeline.writeAndFlush == nil {
		return nil, false
	}
	if c.pipeline.outboundHandlers != 0 {
		return nil, false
	}
	return c.pipeline.writeAndFlush, true
}

func (c *HandlerContext) directStaticBytesWriteAndFlushSink() (staticBytesWriteAndFlushSink, bool) {
	if c == nil || c.pipeline == nil || c.pipeline.outboundHandlers != 0 {
		return nil, false
	}
	sink, ok := c.pipeline.sink.(staticBytesWriteAndFlushSink)
	return sink, ok
}

func (c *HandlerContext) CloseFuture() Future {
	return closeFutureFrom(c)
}

func (c *HandlerContext) Close() error {
	for n := c.prev; n != nil; n = n.prev {
		if n.close != nil {
			return n.close.Close(n)
		}
	}
	if c.pipeline.sink == nil {
		return ErrNoOutboundSink
	}
	return c.pipeline.sink.Close()
}
