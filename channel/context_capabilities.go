package channel

// newHandlerContext 在装配阶段缓存高频处理器能力，避免 I/O 热路径重复做接口探测。
func newHandlerContext(pipeline *Pipeline, name string, handler Handler) *HandlerContext {
	ctx := &HandlerContext{pipeline: pipeline, name: name}
	ctx.bindHandler(handler)
	return ctx
}

func (c *HandlerContext) bindHandler(handler Handler) {
	c.handler = handler
	c.channelRegistered, _ = handler.(ChannelRegisteredHandler)
	c.channelUnregistered, _ = handler.(ChannelUnregisteredHandler)
	c.channelActive, _ = handler.(ChannelActiveHandler)
	c.channelRead, _ = handler.(ChannelReadHandler)
	c.channelReadComplete, _ = handler.(ChannelReadCompleteHandler)
	c.channelInactive, _ = handler.(ChannelInactiveHandler)
	c.channelWritabilityChanged, _ = handler.(ChannelWritabilityChangedHandler)
	c.userEventTriggered, _ = handler.(UserEventTriggeredHandler)
	c.exceptionCaught, _ = handler.(ExceptionCaughtHandler)
	c.write, _ = handler.(WriteHandler)
	c.writeFuture, _ = handler.(WriteFutureHandler)
	c.flush, _ = handler.(FlushHandler)
	c.flushFuture, _ = handler.(FlushFutureHandler)
	c.flushComplete, _ = handler.(FlushCompleteHandler)
	c.close, _ = handler.(CloseHandler)
	c.closeFuture, _ = handler.(CloseFutureHandler)
}
