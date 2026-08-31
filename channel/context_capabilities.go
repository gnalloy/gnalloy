package channel

// newHandlerContext 在装配阶段缓存高频处理器能力，避免 I/O 热路径重复做接口探测。
func newHandlerContext(pipeline *Pipeline, name string, handler Handler) *HandlerContext {
	ctx := &HandlerContext{pipeline: pipeline, name: name}
	ctx.bindHandler(handler)
	return ctx
}

func (c *HandlerContext) bindHandler(handler Handler) {
	c.handler = handler
	c.channelRead, _ = handler.(ChannelReadHandler)
	c.channelReadComplete, _ = handler.(ChannelReadCompleteHandler)
	c.exceptionCaught, _ = handler.(ExceptionCaughtHandler)
	c.write, _ = handler.(WriteHandler)
	c.flush, _ = handler.(FlushHandler)
}
