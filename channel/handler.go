package channel

// Handler 是 Pipeline 中的处理器标记类型。
type Handler any

type HandlerAddedHandler interface {
	HandlerAdded(ctx *HandlerContext) error
}

type HandlerRemovedHandler interface {
	HandlerRemoved(ctx *HandlerContext) error
}

type ChannelRegisteredHandler interface {
	ChannelRegistered(ctx *HandlerContext)
}

type ChannelUnregisteredHandler interface {
	ChannelUnregistered(ctx *HandlerContext)
}

type ChannelActiveHandler interface {
	ChannelActive(ctx *HandlerContext)
}

type ChannelReadHandler interface {
	ChannelRead(ctx *HandlerContext, msg any)
}

type ChannelReadCompleteHandler interface {
	ChannelReadComplete(ctx *HandlerContext)
}

type ChannelInactiveHandler interface {
	ChannelInactive(ctx *HandlerContext)
}

type ChannelWritabilityChangedHandler interface {
	ChannelWritabilityChanged(ctx *HandlerContext)
}

type UserEventTriggeredHandler interface {
	UserEventTriggered(ctx *HandlerContext, event any)
}

type ExceptionCaughtHandler interface {
	ExceptionCaught(ctx *HandlerContext, err error)
}

// WriteHandler 处理出站消息；调用后消息所有权由实现接管，返回错误也不得交还上游。
type WriteHandler interface {
	Write(ctx *HandlerContext, msg any) error
}

// WriteAndFlushHandler 在一次出站调用中完成消息转换与 flush 传播。
// 未实现该接口的处理器继续使用独立 Write 和 Flush 契约。
// 调用后消息所有权由实现接管，返回错误也不得交还上游。
type WriteAndFlushHandler interface {
	WriteAndFlush(ctx *HandlerContext, msg any) error
}

type WriteFutureHandler interface {
	WriteFuture(ctx *HandlerContext, msg any) Future
}

type FlushHandler interface {
	Flush(ctx *HandlerContext) error
}

type FlushFutureHandler interface {
	FlushFuture(ctx *HandlerContext) Future
}

type FlushCompleteHandler interface {
	FlushComplete(ctx *HandlerContext)
}

type CloseHandler interface {
	Close(ctx *HandlerContext) error
}

type CloseFutureHandler interface {
	CloseFuture(ctx *HandlerContext) Future
}
