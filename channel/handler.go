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

type WriteHandler interface {
	Write(ctx *HandlerContext, msg any) error
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
