package codec

import "gnalloy.org/gnalloy/channel"

// CombinedChannelDuplexHandler 将一个入站 Handler 与一个出站 Handler 组合成双工 Handler。
type CombinedChannelDuplexHandler struct {
	inbound  channel.Handler
	outbound channel.Handler
}

func NewCombinedChannelDuplexHandler(inbound channel.Handler, outbound channel.Handler) *CombinedChannelDuplexHandler {
	return &CombinedChannelDuplexHandler{inbound: inbound, outbound: outbound}
}

func (h *CombinedChannelDuplexHandler) ChannelActive(ctx *channel.HandlerContext) {
	if next, ok := h.inbound.(channel.ChannelActiveHandler); ok {
		next.ChannelActive(ctx)
		return
	}
	ctx.FireChannelActive()
}

func (h *CombinedChannelDuplexHandler) ChannelRegistered(ctx *channel.HandlerContext) {
	if next, ok := h.inbound.(channel.ChannelRegisteredHandler); ok {
		next.ChannelRegistered(ctx)
		return
	}
	ctx.FireChannelRegistered()
}

func (h *CombinedChannelDuplexHandler) ChannelUnregistered(ctx *channel.HandlerContext) {
	if next, ok := h.inbound.(channel.ChannelUnregisteredHandler); ok {
		next.ChannelUnregistered(ctx)
		return
	}
	ctx.FireChannelUnregistered()
}

func (h *CombinedChannelDuplexHandler) ChannelRead(ctx *channel.HandlerContext, msg any) {
	if next, ok := h.inbound.(channel.ChannelReadHandler); ok {
		next.ChannelRead(ctx, msg)
		return
	}
	ctx.FireChannelRead(msg)
}

func (h *CombinedChannelDuplexHandler) ChannelReadComplete(ctx *channel.HandlerContext) {
	if next, ok := h.inbound.(channel.ChannelReadCompleteHandler); ok {
		next.ChannelReadComplete(ctx)
		return
	}
	ctx.FireChannelReadComplete()
}

func (h *CombinedChannelDuplexHandler) ChannelInactive(ctx *channel.HandlerContext) {
	if next, ok := h.inbound.(channel.ChannelInactiveHandler); ok {
		next.ChannelInactive(ctx)
		return
	}
	ctx.FireChannelInactive()
}

func (h *CombinedChannelDuplexHandler) ChannelWritabilityChanged(ctx *channel.HandlerContext) {
	if next, ok := h.inbound.(channel.ChannelWritabilityChangedHandler); ok {
		next.ChannelWritabilityChanged(ctx)
		return
	}
	ctx.FireChannelWritabilityChanged()
}

func (h *CombinedChannelDuplexHandler) UserEventTriggered(ctx *channel.HandlerContext, event any) {
	if next, ok := h.inbound.(channel.UserEventTriggeredHandler); ok {
		next.UserEventTriggered(ctx, event)
		return
	}
	ctx.FireUserEventTriggered(event)
}

func (h *CombinedChannelDuplexHandler) ExceptionCaught(ctx *channel.HandlerContext, err error) {
	if next, ok := h.inbound.(channel.ExceptionCaughtHandler); ok {
		next.ExceptionCaught(ctx, err)
		return
	}
	ctx.FireExceptionCaught(err)
}

func (h *CombinedChannelDuplexHandler) Write(ctx *channel.HandlerContext, msg any) error {
	if next, ok := h.outbound.(channel.WriteHandler); ok {
		return next.Write(ctx, msg)
	}
	return ctx.Write(msg)
}

func (h *CombinedChannelDuplexHandler) Flush(ctx *channel.HandlerContext) error {
	if next, ok := h.outbound.(channel.FlushHandler); ok {
		return next.Flush(ctx)
	}
	return ctx.Flush()
}

func (h *CombinedChannelDuplexHandler) FlushComplete(ctx *channel.HandlerContext) {
	if next, ok := h.outbound.(channel.FlushCompleteHandler); ok {
		next.FlushComplete(ctx)
		return
	}
	ctx.FireFlushComplete()
}

func (h *CombinedChannelDuplexHandler) Close(ctx *channel.HandlerContext) error {
	if next, ok := h.outbound.(channel.CloseHandler); ok {
		return next.Close(ctx)
	}
	return ctx.Close()
}
