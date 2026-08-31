package channel

func writeFutureFrom(c *HandlerContext, msg any) Future {
	for n := c.prev; n != nil; n = n.prev {
		if h, ok := n.handler.(interface {
			WriteFuture(ctx *HandlerContext, msg any) Future
		}); ok {
			return h.WriteFuture(n, msg)
		}
		if n.write != nil {
			err := n.write.Write(n, msg)
			if err != nil {
				return FailedFuture(err)
			}
			return SucceededFuture()
		}
	}
	if c.pipeline.sink == nil {
		return FailedFuture(ErrNoOutboundSink)
	}
	if sink, ok := c.pipeline.sink.(FutureOutboundSink); ok {
		return sink.WriteFuture(msg)
	}
	err := c.pipeline.sink.Write(msg)
	if err != nil {
		return FailedFuture(err)
	}
	return SucceededFuture()
}

func flushFutureFrom(c *HandlerContext) Future {
	for n := c.prev; n != nil; n = n.prev {
		if h, ok := n.handler.(interface {
			FlushFuture(ctx *HandlerContext) Future
		}); ok {
			return h.FlushFuture(n)
		}
		if n.flush != nil {
			err := n.flush.Flush(n)
			if err != nil {
				return FailedFuture(err)
			}
			return SucceededFuture()
		}
	}
	if c.pipeline.sink == nil {
		return FailedFuture(ErrNoOutboundSink)
	}
	if sink, ok := c.pipeline.sink.(FutureOutboundSink); ok {
		return sink.FlushFuture()
	}
	err := c.pipeline.sink.Flush()
	if err != nil {
		return FailedFuture(err)
	}
	return SucceededFuture()
}

func closeFutureFrom(c *HandlerContext) Future {
	for n := c.prev; n != nil; n = n.prev {
		if h, ok := n.handler.(interface {
			CloseFuture(ctx *HandlerContext) Future
		}); ok {
			return h.CloseFuture(n)
		}
		if h, ok := n.handler.(CloseHandler); ok {
			err := h.Close(n)
			if err != nil {
				return FailedFuture(err)
			}
			return SucceededFuture()
		}
	}
	if c.pipeline.sink == nil {
		return FailedFuture(ErrNoOutboundSink)
	}
	if sink, ok := c.pipeline.sink.(FutureOutboundSink); ok {
		return sink.CloseFuture()
	}
	err := c.pipeline.sink.Close()
	if err != nil {
		return FailedFuture(err)
	}
	return SucceededFuture()
}
