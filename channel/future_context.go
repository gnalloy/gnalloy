package channel

func writeFutureFrom(c *HandlerContext, msg any) Future {
	for n := c.prev; n != nil; n = n.prev {
		if n.writeFuture != nil {
			return n.writeFuture.WriteFuture(n, msg)
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
		if n.flushFuture != nil {
			return n.flushFuture.FlushFuture(n)
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
		if n.closeFuture != nil {
			return n.closeFuture.CloseFuture(n)
		}
		if n.close != nil {
			err := n.close.Close(n)
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
