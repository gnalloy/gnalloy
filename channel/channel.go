package channel

import (
	"sync/atomic"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/timer"
	"gnalloy.org/gnalloy/transport"
)

// Channel 是业务层看到的连接抽象，不暴露 fd、CQE 等平台细节。
type Channel interface {
	ID() transport.ChannelID
	Pipeline() *Pipeline
	Allocator() buffer.Allocator
	Attributes() *AttributeMap
	Options() *ChannelOptions
	Write(msg any) error
	Flush() error
	WriteAndFlush(msg any) error
	WriteFuture(msg any) Future
	FlushFuture() Future
	WriteAndFlushFuture(msg any) Future
	CloseFuture() Future
	Close() error
	Read() error
	IsWritable() bool
	PendingOutboundBytes() int64
	WriteBufferWatermark() transport.WriteBufferWatermark
	ScheduleTimer(delayMillis int64, cb timer.Callback) (*timer.Task, error)
	CancelTimer(task *timer.Task) bool
}

// OutboundSink 是出站事件穿过 Pipeline 后的最终写出端。
type OutboundSink interface {
	Write(msg any) error
	Flush() error
	Close() error
}

type FutureOutboundSink interface {
	WriteFuture(msg any) Future
	FlushFuture() Future
	CloseFuture() Future
}

type WritabilitySink interface {
	IsWritable() bool
}

type ReadSink interface {
	Read() error
}

type OutboundBufferSink interface {
	WritabilitySink
	PendingOutboundBytes() int64
	WriteBufferWatermark() transport.WriteBufferWatermark
}

type LocalChannel struct {
	id       transport.ChannelID
	pipeline *Pipeline
	alloc    buffer.Allocator
	timer    *timer.Wheel
	attrs    *AttributeMap
	options  *ChannelOptions

	eventExecutor atomic.Value
	ownerWrites   ownerWriteQueue
}

func NewLocalChannel(id transport.ChannelID, alloc buffer.Allocator, sink OutboundSink) *LocalChannel {
	return NewLocalChannelWithTimer(id, alloc, sink, nil)
}

func NewLocalChannelWithTimer(id transport.ChannelID, alloc buffer.Allocator, sink OutboundSink, wheel *timer.Wheel) *LocalChannel {
	ch := &LocalChannel{id: id, alloc: alloc, timer: wheel, attrs: NewAttributeMap(), options: NewChannelOptions()}
	OptionAutoRead.Set(ch.options, true)
	ch.pipeline = NewPipeline(ch, sink)
	return ch
}

func (c *LocalChannel) ID() transport.ChannelID {
	return c.id
}

func (c *LocalChannel) Pipeline() *Pipeline {
	return c.pipeline
}

func (c *LocalChannel) Allocator() buffer.Allocator {
	return c.alloc
}

func (c *LocalChannel) Attributes() *AttributeMap {
	return c.attrs
}

func (c *LocalChannel) Options() *ChannelOptions {
	return c.options
}

func (c *LocalChannel) Write(msg any) error {
	if c.ownerExecutor() == nil {
		return c.pipeline.Write(msg)
	}
	future := c.WriteFuture(msg)
	if future.IsDone() {
		return future.Err()
	}
	return nil
}

func (c *LocalChannel) WriteFuture(msg any) Future {
	if executor := c.ownerExecutor(); executor != nil {
		return c.submitOwnerFuture(executor, msg, func() Future {
			return c.pipeline.WriteFuture(msg)
		})
	}
	return c.pipeline.WriteFuture(msg)
}

func (c *LocalChannel) Flush() error {
	if c.ownerExecutor() == nil {
		return c.pipeline.Flush()
	}
	future := c.FlushFuture()
	if future.IsDone() {
		return future.Err()
	}
	return nil
}

func (c *LocalChannel) FlushFuture() Future {
	if executor := c.ownerExecutor(); executor != nil {
		return c.submitOwnerFuture(executor, nil, func() Future {
			return c.pipeline.FlushFuture()
		})
	}
	return c.pipeline.FlushFuture()
}

func (c *LocalChannel) WriteAndFlush(msg any) error {
	if executor := c.ownerExecutor(); executor != nil {
		return c.submitOwnerWriteAndFlush(executor, msg)
	}
	return c.pipeline.WriteAndFlush(msg)
}

func (c *LocalChannel) WriteAndFlushFuture(msg any) Future {
	if executor := c.ownerExecutor(); executor != nil {
		return c.submitOwnerFuture(executor, msg, func() Future {
			return c.pipeline.WriteAndFlushFuture(msg)
		})
	}
	return c.pipeline.WriteAndFlushFuture(msg)
}

func (c *LocalChannel) CloseFuture() Future {
	if executor := c.ownerExecutor(); executor != nil {
		return c.submitOwnerFuture(executor, nil, func() Future {
			return c.pipeline.CloseFuture()
		})
	}
	return c.pipeline.CloseFuture()
}

func (c *LocalChannel) Close() error {
	future := c.CloseFuture()
	if future.IsDone() {
		return future.Err()
	}
	return nil
}

func (c *LocalChannel) Read() error {
	if executor := c.ownerExecutor(); executor != nil {
		return executor.Submit(func() {
			if err := c.readDirect(); err != nil {
				c.pipeline.FireExceptionCaught(err)
			}
		})
	}
	return c.readDirect()
}

func (c *LocalChannel) readDirect() error {
	if sink, ok := c.pipeline.sink.(ReadSink); ok {
		return sink.Read()
	}
	return nil
}

func (c *LocalChannel) IsWritable() bool {
	if sink, ok := c.pipeline.sink.(WritabilitySink); ok {
		return sink.IsWritable()
	}
	return false
}

func (c *LocalChannel) PendingOutboundBytes() int64 {
	if sink, ok := c.pipeline.sink.(OutboundBufferSink); ok {
		return sink.PendingOutboundBytes()
	}
	return 0
}

func (c *LocalChannel) WriteBufferWatermark() transport.WriteBufferWatermark {
	if sink, ok := c.pipeline.sink.(OutboundBufferSink); ok {
		return sink.WriteBufferWatermark()
	}
	return transport.DefaultWriteBufferWatermark()
}

func (c *LocalChannel) ScheduleTimer(delayMillis int64, cb timer.Callback) (*timer.Task, error) {
	if c.timer == nil {
		return nil, ErrNoTimer
	}
	return c.timer.Schedule(delayMillis, cb)
}

func (c *LocalChannel) CancelTimer(task *timer.Task) bool {
	if c.timer == nil {
		return false
	}
	return c.timer.Cancel(task)
}
