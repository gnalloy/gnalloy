package channel

import (
	"sync/atomic"

	"gnalloy.org/gnalloy/message"
)

type Pipeline struct {
	ch                    Channel
	sink                  OutboundSink
	writeAndFlush         writeAndFlushSink
	outboundHandlers      int
	readCompleteHandlers  int
	flushCompleteHandlers int
	writabilityHandlers   int
	concurrentWrite       atomic.Bool
	head                  *HandlerContext
	tail                  *HandlerContext
	names                 map[string]*HandlerContext
}

func NewPipeline(ch Channel, sink OutboundSink) *Pipeline {
	p := &Pipeline{
		ch:    ch,
		sink:  sink,
		names: make(map[string]*HandlerContext, 8),
	}
	if sink, ok := sink.(writeAndFlushSink); ok {
		p.writeAndFlush = sink
	}
	p.refreshConcurrentWrite()
	p.head = newHandlerContext(p, "$head", headHandler{})
	p.tail = newHandlerContext(p, "$tail", tailHandler{})
	p.head.next = p.tail
	p.tail.prev = p.head
	return p
}

func (p *Pipeline) Channel() Channel {
	return p.ch
}

func (p *Pipeline) AddLast(name string, h Handler) error {
	if err := p.validateNewHandler(name, h); err != nil {
		return err
	}
	ctx := newHandlerContext(p, name, h)
	prev := p.tail.prev
	p.linkBetween(prev, p.tail, ctx)
	if err := p.callHandlerAdded(ctx); err != nil {
		_ = p.unlink(ctx)
		return err
	}
	return nil
}

func (p *Pipeline) AddFirst(name string, h Handler) error {
	if err := p.validateNewHandler(name, h); err != nil {
		return err
	}
	ctx := newHandlerContext(p, name, h)
	next := p.head.next
	p.linkBetween(p.head, next, ctx)
	if err := p.callHandlerAdded(ctx); err != nil {
		_ = p.unlink(ctx)
		return err
	}
	return nil
}

// AddBefore 在已存在的处理器之前插入新处理器。
func (p *Pipeline) AddBefore(baseName string, name string, h Handler) error {
	base, ok := p.names[baseName]
	if !ok {
		return ErrHandlerNotFound
	}
	if err := p.validateNewHandler(name, h); err != nil {
		return err
	}
	ctx := newHandlerContext(p, name, h)
	p.linkBetween(base.prev, base, ctx)
	if err := p.callHandlerAdded(ctx); err != nil {
		_ = p.unlink(ctx)
		return err
	}
	return nil
}

// AddAfter 在已存在的处理器之后插入新处理器。
func (p *Pipeline) AddAfter(baseName string, name string, h Handler) error {
	base, ok := p.names[baseName]
	if !ok {
		return ErrHandlerNotFound
	}
	if err := p.validateNewHandler(name, h); err != nil {
		return err
	}
	ctx := newHandlerContext(p, name, h)
	p.linkBetween(base, base.next, ctx)
	if err := p.callHandlerAdded(ctx); err != nil {
		_ = p.unlink(ctx)
		return err
	}
	return nil
}

// Replace 用新处理器替换指定处理器，并保持原位置不变。
func (p *Pipeline) Replace(oldName string, newName string, h Handler) error {
	old, ok := p.names[oldName]
	if !ok {
		return ErrHandlerNotFound
	}
	if newName == "" || h == nil {
		return ErrHandlerNotFound
	}
	if newName != oldName {
		if _, exists := p.names[newName]; exists {
			return ErrDuplicateHandler
		}
	}
	replacement := newHandlerContext(p, newName, h)
	prev, next := old.prev, old.next
	p.disableConcurrentWrite()
	prev.next = replacement
	next.prev = replacement
	replacement.prev = prev
	replacement.next = next
	delete(p.names, oldName)
	p.names[newName] = replacement
	p.replaceHandlerCapabilities(old, replacement)
	old.prev = nil
	old.next = nil
	if err := p.callHandlerAdded(replacement); err != nil {
		_ = p.unlink(replacement)
		p.linkBetween(prev, next, old)
		return err
	}
	if removed, ok := old.handler.(HandlerRemovedHandler); ok {
		if err := removed.HandlerRemoved(old); err != nil {
			_ = p.unlink(replacement)
			p.linkBetween(prev, next, old)
			return err
		}
	}
	return nil
}

func (p *Pipeline) Remove(name string) error {
	ctx, ok := p.names[name]
	if !ok {
		return ErrHandlerNotFound
	}
	return p.unlink(ctx)
}

func (p *Pipeline) Context(name string) (*HandlerContext, bool) {
	ctx, ok := p.names[name]
	return ctx, ok
}

// FirstContext 返回第一个业务处理器上下文。
func (p *Pipeline) FirstContext() (*HandlerContext, bool) {
	if p.head.next == nil || p.head.next == p.tail {
		return nil, false
	}
	return p.head.next, true
}

// LastContext 返回最后一个业务处理器上下文。
func (p *Pipeline) LastContext() (*HandlerContext, bool) {
	if p.tail.prev == nil || p.tail.prev == p.head {
		return nil, false
	}
	return p.tail.prev, true
}

// Names 按入站执行顺序返回业务处理器名称快照。
func (p *Pipeline) Names() []string {
	names := make([]string, 0, len(p.names))
	for ctx := p.head.next; ctx != nil && ctx != p.tail; ctx = ctx.next {
		names = append(names, ctx.name)
	}
	return names
}

func (p *Pipeline) FireChannelActive() {
	p.head.FireChannelActive()
}

func (p *Pipeline) FireChannelRegistered() {
	p.head.FireChannelRegistered()
}

func (p *Pipeline) FireChannelUnregistered() {
	p.head.FireChannelUnregistered()
}

func (p *Pipeline) FireChannelRead(msg any) {
	p.head.FireChannelRead(msg)
}

func (p *Pipeline) FireChannelReadComplete() {
	if p.readCompleteHandlers == 0 {
		return
	}
	p.head.FireChannelReadComplete()
}

func (p *Pipeline) FireChannelInactive() {
	p.head.FireChannelInactive()
}

func (p *Pipeline) FireChannelWritabilityChanged() {
	p.head.FireChannelWritabilityChanged()
}

func (p *Pipeline) FireUserEventTriggered(event any) {
	p.head.FireUserEventTriggered(event)
}

func (p *Pipeline) FireExceptionCaught(err error) {
	p.head.FireExceptionCaught(err)
}

func (p *Pipeline) FireFlushComplete() {
	if p.flushCompleteHandlers == 0 {
		return
	}
	p.head.FireFlushComplete()
}

func (p *Pipeline) WriteFuture(msg any) Future {
	return p.tail.WriteFuture(msg)
}

func (p *Pipeline) Write(msg any) error {
	return p.tail.Write(msg)
}

func (p *Pipeline) WriteAndFlush(msg any) error {
	return p.tail.WriteAndFlush(msg)
}

func (p *Pipeline) tryWriteAndFlushConcurrent(msg any) (bool, error) {
	if p == nil || !p.concurrentWrite.Load() {
		return false, nil
	}
	sink, ok := p.sink.(concurrentWriteAndFlushSink)
	if !ok {
		return false, nil
	}
	return sink.TryWriteAndFlushConcurrent(msg)
}

func (p *Pipeline) FlushFuture() Future {
	return p.tail.FlushFuture()
}

func (p *Pipeline) Flush() error {
	return p.tail.Flush()
}

func (p *Pipeline) WriteAndFlushFuture(msg any) Future {
	writeFuture := p.WriteFuture(msg)
	if writeFuture.Err() != nil {
		return writeFuture
	}
	flushFuture := p.FlushFuture()
	return flushFuture
}

func (p *Pipeline) CloseFuture() Future {
	return p.tail.CloseFuture()
}

func (p *Pipeline) Close() error {
	return p.tail.Close()
}

func (p *Pipeline) callHandlerAdded(ctx *HandlerContext) error {
	if h, ok := ctx.handler.(HandlerAddedHandler); ok {
		return h.HandlerAdded(ctx)
	}
	return nil
}

func (p *Pipeline) validateNewHandler(name string, h Handler) error {
	if name == "" || h == nil {
		return ErrHandlerNotFound
	}
	if _, exists := p.names[name]; exists {
		return ErrDuplicateHandler
	}
	return nil
}

func (p *Pipeline) linkBetween(prev *HandlerContext, next *HandlerContext, ctx *HandlerContext) {
	p.disableConcurrentWrite()
	prev.next = ctx
	ctx.prev = prev
	ctx.next = next
	next.prev = ctx
	p.names[ctx.name] = ctx
	p.addHandlerCapabilities(ctx)
}

func (p *Pipeline) unlink(ctx *HandlerContext) error {
	if ctx == nil || ctx == p.head || ctx == p.tail {
		return ErrHandlerNotFound
	}
	p.disableConcurrentWrite()
	if h, ok := ctx.handler.(HandlerRemovedHandler); ok {
		if err := h.HandlerRemoved(ctx); err != nil {
			return err
		}
	}
	ctx.prev.next = ctx.next
	ctx.next.prev = ctx.prev
	ctx.prev = nil
	ctx.next = nil
	delete(p.names, ctx.name)
	p.removeHandlerCapabilities(ctx)
	return nil
}

func (p *Pipeline) addHandlerCapabilities(ctx *HandlerContext) {
	p.addHandlerCapabilitiesOnly(ctx)
	p.refreshConcurrentWrite()
}

func (p *Pipeline) addHandlerCapabilitiesOnly(ctx *HandlerContext) {
	if isOutboundContext(ctx) {
		p.outboundHandlers++
	}
	if ctx.channelReadComplete != nil {
		p.readCompleteHandlers++
	}
	if ctx.flushComplete != nil {
		p.flushCompleteHandlers++
	}
	if ctx.channelWritabilityChanged != nil {
		p.writabilityHandlers++
	}
}

func (p *Pipeline) removeHandlerCapabilities(ctx *HandlerContext) {
	p.removeHandlerCapabilitiesOnly(ctx)
	p.refreshConcurrentWrite()
}

func (p *Pipeline) removeHandlerCapabilitiesOnly(ctx *HandlerContext) {
	if isOutboundContext(ctx) && p.outboundHandlers > 0 {
		p.outboundHandlers--
	}
	if ctx.channelReadComplete != nil && p.readCompleteHandlers > 0 {
		p.readCompleteHandlers--
	}
	if ctx.flushComplete != nil && p.flushCompleteHandlers > 0 {
		p.flushCompleteHandlers--
	}
	if ctx.channelWritabilityChanged != nil && p.writabilityHandlers > 0 {
		p.writabilityHandlers--
	}
}

func (p *Pipeline) replaceHandlerCapabilities(old *HandlerContext, next *HandlerContext) {
	p.removeHandlerCapabilitiesOnly(old)
	p.addHandlerCapabilitiesOnly(next)
	p.refreshConcurrentWrite()
}

func (p *Pipeline) refreshConcurrentWrite() {
	_, supported := p.sink.(concurrentWriteAndFlushSink)
	enabled := supported && p.outboundHandlers == 0 && p.flushCompleteHandlers == 0 && p.writabilityHandlers == 0
	p.concurrentWrite.Store(enabled)
	if control, ok := p.sink.(concurrentWriteControl); ok {
		control.setConcurrentWriteEnabled(enabled)
	}
}

func (p *Pipeline) disableConcurrentWrite() {
	p.concurrentWrite.Store(false)
	if control, ok := p.sink.(concurrentWriteControl); ok {
		control.setConcurrentWriteEnabled(false)
	}
}

func isOutboundContext(ctx *HandlerContext) bool {
	return ctx != nil && (ctx.write != nil || ctx.writeAndFlush != nil || ctx.flush != nil)
}

type headHandler struct{}

type tailHandler struct{}

func (tailHandler) ChannelRead(_ *HandlerContext, msg any) {
	message.Release(msg)
}

func (tailHandler) ExceptionCaught(_ *HandlerContext, _ error) {}
