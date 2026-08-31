//go:build linux

package iouring

import (
	"unsafe"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport/poller"
)

const (
	inlineWriteVectors      = 16
	maxRetainedWriteVectors = 64
)

// writeVectorContext 保存提交给内核的 iovec，直到对应 CQE 回收前地址必须稳定。
type writeVectorContext struct {
	inline  [inlineWriteVectors]iovec
	vectors []iovec
	next    *writeVectorContext
}

func (p *Poller) acquireWriteVectorContext(req poller.IORequest) (*writeVectorContext, error) {
	ctx := p.freeWritev
	if ctx == nil {
		ctx = &writeVectorContext{}
	} else {
		p.freeWritev = ctx.next
		ctx.next = nil
	}
	vectors, err := makeIOVectors(req, ctx.scratch())
	if err != nil {
		p.recycleWriteVectorContext(ctx)
		return nil, err
	}
	ctx.vectors = vectors
	return ctx, nil
}

func (p *Poller) releaseWriteVectorContext(id uint64) {
	ctx := p.writev[id]
	if ctx == nil {
		return
	}
	delete(p.writev, id)
	p.recycleWriteVectorContext(ctx)
}

func (p *Poller) recycleWriteVectorContext(ctx *writeVectorContext) {
	if ctx == nil {
		return
	}
	ctx.reset()
	ctx.next = p.freeWritev
	p.freeWritev = ctx
}

func (ctx *writeVectorContext) scratch() []iovec {
	if ctx == nil {
		return nil
	}
	if cap(ctx.vectors) <= inlineWriteVectors {
		return ctx.inline[:0]
	}
	return ctx.vectors[:0]
}

func (ctx *writeVectorContext) reset() {
	clear(ctx.vectors)
	if cap(ctx.vectors) > maxRetainedWriteVectors {
		ctx.vectors = nil
		return
	}
	if ctx.vectors != nil {
		ctx.vectors = ctx.vectors[:0]
	}
}

func makeIOVectors(req poller.IORequest, dst []iovec) ([]iovec, error) {
	vectors := dst
	if vectors == nil {
		vectors = make([]iovec, 0, iovCapacityHint(req))
	}
	var err error
	if req.Buf != nil {
		vectors, err = appendIOVectors(vectors, req.Buf)
	} else {
		for _, buf := range req.Bufs {
			vectors, err = appendIOVectors(vectors, buf)
			if err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if len(vectors) == 0 {
		return nil, poller.ErrInvalidIORequest
	}
	return vectors, nil
}

func iovCapacityHint(req poller.IORequest) int {
	if req.Buf != nil {
		return readableVectorCapacityHint(req.Buf)
	}
	n := 0
	for _, buf := range req.Bufs {
		n += readableVectorCapacityHint(buf)
	}
	if n == 0 {
		return 1
	}
	return n
}

func readableVectorCapacityHint(src buffer.ByteBuf) int {
	if src == nil || src.ReadableBytes() == 0 {
		return 0
	}
	if composite, ok := src.(*buffer.CompositeByteBuf); ok {
		return composite.ComponentCount()
	}
	return 1
}

func appendIOVectors(dst []iovec, src buffer.ByteBuf) ([]iovec, error) {
	if src == nil || src.ReadableBytes() == 0 {
		return dst, nil
	}
	if data, ok := buffer.ContiguousReadableBytes(src); ok {
		return appendIovec(dst, data), nil
	}
	before := len(dst)
	buffer.ForEachReadableSlice(src, func(data []byte) bool {
		if len(data) == 0 {
			return true
		}
		dst = appendIovec(dst, data)
		return true
	})
	if len(dst) == before {
		return nil, poller.ErrInvalidIORequest
	}
	return dst, nil
}

func appendIovec(dst []iovec, data []byte) []iovec {
	return append(dst, iovec{
		base: uintptr(unsafe.Pointer(&data[0])),
		len:  uintptr(len(data)),
	})
}
