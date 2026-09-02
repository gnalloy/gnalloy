package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type ChunkedInput interface {
	ReadChunk(alloc buffer.Allocator) (chunk buffer.ByteBuf, done bool, err error)
	Close() error
}

type ChunkedWriteHandler struct{}

func NewChunkedWriteHandler() *ChunkedWriteHandler {
	return &ChunkedWriteHandler{}
}

func (h *ChunkedWriteHandler) Write(ctx *channel.HandlerContext, msg any) error {
	input, ok := msg.(ChunkedInput)
	if !ok {
		return ctx.Write(msg)
	}
	defer input.Close()
	for {
		chunk, done, err := input.ReadChunk(ctx.Channel().Allocator())
		if err != nil {
			if chunk != nil {
				chunk.Release()
			}
			return err
		}
		if chunk != nil && chunk.ReadableBytes() > 0 {
			if err := ctx.Write(chunk); err != nil {
				return err
			}
		} else if chunk != nil {
			chunk.Release()
		}
		if done {
			return ctx.Flush()
		}
	}
}

type ChunkedByteBufInput struct {
	buf       buffer.ByteBuf
	chunkSize int
	closed    bool
}

func NewChunkedByteBufInput(buf buffer.ByteBuf, chunkSize int) (*ChunkedByteBufInput, error) {
	if buf == nil || chunkSize <= 0 {
		if buf != nil {
			buf.Release()
		}
		return nil, ErrInvalidFrameLength
	}
	return &ChunkedByteBufInput{buf: buf, chunkSize: chunkSize}, nil
}

func (i *ChunkedByteBufInput) ReadChunk(buffer.Allocator) (buffer.ByteBuf, bool, error) {
	if i.closed || i.buf == nil || i.buf.ReadableBytes() == 0 {
		return nil, true, nil
	}
	length := i.chunkSize
	if length > i.buf.ReadableBytes() {
		length = i.buf.ReadableBytes()
	}
	chunk, err := i.buf.Slice(i.buf.ReaderIndex(), length)
	if err != nil {
		return nil, false, err
	}
	if err := i.buf.SkipBytes(length); err != nil {
		chunk.Release()
		return nil, false, err
	}
	return chunk, i.buf.ReadableBytes() == 0, nil
}

func (i *ChunkedByteBufInput) Close() error {
	if i.closed {
		return nil
	}
	i.closed = true
	if i.buf != nil {
		i.buf.Release()
		i.buf = nil
	}
	return nil
}
