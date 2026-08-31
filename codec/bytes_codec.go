package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// ByteSliceDecoder 对齐 Netty ByteArrayDecoder，将 ByteBuf 复制为 []byte。
// 极致性能场景应直接在业务 Handler 中消费 ByteBuf，避免该复制。
type ByteSliceDecoder struct{}

func NewByteSliceDecoder() *ByteSliceDecoder {
	return &ByteSliceDecoder{}
}

func (d *ByteSliceDecoder) ChannelRead(ctx *channel.HandlerContext, msg any) {
	in, ok := msg.(buffer.ByteBuf)
	if !ok {
		ctx.FireChannelRead(msg)
		return
	}
	var out []byte
	if readable := in.ReadableBytes(); readable > 0 {
		out = make([]byte, readable)
		buffer.CopyReadableBytes(out, in)
	}
	in.Release()
	ctx.FireChannelRead(out)
}

// ByteSliceEncoder 将 []byte 复制进 Channel allocator 分配的 ByteBuf。
type ByteSliceEncoder struct{}

func NewByteSliceEncoder() *ByteSliceEncoder {
	return &ByteSliceEncoder{}
}

func (e *ByteSliceEncoder) Write(ctx *channel.HandlerContext, msg any) error {
	in, ok := msg.([]byte)
	if !ok {
		return ctx.Write(msg)
	}
	if len(in) == 0 {
		return nil
	}
	out, err := ctx.Channel().Allocator().Acquire(len(in))
	if err != nil {
		return err
	}
	if _, err := out.WriteBytes(in); err != nil {
		out.Release()
		return err
	}
	return WriteOutboundBuffer(ctx, out)
}
