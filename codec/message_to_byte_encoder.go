package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

const defaultEncoderBufferSize = 256

// ByteEncoder 是 MessageToByteEncoder 的策略接口。
type ByteEncoder interface {
	AcceptOutboundMessage(msg any) bool
	EstimateSize(ctx *channel.HandlerContext, msg any) int
	Encode(ctx *channel.HandlerContext, msg any, out buffer.ByteBuf) error
}

// MessageToByteEncoder 对齐 Netty 的消息到 ByteBuf 出站编码模板。
type MessageToByteEncoder struct {
	encoder ByteEncoder
}

func NewMessageToByteEncoder(encoder ByteEncoder) *MessageToByteEncoder {
	return &MessageToByteEncoder{encoder: encoder}
}

func (e *MessageToByteEncoder) Write(ctx *channel.HandlerContext, msg any) error {
	if e.encoder == nil {
		releaseMessage(msg)
		return ErrInvalidEncoder
	}
	if !e.encoder.AcceptOutboundMessage(msg) {
		return ctx.Write(msg)
	}
	size := e.encoder.EstimateSize(ctx, msg)
	if size <= 0 {
		size = defaultEncoderBufferSize
	}
	out, err := ctx.Channel().Allocator().Acquire(size)
	if err != nil {
		releaseMessage(msg)
		return err
	}
	err = e.encoder.Encode(ctx, msg, out)
	releaseMessage(msg)
	if err != nil {
		out.Release()
		return err
	}
	if out.ReadableBytes() == 0 {
		out.Release()
		return nil
	}
	return WriteOutboundBuffer(ctx, out)
}

type byteEncoderFunc struct {
	accept   func(any) bool
	estimate func(*channel.HandlerContext, any) int
	encode   func(*channel.HandlerContext, any, buffer.ByteBuf) error
}

func NewMessageToByteEncoderFunc(accept func(any) bool, estimate func(*channel.HandlerContext, any) int, encode func(*channel.HandlerContext, any, buffer.ByteBuf) error) *MessageToByteEncoder {
	return NewMessageToByteEncoder(byteEncoderFunc{accept: accept, estimate: estimate, encode: encode})
}

func (f byteEncoderFunc) AcceptOutboundMessage(msg any) bool {
	return f.accept == nil || f.accept(msg)
}

func (f byteEncoderFunc) EstimateSize(ctx *channel.HandlerContext, msg any) int {
	if f.estimate == nil {
		return defaultEncoderBufferSize
	}
	return f.estimate(ctx, msg)
}

func (f byteEncoderFunc) Encode(ctx *channel.HandlerContext, msg any, out buffer.ByteBuf) error {
	return f.encode(ctx, msg, out)
}
