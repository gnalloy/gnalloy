package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// MessageDecoder 是 MessageToMessageDecoder 的策略接口。
type MessageDecoder interface {
	AcceptInboundMessage(msg any) bool
	Decode(ctx *channel.HandlerContext, msg any, out *MessageList) error
}

// MessageToMessageDecoder 对齐 Netty 的消息到消息入站转换模板。
type MessageToMessageDecoder struct {
	decoder MessageDecoder
	out     MessageList
}

func NewMessageToMessageDecoder(decoder MessageDecoder) *MessageToMessageDecoder {
	return &MessageToMessageDecoder{decoder: decoder}
}

func (d *MessageToMessageDecoder) ChannelRead(ctx *channel.HandlerContext, msg any) {
	if d.decoder == nil {
		releaseMessage(msg)
		ctx.FireExceptionCaught(ErrInvalidDecoder)
		return
	}
	if !d.decoder.AcceptInboundMessage(msg) {
		ctx.FireChannelRead(msg)
		return
	}
	d.out.Reset()
	err := d.decoder.Decode(ctx, msg, &d.out)
	releaseMessage(msg)
	if err != nil {
		d.out.ReleaseAll()
		ctx.FireExceptionCaught(err)
		return
	}
	for i := 0; i < d.out.Len(); i++ {
		ctx.FireChannelRead(d.out.At(i))
	}
	d.out.Reset()
}

type messageDecoderFunc struct {
	accept func(any) bool
	decode func(*channel.HandlerContext, any, *MessageList) error
}

func NewMessageToMessageDecoderFunc(accept func(any) bool, decode func(*channel.HandlerContext, any, *MessageList) error) *MessageToMessageDecoder {
	return NewMessageToMessageDecoder(messageDecoderFunc{accept: accept, decode: decode})
}

func (f messageDecoderFunc) AcceptInboundMessage(msg any) bool {
	return f.accept == nil || f.accept(msg)
}

func (f messageDecoderFunc) Decode(ctx *channel.HandlerContext, msg any, out *MessageList) error {
	return f.decode(ctx, msg, out)
}

func acceptByteBuf(msg any) bool {
	_, ok := msg.(buffer.ByteBuf)
	return ok
}
