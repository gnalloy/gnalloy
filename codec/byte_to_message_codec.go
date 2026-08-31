package codec

import "gnalloy.org/gnalloy/channel"

// ByteToMessageCodec 组合入站 ByteToMessageDecoder 与出站 MessageToByteEncoder。
type ByteToMessageCodec struct {
	inbound  *ByteToMessageDecoder
	outbound *MessageToByteEncoder
}

func NewByteToMessageCodec(decoder ByteDecoder, encoder ByteEncoder) *ByteToMessageCodec {
	return &ByteToMessageCodec{
		inbound:  NewByteToMessageDecoder(decoder),
		outbound: NewMessageToByteEncoder(encoder),
	}
}

func (c *ByteToMessageCodec) ChannelRead(ctx *channel.HandlerContext, msg any) {
	c.inbound.ChannelRead(ctx, msg)
}

func (c *ByteToMessageCodec) ChannelInactive(ctx *channel.HandlerContext) {
	c.inbound.ChannelInactive(ctx)
}

func (c *ByteToMessageCodec) Write(ctx *channel.HandlerContext, msg any) error {
	return c.outbound.Write(ctx, msg)
}
