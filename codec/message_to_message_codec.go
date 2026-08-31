package codec

import "gnalloy.org/gnalloy/channel"

// MessageToMessageCodec 组合入站消息解码器与出站消息编码器。
type MessageToMessageCodec struct {
	inbound  *MessageToMessageDecoder
	outbound *MessageToMessageEncoder
}

func NewMessageToMessageCodec(decoder MessageDecoder, encoder MessageEncoder) *MessageToMessageCodec {
	return &MessageToMessageCodec{
		inbound:  NewMessageToMessageDecoder(decoder),
		outbound: NewMessageToMessageEncoder(encoder),
	}
}

func (c *MessageToMessageCodec) ChannelRead(ctx *channel.HandlerContext, msg any) {
	c.inbound.ChannelRead(ctx, msg)
}

func (c *MessageToMessageCodec) Write(ctx *channel.HandlerContext, msg any) error {
	return c.outbound.Write(ctx, msg)
}
