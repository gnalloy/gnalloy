package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// WriteOutboundBuffer 写出已完成编码的 ByteBuf，并把引用所有权转移给下游。
func WriteOutboundBuffer(ctx *channel.HandlerContext, out buffer.ByteBuf) error {
	return ctx.Write(out)
}

// WriteOutboundBufferAndFlush 写出已完成编码的 ByteBuf 并立即 flush，同时把引用所有权转移给下游。
func WriteOutboundBufferAndFlush(ctx *channel.HandlerContext, out buffer.ByteBuf) error {
	return ctx.WriteAndFlush(out)
}
