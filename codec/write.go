package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// WriteOutboundBuffer 写出已完成编码的 ByteBuf；写失败时由当前 encoder 归还引用。
func WriteOutboundBuffer(ctx *channel.HandlerContext, out buffer.ByteBuf) error {
	if err := ctx.Write(out); err != nil {
		out.Release()
		return err
	}
	return nil
}

// WriteOutboundBufferAndFlush 写出已完成编码的 ByteBuf 并立即 flush；写失败时由当前 encoder 归还引用。
func WriteOutboundBufferAndFlush(ctx *channel.HandlerContext, out buffer.ByteBuf) error {
	if err := ctx.WriteAndFlush(out); err != nil {
		out.Release()
		return err
	}
	return nil
}
