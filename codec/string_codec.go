package codec

import (
	"unsafe"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// StringDecoder 将入站 ByteBuf 转为 Go string。转换会复制 payload，这是 string 不可变语义决定的。
type StringDecoder struct{}

func NewStringDecoder() *StringDecoder {
	return &StringDecoder{}
}

func (d *StringDecoder) ChannelRead(ctx *channel.HandlerContext, msg any) {
	in, ok := msg.(buffer.ByteBuf)
	if !ok {
		ctx.FireChannelRead(msg)
		return
	}
	s := buffer.ReadableString(in)
	in.Release()
	ctx.FireChannelRead(s)
}

// StringEncoder 将出站 string 编码为 ByteBuf。
type StringEncoder struct{}

func NewStringEncoder() *StringEncoder {
	return &StringEncoder{}
}

func (e *StringEncoder) Write(ctx *channel.HandlerContext, msg any) error {
	s, ok := msg.(string)
	if !ok {
		return ctx.Write(msg)
	}
	if len(s) == 0 {
		return nil
	}
	out, err := ctx.Channel().Allocator().Acquire(len(s))
	if err != nil {
		return err
	}
	if _, err := out.WriteBytes(readOnlyStringBytes(s)); err != nil {
		out.Release()
		return err
	}
	return WriteOutboundBuffer(ctx, out)
}

func readOnlyStringBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
