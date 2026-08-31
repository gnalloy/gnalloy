package codec

import (
	"gnalloy.org/gnalloy/channel"
)

// MessageEncoder 是 MessageToMessageEncoder 的策略接口。
type MessageEncoder interface {
	AcceptOutboundMessage(msg any) bool
	Encode(ctx *channel.HandlerContext, msg any, out *MessageList) error
}

// MessageToMessageEncoder 对齐 Netty 的消息到消息出站转换模板。
type MessageToMessageEncoder struct {
	encoder MessageEncoder
	out     MessageList
}

func NewMessageToMessageEncoder(encoder MessageEncoder) *MessageToMessageEncoder {
	return &MessageToMessageEncoder{encoder: encoder}
}

func (e *MessageToMessageEncoder) Write(ctx *channel.HandlerContext, msg any) error {
	if e.encoder == nil {
		releaseMessage(msg)
		return ErrInvalidEncoder
	}
	if !e.encoder.AcceptOutboundMessage(msg) {
		return ctx.Write(msg)
	}
	e.out.Reset()
	err := e.encoder.Encode(ctx, msg, &e.out)
	releaseMessage(msg)
	if err != nil {
		e.out.ReleaseAll()
		return err
	}
	for i := 0; i < e.out.Len(); i++ {
		if err := ctx.Write(e.out.At(i)); err != nil {
			for j := i; j < e.out.Len(); j++ {
				releaseMessage(e.out.At(j))
			}
			e.out.Reset()
			return err
		}
	}
	e.out.Reset()
	return nil
}

type messageEncoderFunc struct {
	accept func(any) bool
	encode func(*channel.HandlerContext, any, *MessageList) error
}

func NewMessageToMessageEncoderFunc(accept func(any) bool, encode func(*channel.HandlerContext, any, *MessageList) error) *MessageToMessageEncoder {
	return NewMessageToMessageEncoder(messageEncoderFunc{accept: accept, encode: encode})
}

func (f messageEncoderFunc) AcceptOutboundMessage(msg any) bool {
	return f.accept == nil || f.accept(msg)
}

func (f messageEncoderFunc) Encode(ctx *channel.HandlerContext, msg any, out *MessageList) error {
	return f.encode(ctx, msg, out)
}
