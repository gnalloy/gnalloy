package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// FixedLengthFrameDecoder 按固定字节数从 TCP 字节流中切帧。
type FixedLengthFrameDecoder struct {
	*ByteToMessageDecoder

	frameLength int
}

func NewFixedLengthFrameDecoder(frameLength int) (*FixedLengthFrameDecoder, error) {
	if frameLength <= 0 {
		return nil, ErrInvalidFrameLength
	}
	d := &FixedLengthFrameDecoder{frameLength: frameLength}
	d.ByteToMessageDecoder = NewByteToMessageDecoder(d)
	return d, nil
}

func (d *FixedLengthFrameDecoder) Decode(_ *channel.HandlerContext, in *buffer.CompositeByteBuf) (any, error) {
	if in.ReadableBytes() < d.frameLength {
		return nil, nil
	}
	frame, err := in.Slice(in.ReaderIndex(), d.frameLength)
	if err != nil {
		return nil, err
	}
	if err := in.SkipBytes(d.frameLength); err != nil {
		frame.Release()
		return nil, err
	}
	return frame, nil
}
