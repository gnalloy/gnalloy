package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type LineSeparator struct {
	bytes []byte
}

var (
	LineSeparatorUnix    = LineSeparator{bytes: []byte{'\n'}}
	LineSeparatorWindows = LineSeparator{bytes: []byte{'\r', '\n'}}
)

func NewLineSeparator(delimiter []byte) (LineSeparator, error) {
	if len(delimiter) == 0 {
		return LineSeparator{}, ErrInvalidDelimiter
	}
	return LineSeparator{bytes: append([]byte(nil), delimiter...)}, nil
}

func (s LineSeparator) Bytes() []byte {
	return append([]byte(nil), s.bytes...)
}

type LineEncoder struct {
	separator []byte
}

func NewLineEncoder(separator LineSeparator) *LineEncoder {
	if len(separator.bytes) == 0 {
		separator = LineSeparatorUnix
	}
	return &LineEncoder{separator: append([]byte(nil), separator.bytes...)}
}

func (e *LineEncoder) Write(ctx *channel.HandlerContext, msg any) error {
	return writeDelimited(ctx, msg, e.separator, true)
}

type DelimiterBasedFrameEncoder struct {
	delimiter []byte
}

func NewDelimiterBasedFrameEncoder(delimiter []byte) (*DelimiterBasedFrameEncoder, error) {
	if len(delimiter) == 0 {
		return nil, ErrInvalidDelimiter
	}
	return &DelimiterBasedFrameEncoder{delimiter: append([]byte(nil), delimiter...)}, nil
}

func (e *DelimiterBasedFrameEncoder) Write(ctx *channel.HandlerContext, msg any) error {
	return writeDelimited(ctx, msg, e.delimiter, true)
}

type FixedLengthFrameEncoder struct {
	frameLength int
}

func NewFixedLengthFrameEncoder(frameLength int) (*FixedLengthFrameEncoder, error) {
	if frameLength <= 0 {
		return nil, ErrInvalidFrameLength
	}
	return &FixedLengthFrameEncoder{frameLength: frameLength}, nil
}

func (e *FixedLengthFrameEncoder) Write(ctx *channel.HandlerContext, msg any) error {
	switch in := msg.(type) {
	case buffer.ByteBuf:
		if in.ReadableBytes() != e.frameLength {
			in.Release()
			return ErrInvalidFrameLength
		}
		return WriteOutboundBuffer(ctx, in)
	case []byte:
		if len(in) != e.frameLength {
			return ErrInvalidFrameLength
		}
		return writeBytes(ctx, in)
	case string:
		if len(in) != e.frameLength {
			return ErrInvalidFrameLength
		}
		return writeBytes(ctx, readOnlyStringBytes(in))
	default:
		return ctx.Write(msg)
	}
}

func writeDelimited(ctx *channel.HandlerContext, msg any, delimiter []byte, passByteBuf bool) error {
	if len(delimiter) == 0 {
		return ctx.Write(msg)
	}
	switch in := msg.(type) {
	case buffer.ByteBuf:
		if !passByteBuf {
			return writeJoined(ctx, in.Bytes(), delimiter)
		}
		delim, err := ctx.Channel().Allocator().Acquire(len(delimiter))
		if err != nil {
			in.Release()
			return err
		}
		if _, err := delim.WriteBytes(delimiter); err != nil {
			delim.Release()
			in.Release()
			return err
		}
		if err := ctx.Write(in); err != nil {
			delim.Release()
			in.Release()
			return err
		}
		if err := ctx.Write(delim); err != nil {
			delim.Release()
			return err
		}
		return nil
	case []byte:
		return writeJoined(ctx, in, delimiter)
	case string:
		return writeJoined(ctx, readOnlyStringBytes(in), delimiter)
	default:
		return ctx.Write(msg)
	}
}

func writeJoined(ctx *channel.HandlerContext, payload []byte, suffix []byte) error {
	out, err := ctx.Channel().Allocator().Acquire(len(payload) + len(suffix))
	if err != nil {
		return err
	}
	if _, err := out.WriteBytes(payload); err != nil {
		out.Release()
		return err
	}
	if _, err := out.WriteBytes(suffix); err != nil {
		out.Release()
		return err
	}
	return WriteOutboundBuffer(ctx, out)
}

func writeBytes(ctx *channel.HandlerContext, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	out, err := ctx.Channel().Allocator().Acquire(len(payload))
	if err != nil {
		return err
	}
	if _, err := out.WriteBytes(payload); err != nil {
		out.Release()
		return err
	}
	return WriteOutboundBuffer(ctx, out)
}
