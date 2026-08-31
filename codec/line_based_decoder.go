package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// LineBasedFrameDecoder 按 \n 或 \r\n 切分文本行，默认剥离行分隔符。
type LineBasedFrameDecoder struct {
	*ByteToMessageDecoder

	maxLength      int
	stripDelimiter bool
	failFast       bool
	discarding     bool
	discarded      int
}

func NewLineBasedFrameDecoder(maxLength int) (*LineBasedFrameDecoder, error) {
	return NewLineBasedFrameDecoderWithOptions(maxLength, true, true)
}

func NewLineBasedFrameDecoderWithOptions(maxLength int, stripDelimiter bool, failFast bool) (*LineBasedFrameDecoder, error) {
	if maxLength <= 0 {
		return nil, ErrInvalidFrameLength
	}
	d := &LineBasedFrameDecoder{
		maxLength:      maxLength,
		stripDelimiter: stripDelimiter,
		failFast:       failFast,
	}
	d.ByteToMessageDecoder = NewByteToMessageDecoder(d)
	return d, nil
}

func (d *LineBasedFrameDecoder) Decode(_ *channel.HandlerContext, in *buffer.CompositeByteBuf) (any, error) {
	lineEnd, delimLength := findLineEnd(in)
	if lineEnd >= 0 {
		return d.decodeLine(in, lineEnd, delimLength)
	}
	readable := in.ReadableBytes()
	if readable <= d.maxLength {
		return nil, nil
	}
	d.discarding = true
	d.discarded += readable
	if err := in.SkipBytes(readable); err != nil {
		return nil, err
	}
	if d.failFast {
		return nil, ErrFrameTooLong
	}
	return nil, nil
}

func (d *LineBasedFrameDecoder) decodeLine(in *buffer.CompositeByteBuf, lineEnd int, delimLength int) (buffer.ByteBuf, error) {
	readerIndex := in.ReaderIndex()
	totalLength := lineEnd - readerIndex + 1
	frameLength := totalLength - delimLength
	if d.discarding {
		d.discarding = false
		d.discarded = 0
		if err := in.SkipBytes(totalLength); err != nil {
			return nil, err
		}
		if !d.failFast {
			return nil, ErrFrameTooLong
		}
		return nil, nil
	}
	if frameLength > d.maxLength {
		if err := in.SkipBytes(totalLength); err != nil {
			return nil, err
		}
		return nil, ErrFrameTooLong
	}

	sliceLength := frameLength
	if !d.stripDelimiter {
		sliceLength = totalLength
	}
	frame, err := in.Slice(readerIndex, sliceLength)
	if err != nil {
		return nil, err
	}
	if err := in.SkipBytes(totalLength); err != nil {
		frame.Release()
		return nil, err
	}
	return frame, nil
}

func findLineEnd(in *buffer.CompositeByteBuf) (int, int) {
	readerIndex := in.ReaderIndex()
	lineEnd, ok := in.IndexByte(readerIndex, '\n')
	if !ok {
		return -1, 0
	}
	if lineEnd > readerIndex {
		prev, _ := in.GetByte(lineEnd - 1)
		if prev == '\r' {
			return lineEnd, 2
		}
	}
	return lineEnd, 1
}
