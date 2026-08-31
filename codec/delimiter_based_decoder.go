package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

// DelimiterBasedFrameDecoder 使用一个或多个分隔符切分 TCP 字节流。
// 当多个分隔符同时匹配时，选择产生最短帧的分隔符，行为对齐 Netty。
type DelimiterBasedFrameDecoder struct {
	*ByteToMessageDecoder

	maxFrameLength int
	stripDelimiter bool
	failFast       bool
	delimiters     [][]byte
	discarding     bool
	discarded      int
}

func NewDelimiterBasedFrameDecoder(maxFrameLength int, stripDelimiter bool, failFast bool, delimiters ...[]byte) (*DelimiterBasedFrameDecoder, error) {
	if maxFrameLength <= 0 {
		return nil, ErrInvalidFrameLength
	}
	if len(delimiters) == 0 {
		return nil, ErrInvalidDelimiter
	}
	copied := make([][]byte, len(delimiters))
	for i := range delimiters {
		if len(delimiters[i]) == 0 {
			return nil, ErrInvalidDelimiter
		}
		copied[i] = append([]byte(nil), delimiters[i]...)
	}
	d := &DelimiterBasedFrameDecoder{
		maxFrameLength: maxFrameLength,
		stripDelimiter: stripDelimiter,
		failFast:       failFast,
		delimiters:     copied,
	}
	d.ByteToMessageDecoder = NewByteToMessageDecoder(d)
	return d, nil
}

func (d *DelimiterBasedFrameDecoder) Decode(_ *channel.HandlerContext, in *buffer.CompositeByteBuf) (any, error) {
	frameLength, delimLength := d.findDelimiter(in)
	if frameLength >= 0 {
		return d.decodeDelimitedFrame(in, frameLength, delimLength)
	}
	readable := in.ReadableBytes()
	if readable <= d.maxFrameLength {
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

func (d *DelimiterBasedFrameDecoder) decodeDelimitedFrame(in *buffer.CompositeByteBuf, frameLength int, delimLength int) (buffer.ByteBuf, error) {
	totalLength := frameLength + delimLength
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
	if frameLength > d.maxFrameLength {
		if err := in.SkipBytes(totalLength); err != nil {
			return nil, err
		}
		return nil, ErrFrameTooLong
	}
	sliceLength := frameLength
	if !d.stripDelimiter {
		sliceLength = totalLength
	}
	frame, err := in.Slice(in.ReaderIndex(), sliceLength)
	if err != nil {
		return nil, err
	}
	if err := in.SkipBytes(totalLength); err != nil {
		frame.Release()
		return nil, err
	}
	return frame, nil
}

func (d *DelimiterBasedFrameDecoder) findDelimiter(in *buffer.CompositeByteBuf) (int, int) {
	bestFrameLength := -1
	bestDelimiterLength := 0
	for _, delimiter := range d.delimiters {
		frameLength := indexOfDelimiter(in, delimiter)
		if frameLength < 0 {
			continue
		}
		if bestFrameLength < 0 || frameLength < bestFrameLength {
			bestFrameLength = frameLength
			bestDelimiterLength = len(delimiter)
		}
	}
	return bestFrameLength, bestDelimiterLength
}

func indexOfDelimiter(in *buffer.CompositeByteBuf, delimiter []byte) int {
	readerIndex := in.ReaderIndex()
	index, ok := in.Index(readerIndex, delimiter)
	if !ok {
		return -1
	}
	return index - readerIndex
}

func delimiterMatches(in *buffer.CompositeByteBuf, index int, delimiter []byte) bool {
	for i := range delimiter {
		b, ok := in.GetByte(index + i)
		if !ok || b != delimiter[i] {
			return false
		}
	}
	return true
}

func LineDelimiters() [][]byte {
	return [][]byte{{'\r', '\n'}, {'\n'}}
}

func NulDelimiter() [][]byte {
	return [][]byte{{0}}
}
