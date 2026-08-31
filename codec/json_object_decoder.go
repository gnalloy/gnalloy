package codec

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/channel"
)

type JsonObjectDecoder struct {
	*ByteToMessageDecoder
	maxObjectLength int
}

func NewJsonObjectDecoder(maxObjectLength int) (*JsonObjectDecoder, error) {
	if maxObjectLength <= 0 {
		return nil, ErrInvalidFrameLength
	}
	d := &JsonObjectDecoder{maxObjectLength: maxObjectLength}
	d.ByteToMessageDecoder = NewByteToMessageDecoder(d)
	return d, nil
}

func (d *JsonObjectDecoder) Decode(_ *channel.HandlerContext, in *buffer.CompositeByteBuf) (any, error) {
	reader := in.ReaderIndex()
	writer := in.WriterIndex()
	start := reader
	for start < writer {
		b, _ := in.GetByte(start)
		if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
			break
		}
		start++
	}
	if start > reader {
		if err := in.SkipBytes(start - reader); err != nil {
			return nil, err
		}
		return nil, nil
	}
	if start == writer {
		return nil, nil
	}

	first, _ := in.GetByte(start)
	if first != '{' && first != '[' {
		return nil, ErrInvalidFrameLength
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < writer; i++ {
		if i-start+1 > d.maxObjectLength {
			return nil, NewTooLongFrameError(i-start+1, d.maxObjectLength, 0)
		}
		b, _ := in.GetByte(i)
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if b == '\\' {
				escaped = true
				continue
			}
			if b == '"' {
				inString = false
			}
			continue
		}
		switch b {
		case '"':
			inString = true
		case '{', '[':
			depth++
		case '}', ']':
			depth--
			if depth < 0 {
				return nil, ErrInvalidFrameLength
			}
			if depth == 0 {
				length := i - start + 1
				frame, err := in.Slice(start, length)
				if err != nil {
					return nil, err
				}
				if err := in.SkipBytes(length); err != nil {
					frame.Release()
					return nil, err
				}
				return frame, nil
			}
		}
	}
	if writer-start > d.maxObjectLength {
		return nil, NewTooLongFrameError(writer-start, d.maxObjectLength, 0)
	}
	return nil, nil
}
