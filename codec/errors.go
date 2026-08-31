package codec

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidLengthField = errors.New("gnalloy/codec: invalid length field")
	ErrFrameTooLong       = errors.New("gnalloy/codec: frame too long")
	ErrInvalidFrameLength = errors.New("gnalloy/codec: invalid frame length")
	ErrInvalidDelimiter   = errors.New("gnalloy/codec: invalid delimiter")
	ErrInvalidDecoder     = errors.New("gnalloy/codec: invalid byte decoder")
	ErrInvalidEncoder     = errors.New("gnalloy/codec: invalid byte encoder")
	ErrDecoderNoProgress  = errors.New("gnalloy/codec: decoder emitted message without consuming bytes")
	ErrEncodedLengthRange = errors.New("gnalloy/codec: encoded length out of range")
	ErrReplayNeedMore     = errors.New("gnalloy/codec: replay decoder needs more bytes")
)

// TooLongFrameError 携带超长帧的上下文，同时兼容 errors.Is(err, ErrFrameTooLong)。
type TooLongFrameError struct {
	FrameLength    int
	MaxFrameLength int
	Discarded      int
}

func NewTooLongFrameError(frameLength int, maxFrameLength int, discarded int) TooLongFrameError {
	return TooLongFrameError{FrameLength: frameLength, MaxFrameLength: maxFrameLength, Discarded: discarded}
}

func (e TooLongFrameError) Error() string {
	return fmt.Sprintf("%v: frame=%d max=%d discarded=%d", ErrFrameTooLong, e.FrameLength, e.MaxFrameLength, e.Discarded)
}

func (e TooLongFrameError) Is(target error) bool {
	return target == ErrFrameTooLong
}
