package buffer

import "errors"

var (
	ErrReleasedBuffer      = errors.New("gnalloy/buffer: buffer already released")
	ErrInvalidIndex        = errors.New("gnalloy/buffer: invalid index")
	ErrNotEnoughBytes      = errors.New("gnalloy/buffer: not enough readable bytes")
	ErrNoWritableBytes     = errors.New("gnalloy/buffer: no writable bytes")
	ErrInvalidSize         = errors.New("gnalloy/buffer: invalid buffer size")
	ErrAllocatorClosed     = errors.New("gnalloy/buffer: allocator closed")
	ErrAllocatorExhausted  = errors.New("gnalloy/buffer: allocator exhausted")
	ErrAllocatorInUse      = errors.New("gnalloy/buffer: allocator has in-use buffers")
	ErrUnsupportedMmap     = errors.New("gnalloy/buffer: mmap allocator is unsupported on this platform")
	ErrCompositeEmpty      = errors.New("gnalloy/buffer: composite buffer is empty")
	ErrWriterIndexOverflow = errors.New("gnalloy/buffer: writer index overflow")
)
