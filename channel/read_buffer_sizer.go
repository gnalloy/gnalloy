package channel

import "gnalloy.org/gnalloy/buffer"

const (
	defaultMinReadBufferSize     = 512
	defaultInitialReadBufferSize = 512
)

type readBufferSizer struct {
	min  int
	next int
	max  int
}

func newReadBufferSizer(maxSize int) readBufferSizer {
	if maxSize <= 0 {
		maxSize = defaultReadBufferSize
	}
	minSize := defaultMinReadBufferSize
	if maxSize < minSize {
		minSize = maxSize
	}
	nextSize := defaultInitialReadBufferSize
	if nextSize < minSize {
		nextSize = minSize
	}
	if nextSize > maxSize {
		nextSize = maxSize
	}
	return readBufferSizer{min: minSize, next: nextSize, max: maxSize}
}

func (s *readBufferSizer) reset(maxSize int) {
	*s = newReadBufferSizer(maxSize)
}

func (s *readBufferSizer) nextSize() int {
	if s.next <= 0 {
		s.reset(defaultReadBufferSize)
	}
	return s.next
}

func (s *readBufferSizer) record(actual int, attempted int) {
	if actual <= 0 || attempted <= 0 {
		return
	}
	if actual >= attempted {
		s.grow(attempted)
		return
	}
	if actual <= attempted/2 {
		s.shrink(attempted)
	}
}

func (s *readBufferSizer) grow(attempted int) {
	if attempted >= s.max {
		return
	}
	next := attempted << 1
	if next < attempted {
		next = s.max
	}
	if next > s.max {
		next = s.max
	}
	if next > s.next {
		s.next = next
	}
}

func (s *readBufferSizer) shrink(attempted int) {
	if attempted <= s.min {
		return
	}
	next := attempted >> 1
	if next < s.min {
		next = s.min
	}
	if next < s.next {
		s.next = next
	}
}

func readAttemptedSize(buf buffer.ByteBuf) int {
	if buf == nil {
		return 0
	}
	return buf.WriterIndex() + buf.WritableBytes()
}
