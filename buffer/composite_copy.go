package buffer

func (c *CompositeByteBuf) copyReadableRange(dst []byte, index int, length int) (int, error) {
	if len(dst) < length {
		return 0, ErrNoWritableBytes
	}
	if err := c.checkAlive(); err != nil {
		return 0, err
	}
	if length < 0 || index < c.readerIndex || index > c.writerIndex-length {
		return 0, ErrInvalidIndex
	}
	if length == 0 {
		return 0, nil
	}
	componentIndex := c.findComponentIndex(index)
	if componentIndex < 0 {
		return 0, ErrInvalidIndex
	}
	end := index + length
	written := 0
	for i := componentIndex; i < len(c.components) && index < end; i++ {
		comp := &c.components[i]
		from := max(index, comp.start)
		to := min(end, comp.end)
		if to <= from {
			continue
		}
		if data, ok := componentBytes(comp.buf); ok {
			offset := from - comp.start
			written += copy(dst[written:], data[offset:offset+to-from])
			index = to
			continue
		}
		for pos := from; pos < to; pos++ {
			b, ok := comp.buf.GetByte(comp.buf.ReaderIndex() + pos - comp.start)
			if !ok {
				return written, ErrNotEnoughBytes
			}
			dst[written] = b
			written++
		}
		index = to
	}
	if written != length {
		return written, ErrNotEnoughBytes
	}
	return written, nil
}
