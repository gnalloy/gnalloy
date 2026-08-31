package buffer

// ReadableSlices 返回当前可读区域的底层切片视图。
func (c *CompositeByteBuf) ReadableSlices(dst [][]byte) [][]byte {
	if c.refs.Load() <= 0 || c.readerIndex == c.writerIndex {
		return dst
	}
	if len(c.components) == 1 {
		comp := &c.components[0]
		from := max(c.readerIndex, comp.start)
		to := min(c.writerIndex, comp.end)
		if from >= to {
			return dst
		}
		if from == comp.start && to == comp.end {
			return comp.buf.ReadableSlices(dst)
		}
		return appendPartialReadableSlice(dst, comp, from, to)
	}
	for i := range c.components {
		comp := &c.components[i]
		from := max(c.readerIndex, comp.start)
		to := min(c.writerIndex, comp.end)
		if from >= to {
			continue
		}
		if from == comp.start && to == comp.end {
			dst = comp.buf.ReadableSlices(dst)
		} else {
			dst = appendPartialReadableSlice(dst, comp, from, to)
		}
	}
	return dst
}

// ReadableSpan 返回指定可读区间的连续底层视图；跨组件时返回 false。
func (c *CompositeByteBuf) ReadableSpan(index int, length int) ([]byte, bool) {
	if c.refs.Load() <= 0 || length < 0 || index < c.readerIndex || index > c.writerIndex-length {
		return nil, false
	}
	return c.readableSpan(index, length)
}

func appendPartialReadableSlice(dst [][]byte, comp *component, from int, to int) [][]byte {
	if data, ok := componentBytes(comp.buf); ok {
		offset := from - comp.start
		length := to - from
		if offset >= 0 && length > 0 && offset+length <= len(data) {
			return append(dst, data[offset:offset+length])
		}
	}
	part, err := comp.buf.Slice(comp.buf.ReaderIndex()+from-comp.start, to-from)
	if err != nil {
		return dst
	}
	dst = part.ReadableSlices(dst)
	part.Release()
	return dst
}
