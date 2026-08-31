package buffer

// ForEachReadableSlice 逐段访问当前可读区，不推进 readerIndex。
//
// 内置 ByteBuf 类型直接遍历底层连续片段，避免热路径先构造 [][]byte。
// fn 返回 false 时立即停止遍历。
func ForEachReadableSlice(src ByteBuf, fn func([]byte) bool) bool {
	if src == nil || fn == nil || src.ReadableBytes() == 0 {
		return true
	}
	switch b := src.(type) {
	case *DirectByteBuf:
		if b.refs.Load() <= 0 {
			return false
		}
		return fn(b.data[b.readerIndex:b.writerIndex])
	case *slicedByteBuf:
		if b.refs.Load() <= 0 {
			return false
		}
		return fn(b.data[b.readerIndex:b.writerIndex])
	case *sharedByteBuf:
		if b.refs.Load() <= 0 {
			return false
		}
		return fn(b.data[b.readerIndex:b.writerIndex])
	case *CompositeByteBuf:
		if b.refs.Load() <= 0 {
			return false
		}
		return b.forEachReadableSlice(fn)
	default:
		var stack [8][]byte
		for _, part := range src.ReadableSlices(stack[:0]) {
			if len(part) > 0 && !fn(part) {
				return false
			}
		}
		return true
	}
}

func (c *CompositeByteBuf) forEachReadableSlice(fn func([]byte) bool) bool {
	for i := c.findFirstReadableComponent(); i >= 0 && i < len(c.components); i++ {
		comp := &c.components[i]
		from := max(c.readerIndex, comp.start)
		to := min(c.writerIndex, comp.end)
		if to <= from {
			continue
		}
		if data, ok := componentBytes(comp.buf); ok {
			offset := from - comp.start
			if !fn(data[offset : offset+to-from]) {
				return false
			}
			continue
		}
		part, err := comp.buf.Slice(comp.buf.ReaderIndex()+from-comp.start, to-from)
		if err != nil {
			return false
		}
		ok := ForEachReadableSlice(part, fn)
		part.Release()
		if !ok {
			return false
		}
	}
	return true
}
