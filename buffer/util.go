package buffer

const hexdigits = "0123456789abcdef"

func HexDump(buf ByteBuf) string {
	if buf == nil || buf.ReadableBytes() == 0 {
		return ""
	}
	out := make([]byte, 0, buf.ReadableBytes()*2)
	for _, slice := range buf.ReadableSlices(nil) {
		for _, b := range slice {
			out = append(out, hexdigits[b>>4], hexdigits[b&0x0f])
		}
	}
	return string(out)
}

func HexDumpRange(buf ByteBuf, index int, length int) string {
	if buf == nil || length <= 0 {
		return ""
	}
	if index < buf.ReaderIndex() || index+length > buf.WriterIndex() {
		return ""
	}
	out := make([]byte, 0, length*2)
	for i := 0; i < length; i++ {
		b, ok := buf.GetByte(index + i)
		if !ok {
			return ""
		}
		out = append(out, hexdigits[b>>4], hexdigits[b&0x0f])
	}
	return string(out)
}

func ByteBufEqual(a ByteBuf, b ByteBuf) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.ReadableBytes() != b.ReadableBytes() {
		return false
	}
	for i := 0; i < a.ReadableBytes(); i++ {
		av, aok := a.GetByte(a.ReaderIndex() + i)
		bv, bok := b.GetByte(b.ReaderIndex() + i)
		if !aok || !bok || av != bv {
			return false
		}
	}
	return true
}

func ByteBufCompare(a ByteBuf, b ByteBuf) int {
	if a == nil || b == nil {
		switch {
		case a == b:
			return 0
		case a == nil:
			return -1
		default:
			return 1
		}
	}
	n := a.ReadableBytes()
	if b.ReadableBytes() < n {
		n = b.ReadableBytes()
	}
	for i := 0; i < n; i++ {
		av, _ := a.GetByte(a.ReaderIndex() + i)
		bv, _ := b.GetByte(b.ReaderIndex() + i)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
	}
	switch {
	case a.ReadableBytes() < b.ReadableBytes():
		return -1
	case a.ReadableBytes() > b.ReadableBytes():
		return 1
	default:
		return 0
	}
}

func ByteBufHashCode(buf ByteBuf) uint32 {
	if buf == nil {
		return 0
	}
	var hash uint32 = 1
	for i := 0; i < buf.ReadableBytes(); i++ {
		b, ok := buf.GetByte(buf.ReaderIndex() + i)
		if !ok {
			return hash
		}
		hash = 31*hash + uint32(b)
	}
	return hash
}

func IndexOfByte(buf ByteBuf, fromIndex int, toIndex int, value byte) int {
	if buf == nil {
		return -1
	}
	if fromIndex < buf.ReaderIndex() {
		fromIndex = buf.ReaderIndex()
	}
	if toIndex > buf.WriterIndex() {
		toIndex = buf.WriterIndex()
	}
	if fromIndex <= toIndex {
		for i := fromIndex; i < toIndex; i++ {
			b, ok := buf.GetByte(i)
			if ok && b == value {
				return i
			}
		}
		return -1
	}
	for i := fromIndex - 1; i >= toIndex; i-- {
		b, ok := buf.GetByte(i)
		if ok && b == value {
			return i
		}
	}
	return -1
}

func IndexOfBytes(buf ByteBuf, fromIndex int, toIndex int, needle []byte) int {
	if buf == nil || len(needle) == 0 {
		return -1
	}
	if fromIndex < buf.ReaderIndex() {
		fromIndex = buf.ReaderIndex()
	}
	if toIndex > buf.WriterIndex() {
		toIndex = buf.WriterIndex()
	}
	if fromIndex > toIndex || toIndex-fromIndex < len(needle) {
		return -1
	}
	maxStart := toIndex - len(needle)
	for i := fromIndex; i <= maxStart; i++ {
		if bytesMatch(buf, i, needle) {
			return i
		}
	}
	return -1
}

func BytesBefore(buf ByteBuf, fromIndex int, toIndex int, needle []byte) int {
	index := IndexOfBytes(buf, fromIndex, toIndex, needle)
	if index < 0 {
		return -1
	}
	return index - fromIndex
}

func bytesMatch(buf ByteBuf, index int, needle []byte) bool {
	for i := range needle {
		b, ok := buf.GetByte(index + i)
		if !ok || b != needle[i] {
			return false
		}
	}
	return true
}
