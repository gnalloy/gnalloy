package buffer

import (
	"bytes"
	"io"
	"sync/atomic"
)

type component struct {
	buf   ByteBuf
	start int
	end   int
}

// CompositeByteBuf 通过多个 ByteBuf 组成逻辑连续视图，不复制底层字节。
type CompositeByteBuf struct {
	components  []component
	readerIndex int
	writerIndex int
	refs        atomic.Int32
	leakID      uint64
}

func NewCompositeByteBuf() *CompositeByteBuf {
	c := &CompositeByteBuf{}
	c.refs.Store(1)
	c.leakID = trackLeak("composite")
	return c
}

func (c *CompositeByteBuf) checkAlive() error {
	if c.refs.Load() <= 0 {
		return ErrReleasedBuffer
	}
	return nil
}

func (c *CompositeByteBuf) Append(buf ByteBuf) {
	if err := c.checkAlive(); err != nil {
		if buf != nil {
			buf.Release()
		}
		return
	}
	if buf == nil || buf.ReadableBytes() == 0 {
		if buf != nil {
			buf.Release()
		}
		return
	}
	start := c.writerIndex
	c.writerIndex += buf.ReadableBytes()
	c.components = append(c.components, component{buf: buf, start: start, end: c.writerIndex})
}

// AppendRetained 保留调用方 ByteBuf 的所有权，并把 Retain 后的引用追加进 Composite。
func (c *CompositeByteBuf) AppendRetained(buf ByteBuf) {
	if buf == nil {
		return
	}
	c.Append(buf.Retain())
}

func (c *CompositeByteBuf) ComponentCount() int {
	return len(c.components)
}

func (c *CompositeByteBuf) IsContiguous() bool {
	return len(c.components) <= 1
}

func (c *CompositeByteBuf) ReadableBytes() int { return c.writerIndex - c.readerIndex }
func (c *CompositeByteBuf) WritableBytes() int { return 0 }
func (c *CompositeByteBuf) Capacity() int      { return c.writerIndex }
func (c *CompositeByteBuf) ReaderIndex() int   { return c.readerIndex }
func (c *CompositeByteBuf) WriterIndex() int   { return c.writerIndex }

func (c *CompositeByteBuf) SetReaderIndex(index int) error {
	if err := c.checkAlive(); err != nil {
		return err
	}
	if index < 0 || index > c.writerIndex {
		return ErrInvalidIndex
	}
	c.readerIndex = index
	return nil
}

func (c *CompositeByteBuf) SetWriterIndex(index int) error {
	if err := c.checkAlive(); err != nil {
		return err
	}
	if index < c.readerIndex || index > c.writerIndex {
		return ErrInvalidIndex
	}
	c.writerIndex = index
	return nil
}

func (c *CompositeByteBuf) Clear() {
	for _, comp := range c.components {
		comp.buf.Release()
	}
	c.components = nil
	c.readerIndex = 0
	c.writerIndex = 0
}

func (c *CompositeByteBuf) GetByte(index int) (byte, bool) {
	if c.refs.Load() <= 0 || index < c.readerIndex || index >= c.writerIndex {
		return 0, false
	}
	if len(c.components) == 1 {
		comp := &c.components[0]
		if index >= comp.start && index < comp.end {
			return comp.buf.GetByte(comp.buf.ReaderIndex() + index - comp.start)
		}
		return 0, false
	}
	comp := c.findComponent(index)
	if comp == nil {
		return 0, false
	}
	return comp.buf.GetByte(comp.buf.ReaderIndex() + index - comp.start)
}

func (c *CompositeByteBuf) ReadByte() (byte, error) {
	if err := c.checkAlive(); err != nil {
		return 0, err
	}
	b, ok := c.GetByte(c.readerIndex)
	if !ok {
		return 0, ErrNotEnoughBytes
	}
	c.readerIndex++
	return b, nil
}

func (c *CompositeByteBuf) ReadUnsigned(offset int, length int, order ByteOrder) (uint64, error) {
	if err := c.checkAlive(); err != nil {
		return 0, err
	}
	if length <= 0 || length > 8 || offset < c.readerIndex || offset+length > c.writerIndex {
		return 0, ErrInvalidIndex
	}
	if data, ok := c.readableSpan(offset, length); ok {
		return readUnsignedFrom(data, order), nil
	}
	var v uint64
	if order == LittleEndian {
		for i := length - 1; i >= 0; i-- {
			b, ok := c.GetByte(offset + i)
			if !ok {
				return 0, ErrNotEnoughBytes
			}
			v = (v << 8) | uint64(b)
		}
		return v, nil
	}
	for i := 0; i < length; i++ {
		b, ok := c.GetByte(offset + i)
		if !ok {
			return 0, ErrNotEnoughBytes
		}
		v = (v << 8) | uint64(b)
	}
	return v, nil
}

func (c *CompositeByteBuf) Read(p []byte) (int, error) {
	if err := c.checkAlive(); err != nil {
		return 0, err
	}
	if c.ReadableBytes() == 0 {
		return 0, io.EOF
	}
	n := c.copyReadableTo(p)
	c.readerIndex += n
	return n, nil
}

func (c *CompositeByteBuf) Write([]byte) (int, error) {
	return 0, ErrNoWritableBytes
}

func (c *CompositeByteBuf) WriteBytes([]byte) (int, error) {
	return 0, ErrNoWritableBytes
}

func (c *CompositeByteBuf) WriteTo(w io.Writer) (int64, error) {
	if err := c.checkAlive(); err != nil {
		return 0, err
	}
	total := int64(0)
	for c.readerIndex < c.writerIndex {
		comp := c.findComponent(c.readerIndex)
		if comp == nil {
			break
		}
		offset := comp.buf.ReaderIndex() + c.readerIndex - comp.start
		length := comp.end - c.readerIndex
		part, err := comp.buf.Slice(offset, length)
		if err != nil {
			return total, err
		}
		n, err := part.WriteTo(w)
		part.Release()
		c.readerIndex += int(n)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (c *CompositeByteBuf) ReadFrom(io.Reader) (int64, error) {
	return 0, ErrNoWritableBytes
}

func (c *CompositeByteBuf) Bytes() []byte {
	out, err := c.Copy()
	if err != nil {
		return nil
	}
	defer out.Release()
	return append([]byte(nil), out.Bytes()...)
}

func (c *CompositeByteBuf) WritableBytesView() []byte {
	return nil
}

func (c *CompositeByteBuf) AdvanceWriter(int) error {
	return ErrNoWritableBytes
}

func (c *CompositeByteBuf) SkipBytes(n int) error {
	if err := c.checkAlive(); err != nil {
		return err
	}
	if n < 0 || n > c.ReadableBytes() {
		return ErrNotEnoughBytes
	}
	c.readerIndex += n
	return nil
}

func (c *CompositeByteBuf) Slice(index int, length int) (ByteBuf, error) {
	if err := c.checkAlive(); err != nil {
		return nil, err
	}
	if index < c.readerIndex || length < 0 || index+length > c.writerIndex {
		return nil, ErrInvalidIndex
	}
	if length == 0 {
		return NewHeapBuffer(0), nil
	}
	if comp := c.singleComponent(index, index+length); comp != nil {
		return comp.buf.Slice(comp.buf.ReaderIndex()+index-comp.start, length)
	}
	out := NewCompositeByteBuf()
	end := index + length
	for i := range c.components {
		comp := &c.components[i]
		if comp.end <= index || comp.start >= end {
			continue
		}
		from := max(index, comp.start)
		to := min(end, comp.end)
		part, err := comp.buf.Slice(comp.buf.ReaderIndex()+from-comp.start, to-from)
		if err != nil {
			out.Release()
			return nil, err
		}
		out.Append(part)
	}
	return out, nil
}

func (c *CompositeByteBuf) Copy() (ByteBuf, error) {
	if err := c.checkAlive(); err != nil {
		return nil, err
	}
	out := NewHeapBuffer(c.ReadableBytes())
	if err := c.writeReadableTo(out); err != nil {
		out.Release()
		return nil, err
	}
	return out, nil
}

func (c *CompositeByteBuf) Retain() ByteBuf {
	for {
		refs := c.refs.Load()
		if refs <= 0 {
			panic(ErrReleasedBuffer)
		}
		if c.refs.CompareAndSwap(refs, refs+1) {
			return c
		}
	}
}

func (c *CompositeByteBuf) Release() bool {
	refs := c.refs.Add(-1)
	if refs > 0 {
		return false
	}
	if refs < 0 {
		panic(ErrReleasedBuffer)
	}
	untrackLeak(c.leakID)
	c.leakID = 0
	for _, comp := range c.components {
		comp.buf.Release()
	}
	c.components = nil
	return true
}

func (c *CompositeByteBuf) RefCnt() int32 { return c.refs.Load() }

func (c *CompositeByteBuf) DiscardReadComponents() {
	if c.readerIndex == 0 || len(c.components) == 0 {
		return
	}
	drop := 0
	for drop < len(c.components) && c.components[drop].end <= c.readerIndex {
		c.components[drop].buf.Release()
		c.components[drop] = component{}
		drop++
	}
	if drop == 0 {
		return
	}
	base := c.readerIndex
	if drop == len(c.components) {
		c.components = c.components[:0]
	} else {
		copy(c.components, c.components[drop:])
		keep := len(c.components) - drop
		clear(c.components[keep:])
		c.components = c.components[:keep]
	}
	for i := range c.components {
		c.components[i].start -= base
		c.components[i].end -= base
	}
	c.writerIndex -= base
	c.readerIndex = 0
}

func (c *CompositeByteBuf) findComponent(index int) *component {
	if i := c.findComponentIndex(index); i >= 0 {
		return &c.components[i]
	}
	return nil
}

func (c *CompositeByteBuf) singleComponent(start int, end int) *component {
	if len(c.components) == 1 {
		comp := &c.components[0]
		if start >= comp.start && end <= comp.end {
			return comp
		}
		return nil
	}
	if i := c.findComponentIndex(start); i >= 0 {
		comp := &c.components[i]
		if start >= comp.start && end <= comp.end {
			return comp
		}
	}
	return nil
}

// IndexByte 返回可读区间内第一个匹配字节的绝对索引。
func (c *CompositeByteBuf) IndexByte(index int, value byte) (int, bool) {
	if c.refs.Load() <= 0 || index < c.readerIndex || index >= c.writerIndex {
		return 0, false
	}
	startComponent := c.findComponentIndex(index)
	if startComponent < 0 {
		return 0, false
	}
	for i := startComponent; i < len(c.components); i++ {
		comp := &c.components[i]
		from := max(index, comp.start)
		to := min(c.writerIndex, comp.end)
		if to <= from {
			continue
		}
		if data, ok := componentBytes(comp.buf); ok {
			offset := from - comp.start
			found := bytes.IndexByte(data[offset:offset+to-from], value)
			if found >= 0 {
				return from + found, true
			}
			continue
		}
		for pos := from; pos < to; pos++ {
			b, ok := comp.buf.GetByte(comp.buf.ReaderIndex() + pos - comp.start)
			if ok && b == value {
				return pos, true
			}
		}
	}
	return 0, false
}

// Index 返回可读区间内第一个匹配字节序列的绝对索引。
func (c *CompositeByteBuf) Index(index int, value []byte) (int, bool) {
	if len(value) == 0 || c.refs.Load() <= 0 || index < c.readerIndex {
		return 0, false
	}
	limit := c.writerIndex - len(value)
	if index > limit {
		return 0, false
	}
	if len(value) == 1 {
		return c.IndexByte(index, value[0])
	}
	for index <= limit {
		found, ok := c.IndexByte(index, value[0])
		if !ok || found > limit {
			return 0, false
		}
		if c.matchAt(found, value) {
			return found, true
		}
		index = found + 1
	}
	return 0, false
}

func (c *CompositeByteBuf) matchAt(index int, value []byte) bool {
	if data, ok := c.readableSpan(index, len(value)); ok {
		return bytes.Equal(data, value)
	}
	for i := range value {
		b, ok := c.GetByte(index + i)
		if !ok || b != value[i] {
			return false
		}
	}
	return true
}

func (c *CompositeByteBuf) readableSpan(index int, length int) ([]byte, bool) {
	if length < 0 {
		return nil, false
	}
	comp := c.findComponent(index)
	if comp == nil || index > comp.end-length {
		return nil, false
	}
	data, ok := componentBytes(comp.buf)
	if !ok {
		return nil, false
	}
	offset := index - comp.start
	if offset < 0 || offset+length > len(data) {
		return nil, false
	}
	return data[offset : offset+length], true
}

func (c *CompositeByteBuf) copyReadableTo(dst []byte) int {
	if len(dst) == 0 {
		return 0
	}
	written := 0
	for i := c.findFirstReadableComponent(); i >= 0 && i < len(c.components) && written < len(dst); i++ {
		comp := &c.components[i]
		from := max(c.readerIndex, comp.start)
		to := min(c.writerIndex, comp.end)
		if to <= from {
			continue
		}
		if data, ok := componentBytes(comp.buf); ok {
			offset := from - comp.start
			written += copy(dst[written:], data[offset:offset+to-from])
			continue
		}
		for pos := from; pos < to && written < len(dst); pos++ {
			b, ok := comp.buf.GetByte(comp.buf.ReaderIndex() + pos - comp.start)
			if !ok {
				return written
			}
			dst[written] = b
			written++
		}
	}
	return written
}

func (c *CompositeByteBuf) writeReadableTo(dst ByteBuf) error {
	var one [1]byte
	for i := c.findFirstReadableComponent(); i >= 0 && i < len(c.components); i++ {
		comp := &c.components[i]
		from := max(c.readerIndex, comp.start)
		to := min(c.writerIndex, comp.end)
		if to <= from {
			continue
		}
		if data, ok := componentBytes(comp.buf); ok {
			offset := from - comp.start
			if _, err := dst.WriteBytes(data[offset : offset+to-from]); err != nil {
				return err
			}
			continue
		}
		for pos := from; pos < to; pos++ {
			b, ok := comp.buf.GetByte(comp.buf.ReaderIndex() + pos - comp.start)
			if !ok {
				return ErrInvalidIndex
			}
			one[0] = b
			if _, err := dst.WriteBytes(one[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CompositeByteBuf) findFirstReadableComponent() int {
	if len(c.components) == 0 || c.readerIndex >= c.writerIndex {
		return -1
	}
	if i := c.findComponentIndex(c.readerIndex); i >= 0 {
		return i
	}
	return 0
}

func (c *CompositeByteBuf) findComponentIndex(index int) int {
	n := len(c.components)
	if n == 0 {
		return -1
	}
	if n == 1 {
		if componentContains(c.components[0], index) {
			return 0
		}
		return -1
	}
	if n == 2 {
		for i := 0; i < 2; i++ {
			if componentContains(c.components[i], index) {
				return i
			}
		}
		return -1
	}
	low, high := 0, n
	for low < high {
		mid := low + (high-low)/2
		comp := c.components[mid]
		if index < comp.start {
			high = mid
			continue
		}
		if index >= comp.end {
			low = mid + 1
			continue
		}
		return mid
	}
	return -1
}

func componentContains(comp component, index int) bool {
	return index >= comp.start && index < comp.end
}

func componentBytes(buf ByteBuf) ([]byte, bool) {
	switch b := buf.(type) {
	case *DirectByteBuf:
		return b.Bytes(), true
	case *slicedByteBuf:
		return b.Bytes(), true
	default:
		return nil, false
	}
}
