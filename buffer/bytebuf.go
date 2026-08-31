package buffer

import (
	"io"
	"sync/atomic"
)

// ByteBuf 是 gnalloy 的基础字节缓冲区，采用读写指针分离和显式生命周期管理。
type ByteBuf interface {
	io.Reader
	io.Writer
	io.WriterTo
	io.ReaderFrom

	ReadableBytes() int
	WritableBytes() int
	Capacity() int
	ReaderIndex() int
	WriterIndex() int
	SetReaderIndex(index int) error
	SetWriterIndex(index int) error
	Clear()

	GetByte(index int) (byte, bool)
	ReadByte() (byte, error)
	ReadUnsigned(offset int, length int, order ByteOrder) (uint64, error)
	WriteBytes(src []byte) (int, error)
	Bytes() []byte
	ReadableSlices(dst [][]byte) [][]byte
	WritableBytesView() []byte
	AdvanceWriter(n int) error
	SkipBytes(n int) error
	Slice(index int, length int) (ByteBuf, error)
	Copy() (ByteBuf, error)

	Retain() ByteBuf
	Release() bool
	RefCnt() int32
}

// ByteOrder 描述长度字段等二进制整数的字节序。
type ByteOrder uint8

const (
	BigEndian ByteOrder = iota
	LittleEndian
)

type releaser interface {
	releaseDirect(buf *DirectByteBuf)
}

type fixedBufferOwner interface {
	fixedBufferIndex(buf *DirectByteBuf) (uint16, bool)
}

// DirectByteBuf 是连续内存 ByteBuf，底层可来自堆、slab 或 mmap arena。
type DirectByteBuf struct {
	data []byte

	readerIndex int
	writerIndex int

	refs atomic.Int32

	owner      releaser
	ownerIndex uint32
	leakID     uint64
}

func newDirectByteBuf(data []byte, owner releaser) *DirectByteBuf {
	b := &DirectByteBuf{data: data, owner: owner}
	b.refs.Store(1)
	b.leakID = trackLeak("direct")
	return b
}

func (b *DirectByteBuf) reset(data []byte, owner releaser) {
	b.data = data
	b.readerIndex = 0
	b.writerIndex = 0
	b.owner = owner
	b.refs.Store(1)
	b.leakID = trackLeak("direct")
}

func (b *DirectByteBuf) checkAlive() error {
	if b.refs.Load() <= 0 {
		return ErrReleasedBuffer
	}
	return nil
}

func (b *DirectByteBuf) ReadableBytes() int {
	return b.writerIndex - b.readerIndex
}

func (b *DirectByteBuf) WritableBytes() int {
	return len(b.data) - b.writerIndex
}

func (b *DirectByteBuf) Capacity() int {
	return len(b.data)
}

func (b *DirectByteBuf) ReaderIndex() int {
	return b.readerIndex
}

func (b *DirectByteBuf) WriterIndex() int {
	return b.writerIndex
}

func (b *DirectByteBuf) SetReaderIndex(index int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if index < 0 || index > b.writerIndex {
		return ErrInvalidIndex
	}
	b.readerIndex = index
	return nil
}

func (b *DirectByteBuf) SetWriterIndex(index int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if index < b.readerIndex || index > len(b.data) {
		return ErrInvalidIndex
	}
	b.writerIndex = index
	return nil
}

func (b *DirectByteBuf) Clear() {
	b.readerIndex = 0
	b.writerIndex = 0
}

func (b *DirectByteBuf) GetByte(index int) (byte, bool) {
	if b.refs.Load() <= 0 || index < b.readerIndex || index >= b.writerIndex {
		return 0, false
	}
	return b.data[index], true
}

func (b *DirectByteBuf) ReadByte() (byte, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if b.ReadableBytes() <= 0 {
		return 0, ErrNotEnoughBytes
	}
	v := b.data[b.readerIndex]
	b.readerIndex++
	return v, nil
}

func (b *DirectByteBuf) ReadUnsigned(offset int, length int, order ByteOrder) (uint64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if length <= 0 || length > 8 || offset < b.readerIndex || offset+length > b.writerIndex {
		return 0, ErrInvalidIndex
	}
	return readUnsignedFrom(b.data[offset:offset+length], order), nil
}

func (b *DirectByteBuf) Read(p []byte) (int, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if b.ReadableBytes() == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.readerIndex:b.writerIndex])
	b.readerIndex += n
	return n, nil
}

func (b *DirectByteBuf) Write(p []byte) (int, error) {
	return b.WriteBytes(p)
}

func (b *DirectByteBuf) WriteBytes(src []byte) (int, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	if len(src) > b.WritableBytes() {
		return 0, ErrNoWritableBytes
	}
	n := copy(b.data[b.writerIndex:], src)
	b.writerIndex += n
	return n, nil
}

func (b *DirectByteBuf) WriteTo(w io.Writer) (int64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	n, err := w.Write(b.data[b.readerIndex:b.writerIndex])
	b.readerIndex += n
	return int64(n), err
}

func (b *DirectByteBuf) ReadFrom(r io.Reader) (int64, error) {
	if err := b.checkAlive(); err != nil {
		return 0, err
	}
	total := int64(0)
	for b.WritableBytes() > 0 {
		n, err := r.Read(b.data[b.writerIndex:])
		if n > 0 {
			b.writerIndex += n
			total += int64(n)
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			return total, nil
		}
	}
	return total, nil
}

func (b *DirectByteBuf) Bytes() []byte {
	if b.refs.Load() <= 0 {
		return nil
	}
	return b.data[b.readerIndex:b.writerIndex]
}

func (b *DirectByteBuf) ReadableSlices(dst [][]byte) [][]byte {
	if b.refs.Load() <= 0 || b.readerIndex == b.writerIndex {
		return dst
	}
	return append(dst, b.data[b.readerIndex:b.writerIndex])
}

func (b *DirectByteBuf) WritableBytesView() []byte {
	if b.refs.Load() <= 0 {
		return nil
	}
	return b.data[b.writerIndex:]
}

func (b *DirectByteBuf) AdvanceWriter(n int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if n < 0 || n > b.WritableBytes() {
		return ErrInvalidIndex
	}
	b.writerIndex += n
	return nil
}

func (b *DirectByteBuf) SkipBytes(n int) error {
	if err := b.checkAlive(); err != nil {
		return err
	}
	if n < 0 || n > b.ReadableBytes() {
		return ErrNotEnoughBytes
	}
	b.readerIndex += n
	return nil
}

func (b *DirectByteBuf) Slice(index int, length int) (ByteBuf, error) {
	if err := b.checkAlive(); err != nil {
		return nil, err
	}
	if index < b.readerIndex || length < 0 || index+length > b.writerIndex {
		return nil, ErrInvalidIndex
	}
	b.Retain()
	return newSlicedByteBuf(b, b.data[index:index+length]), nil
}

func (b *DirectByteBuf) Copy() (ByteBuf, error) {
	if err := b.checkAlive(); err != nil {
		return nil, err
	}
	out := NewHeapBuffer(b.ReadableBytes())
	_, err := out.WriteBytes(b.Bytes())
	if err != nil {
		out.Release()
		return nil, err
	}
	return out, nil
}

func (b *DirectByteBuf) Retain() ByteBuf {
	for {
		refs := b.refs.Load()
		if refs <= 0 {
			panic(ErrReleasedBuffer)
		}
		if b.refs.CompareAndSwap(refs, refs+1) {
			return b
		}
	}
}

func (b *DirectByteBuf) Release() bool {
	refs := b.refs.Add(-1)
	if refs > 0 {
		return false
	}
	if refs < 0 {
		panic(ErrReleasedBuffer)
	}
	untrackLeak(b.leakID)
	b.leakID = 0
	if b.owner != nil {
		b.owner.releaseDirect(b)
	}
	return true
}

func (b *DirectByteBuf) RefCnt() int32 {
	return b.refs.Load()
}

func (b *DirectByteBuf) FixedBufferIndex() (uint16, bool) {
	if b.refs.Load() <= 0 {
		return 0, false
	}
	owner, ok := b.owner.(fixedBufferOwner)
	if !ok {
		return 0, false
	}
	return owner.fixedBufferIndex(b)
}

func readUnsignedFrom(src []byte, order ByteOrder) uint64 {
	var v uint64
	if order == LittleEndian {
		for i := len(src) - 1; i >= 0; i-- {
			v = (v << 8) | uint64(src[i])
		}
		return v
	}
	for i := 0; i < len(src); i++ {
		v = (v << 8) | uint64(src[i])
	}
	return v
}
