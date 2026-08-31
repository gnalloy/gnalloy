package buffer

// NewOwnedBuffer 把外部拥有的字节切片包装为只读 ByteBuf。
//
// 返回的 ByteBuf 初始可读区为整个 data。最后一个引用释放时会调用 release(data)，
// 用于把 TLS、压缩等上游池化切片零拷贝交给 transport。
func NewOwnedBuffer(data []byte, release func([]byte)) ByteBuf {
	buf := newDirectByteBuf(data, ownedBufferReleaser{release: release})
	buf.writerIndex = len(data)
	return buf
}

type ownedBufferReleaser struct {
	release func([]byte)
}

func (r ownedBufferReleaser) releaseDirect(buf *DirectByteBuf) {
	if r.release != nil && buf != nil {
		r.release(buf.data)
	}
	if buf != nil {
		buf.data = nil
		buf.readerIndex = 0
		buf.writerIndex = 0
		buf.owner = nil
	}
}
