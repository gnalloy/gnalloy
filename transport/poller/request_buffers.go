package poller

// RetainBuffers 为异步 I/O 保留请求携带的 ByteBuf。
// TransferBufferOwnership 为 true 时，调用方把当前引用转交给 Poller，不再额外 Retain。
func (r IORequest) RetainBuffers() bool {
	if r.TransferBufferOwnership {
		return false
	}
	if r.Buf != nil {
		r.Buf.Retain()
	}
	for _, buf := range r.Bufs {
		buf.Retain()
	}
	return r.Buf != nil || len(r.Bufs) > 0
}

// ReleaseBuffers 释放 Poller 已接管或额外保留的 ByteBuf 引用。
func (r IORequest) ReleaseBuffers() {
	if r.Buf != nil {
		r.Buf.Release()
	}
	for _, buf := range r.Bufs {
		buf.Release()
	}
}
