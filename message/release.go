package message

import "gnalloy.org/gnalloy/buffer"

// Release 释放常见 gnalloy 消息。
func Release(msg any) {
	if msg == nil {
		return
	}
	if buf, ok := msg.(buffer.ByteBuf); ok {
		buf.Release()
		return
	}
	if releasable, ok := msg.(interface{ Release() bool }); ok {
		releasable.Release()
		return
	}
	if releasable, ok := msg.(interface{ Release() }); ok {
		releasable.Release()
	}
}

// ReleaseAll 释放消息切片中的每个消息。
func ReleaseAll(messages []any) {
	for _, msg := range messages {
		Release(msg)
	}
}
