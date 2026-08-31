package codec

// MessageList 是编解码模板在单次调用内复用的轻量输出容器。
type MessageList struct {
	inline [4]any
	items  []any
}

func (l *MessageList) Add(msg any) {
	if msg == nil {
		return
	}
	if len(l.items) < len(l.inline) {
		l.items = l.inline[:len(l.items)+1]
		l.items[len(l.items)-1] = msg
		return
	}
	l.items = append(l.items, msg)
}

func (l *MessageList) Len() int {
	return len(l.items)
}

func (l *MessageList) At(i int) any {
	return l.items[i]
}

func (l *MessageList) Reset() {
	for i := range l.items {
		l.items[i] = nil
	}
	l.items = l.items[:0]
}

func (l *MessageList) ReleaseAll() {
	for _, msg := range l.items {
		releaseMessage(msg)
	}
	l.Reset()
}
