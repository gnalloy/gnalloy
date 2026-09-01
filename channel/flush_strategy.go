package channel

// FlushStrategy 定义出站 flush 请求映射到底层写 syscall 的时机。
type FlushStrategy uint8

const (
	// FlushImmediate 表示 Flush 立即尝试写出，适合低并发或测试场景。
	FlushImmediate FlushStrategy = iota
	// FlushOnReadComplete 表示读回调内的 Flush 延迟到本轮读完成后执行。
	FlushOnReadComplete
	// FlushOnEventLoopBatch 表示 Flush 延迟到 EventLoop 当前批次尾部执行。
	FlushOnEventLoopBatch
)
