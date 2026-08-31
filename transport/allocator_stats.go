package transport

import "gnalloy.org/gnalloy/buffer"

// AllocatorStatsForEventLoop 返回带 EventLoop 归属标签的 allocator 快照。
func AllocatorStatsForEventLoop(id EventLoopID, alloc buffer.Allocator) buffer.AllocatorStats {
	var stats buffer.AllocatorStats
	if observed, ok := alloc.(buffer.StatAllocator); ok {
		stats = observed.Stats()
	}
	stats.EventLoopID = uint32(id)
	stats.EventLoopLocal = true
	return stats
}
