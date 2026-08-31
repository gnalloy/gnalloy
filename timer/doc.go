// Package timer 提供本地 Hashed Wheel Timer。
//
// Wheel 面向 EventLoop 单线程驱动，不为每个定时任务创建 goroutine 或 time.Timer。
// 调用方应在所属 EventLoop 内 Schedule、Cancel 和 Advance，避免跨线程共享带来的同步开销。
package timer
