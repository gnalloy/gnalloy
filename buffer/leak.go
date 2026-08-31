package buffer

import (
	"runtime"
	"sync"
	"sync/atomic"
)

const leakStackDepth = 16

var globalLeakDetector = &LeakDetector{}

// LeakRecord 描述一个仍未释放的 ByteBuf。
type LeakRecord struct {
	ID    uint64
	Kind  string
	Stack []uintptr
}

// LeakDetector 是轻量 ByteBuf 泄漏探测器，默认关闭，测试或调试时显式开启。
type LeakDetector struct {
	enabled atomic.Bool
	nextID  atomic.Uint64
	records sync.Map
}

type leakRecord struct {
	id    uint64
	kind  string
	stack [leakStackDepth]uintptr
	n     int
}

func EnableLeakDetection(enabled bool) {
	globalLeakDetector.Enable(enabled)
}

func ResetLeakDetection() {
	globalLeakDetector.Reset()
}

func ActiveLeaks() []LeakRecord {
	return globalLeakDetector.Active()
}

func ActiveLeakCount() int {
	return globalLeakDetector.Count()
}

func (d *LeakDetector) Enable(enabled bool) {
	d.enabled.Store(enabled)
}

func (d *LeakDetector) Reset() {
	d.records.Range(func(key any, _ any) bool {
		d.records.Delete(key)
		return true
	})
	d.nextID.Store(0)
}

func (d *LeakDetector) Active() []LeakRecord {
	out := make([]LeakRecord, 0)
	d.records.Range(func(_ any, value any) bool {
		record := value.(leakRecord)
		stack := make([]uintptr, record.n)
		copy(stack, record.stack[:record.n])
		out = append(out, LeakRecord{ID: record.id, Kind: record.kind, Stack: stack})
		return true
	})
	return out
}

func (d *LeakDetector) Count() int {
	count := 0
	d.records.Range(func(any, any) bool {
		count++
		return true
	})
	return count
}

func trackLeak(kind string) uint64 {
	if !globalLeakDetector.enabled.Load() {
		return 0
	}
	id := globalLeakDetector.nextID.Add(1)
	record := leakRecord{id: id, kind: kind}
	record.n = runtime.Callers(3, record.stack[:])
	globalLeakDetector.records.Store(id, record)
	return id
}

func untrackLeak(id uint64) {
	if id == 0 {
		return
	}
	globalLeakDetector.records.Delete(id)
}
