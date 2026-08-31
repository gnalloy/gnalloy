package queue

import (
	"runtime"
	"sync/atomic"
)

const cacheLineSize = 64

type cachePad [cacheLineSize]byte

type cell[T any] struct {
	seq atomic.Uint64
	val T
}

// MPSC 是 bounded 多生产者单消费者 CAS 环形队列。
// 它用于跨 EventLoop 投递任务，消费者必须只有 owner EventLoop 一个。
type MPSC[T any] struct {
	_pad0 cachePad
	head  atomic.Uint64
	_pad1 cachePad
	tail  atomic.Uint64
	_pad2 cachePad

	mask  uint64
	cells []cell[T]

	_pad3 cachePad
}

func NewMPSC[T any](capacity uint64) *MPSC[T] {
	capacity = roundPow2(capacity)
	q := &MPSC[T]{
		mask:  capacity - 1,
		cells: make([]cell[T], capacity),
	}
	for i := uint64(0); i < capacity; i++ {
		q.cells[i].seq.Store(i)
	}
	return q
}

func (q *MPSC[T]) Offer(v T) bool {
	var spin uint32
	for {
		tail := q.tail.Load()
		c := &q.cells[tail&q.mask]
		seq := c.seq.Load()
		diff := int64(seq) - int64(tail)

		if diff == 0 {
			if q.tail.CompareAndSwap(tail, tail+1) {
				c.val = v
				c.seq.Store(tail + 1)
				return true
			}
			backoff(&spin)
			continue
		}
		if diff < 0 {
			return false
		}
		backoff(&spin)
	}
}

func (q *MPSC[T]) Poll() (T, bool) {
	head := q.head.Load()
	c := &q.cells[head&q.mask]
	seq := c.seq.Load()
	diff := int64(seq) - int64(head+1)
	if diff != 0 {
		var zero T
		return zero, false
	}

	v := c.val
	var zero T
	c.val = zero
	q.head.Store(head + 1)
	c.seq.Store(head + q.mask + 1)
	return v, true
}

func (q *MPSC[T]) Len() uint64 {
	return q.tail.Load() - q.head.Load()
}

func (q *MPSC[T]) Cap() uint64 {
	return q.mask + 1
}

func roundPow2(v uint64) uint64 {
	if v < 2 {
		return 2
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	return v + 1
}

func backoff(spin *uint32) {
	if *spin < 16 {
		*spin++
		return
	}
	runtime.Gosched()
}
