package channel

import (
	"sync"

	"gnalloy.org/gnalloy/transport"
)

type ownerWriteEntry struct {
	msg  any
	next *ownerWriteEntry
}

type ownerWriteQueue struct {
	mu        sync.Mutex
	head      *ownerWriteEntry
	tail      *ownerWriteEntry
	free      *ownerWriteEntry
	scheduled bool
	drainTask transport.Task
}

func (c *LocalChannel) submitOwnerWriteAndFlush(executor FutureListenerExecutor, msg any) error {
	q := &c.ownerWrites
	q.mu.Lock()
	entry := q.acquire(msg)
	if q.tail == nil {
		q.head = entry
		q.tail = entry
	} else {
		q.tail.next = entry
		q.tail = entry
	}
	if q.scheduled {
		q.mu.Unlock()
		return nil
	}
	q.scheduled = true
	if q.drainTask == nil {
		q.drainTask = c.drainOwnerWrites
	}
	task := q.drainTask
	q.mu.Unlock()

	if err := executor.Submit(task); err != nil {
		c.rejectOwnerWrites(err)
		return err
	}
	return nil
}

func (c *LocalChannel) drainOwnerWrites() {
	q := &c.ownerWrites
	for {
		q.mu.Lock()
		entry := q.head
		if entry == nil {
			q.scheduled = false
			q.mu.Unlock()
			return
		}
		q.head = entry.next
		if q.head == nil {
			q.tail = nil
		}
		entry.next = nil
		q.mu.Unlock()

		err := c.pipeline.WriteAndFlush(entry.msg)
		q.mu.Lock()
		q.release(entry)
		q.mu.Unlock()
		if err != nil {
			c.pipeline.FireExceptionCaught(err)
		}
	}
}

func (c *LocalChannel) rejectOwnerWrites(err error) {
	q := &c.ownerWrites
	q.mu.Lock()
	head := q.head
	q.head = nil
	q.tail = nil
	q.scheduled = false
	q.mu.Unlock()

	for entry := head; entry != nil; {
		next := entry.next
		releaseMessage(entry.msg)
		q.mu.Lock()
		q.release(entry)
		q.mu.Unlock()
		entry = next
	}
	if err != nil {
		c.pipeline.FireExceptionCaught(err)
	}
}

func (q *ownerWriteQueue) acquire(msg any) *ownerWriteEntry {
	if q.free == nil {
		return &ownerWriteEntry{msg: msg}
	}
	entry := q.free
	q.free = entry.next
	entry.msg = msg
	entry.next = nil
	return entry
}

func (q *ownerWriteQueue) release(entry *ownerWriteEntry) {
	entry.msg = nil
	entry.next = q.free
	q.free = entry
}
