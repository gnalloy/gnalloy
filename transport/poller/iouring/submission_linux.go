//go:build linux

package iouring

import (
	"sync/atomic"

	"gnalloy.org/gnalloy/transport/poller"
)

const inlineBatchRequests = 16

type batchRequest struct {
	id       uint64
	req      poller.IORequest
	retained bool
}

type requestIDSet struct {
	inline [inlineBatchRequests]uint64
	count  int
	spill  map[uint64]struct{}
}

func newRequestIDSet(size int) requestIDSet {
	if size <= inlineBatchRequests {
		return requestIDSet{}
	}
	return requestIDSet{spill: make(map[uint64]struct{}, size)}
}

func (s *requestIDSet) Add(id uint64) bool {
	if id == 0 {
		return false
	}
	if s.spill != nil {
		if _, ok := s.spill[id]; ok {
			return false
		}
		s.spill[id] = struct{}{}
		return true
	}
	for i := 0; i < s.count; i++ {
		if s.inline[i] == id {
			return false
		}
	}
	s.inline[s.count] = id
	s.count++
	return true
}

func (s *requestIDSet) Contains(id uint64) bool {
	if s == nil || id == 0 {
		return false
	}
	if s.spill != nil {
		_, ok := s.spill[id]
		return ok
	}
	for i := 0; i < s.count; i++ {
		if s.inline[i] == id {
			return true
		}
	}
	return false
}

func (p *Poller) Submit(req poller.IORequest) error {
	if req.Op == poller.OpWakeup {
		return p.Wakeup()
	}
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	if !validRequest(req) {
		return poller.ErrInvalidIORequest
	}
	id, err := p.assignSingleRequestID(&req)
	if err != nil {
		return err
	}
	retained := req.RetainBuffers()
	nextTail, err := p.prepare(uint64(id), req)
	if err != nil {
		p.releaseWriteVectorContext(uint64(id))
		delete(p.msgctx, uint64(id))
		if retained {
			req.ReleaseBuffers()
		}
		return err
	}
	p.pending[uint64(id)] = req
	atomic.StoreUint32(p.sq.tail, nextTail)
	if err := p.enter(1, 0, p.submitEnterFlags()); err != nil {
		delete(p.pending, uint64(id))
		p.releaseWriteVectorContext(uint64(id))
		delete(p.msgctx, uint64(id))
		if retained {
			req.ReleaseBuffers()
		}
		return err
	}
	return nil
}

func (p *Poller) SubmitBatch(reqs []poller.IORequest) error {
	if len(reqs) == 0 {
		return nil
	}
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	for i := range reqs {
		if reqs[i].Op == poller.OpWakeup {
			return poller.ErrInvalidIORequest
		}
	}
	if len(reqs) == 1 {
		return p.Submit(reqs[0])
	}
	return p.submitBatch(reqs)
}

func (p *Poller) submitBatch(reqs []poller.IORequest) error {
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	if len(reqs) == 0 {
		return nil
	}
	seen := newRequestIDSet(len(reqs))
	if err := p.validateBatch(reqs, &seen); err != nil {
		return err
	}
	if len(reqs) > int(p.sqAvailable()) {
		return poller.ErrSubmissionQueueFull
	}

	var inline [inlineBatchRequests]batchRequest
	prepared := inline[:0]
	if len(reqs) > inlineBatchRequests {
		prepared = make([]batchRequest, 0, len(reqs))
	}

	nextTail := atomic.LoadUint32(p.sq.tail)
	for i := range reqs {
		req := reqs[i]
		id := uint64(req.OpID)
		if id == 0 {
			var err error
			id, err = p.nextAvailableUserData(&seen)
			if err != nil {
				p.rollbackBatch(prepared)
				return err
			}
			req.OpID = poller.OpID(id)
			seen.Add(id)
		}

		retained := req.RetainBuffers()
		tail, err := p.prepareAt(id, req, nextTail)
		if err != nil {
			p.releaseWriteVectorContext(id)
			delete(p.msgctx, id)
			if retained {
				req.ReleaseBuffers()
			}
			p.rollbackBatch(prepared)
			return err
		}
		nextTail = tail
		prepared = append(prepared, batchRequest{id: id, req: req, retained: retained})
	}

	for i := range prepared {
		p.pending[prepared[i].id] = prepared[i].req
	}
	atomic.StoreUint32(p.sq.tail, nextTail)
	if err := p.enter(uint32(len(prepared)), 0, p.submitEnterFlags()); err != nil {
		p.rollbackBatch(prepared)
		return err
	}
	return nil
}

func (p *Poller) assignSingleRequestID(req *poller.IORequest) (uint64, error) {
	id := uint64(req.OpID)
	if id != 0 {
		if _, ok := p.pending[id]; ok {
			return 0, poller.ErrInvalidIORequest
		}
		return id, nil
	}
	id, err := p.nextAvailableUserData(nil)
	if err != nil {
		return 0, err
	}
	req.OpID = poller.OpID(id)
	return id, nil
}

func (p *Poller) validateBatch(reqs []poller.IORequest, seen *requestIDSet) error {
	for i := range reqs {
		req := reqs[i]
		if !validRequest(req) {
			return poller.ErrInvalidIORequest
		}
		id := uint64(req.OpID)
		if id == 0 {
			continue
		}
		if _, ok := p.pending[id]; ok {
			return poller.ErrInvalidIORequest
		}
		if !seen.Add(id) {
			return poller.ErrInvalidIORequest
		}
	}
	return nil
}

func (p *Poller) nextAvailableUserData(seen *requestIDSet) (uint64, error) {
	attempts := len(p.pending) + 1
	if seen != nil {
		attempts += seen.count
		if seen.spill != nil {
			attempts += len(seen.spill)
		}
	}
	for i := 0; i <= attempts; i++ {
		id := p.nextUserData()
		if id == 0 {
			continue
		}
		if _, ok := p.pending[id]; ok {
			continue
		}
		if seen.Contains(id) {
			continue
		}
		return id, nil
	}
	return 0, poller.ErrInvalidIORequest
}

func (p *Poller) rollbackBatch(prepared []batchRequest) {
	for i := range prepared {
		req := prepared[i].req
		id := prepared[i].id
		delete(p.pending, id)
		p.releaseWriteVectorContext(id)
		delete(p.msgctx, id)
		if prepared[i].retained {
			req.ReleaseBuffers()
		}
	}
}

func (p *Poller) sqAvailable() uint32 {
	head := atomic.LoadUint32(p.sq.head)
	tail := atomic.LoadUint32(p.sq.tail)
	entries := atomic.LoadUint32(p.sq.ringEntries)
	used := tail - head
	if used >= entries {
		return 0
	}
	return entries - used
}
