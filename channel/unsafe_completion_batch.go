package channel

import "gnalloy.org/gnalloy/transport"

func (u *Unsafe) submitAfterReadCompletion() error {
	flush := u.deferredFlush
	u.deferredFlush = false
	read := u.AutoRead()
	if flush && read && !u.readPending && !u.hasFileRegionHead() {
		if batcher, ok := u.poller.(transport.BatchSubmitter); ok {
			return u.submitWriteAndReadBatch(batcher)
		}
	}
	if flush {
		if u.hasFileRegionHead() {
			if err := u.flushReady(); err != nil {
				return err
			}
		} else {
			if err := u.submitWrite(); err != nil {
				return err
			}
		}
	}
	if read {
		return u.submitRead()
	}
	return nil
}

func (u *Unsafe) submitWriteAndReadBatch(batcher transport.BatchSubmitter) error {
	writeReq, ok := u.prepareWriteRequest()
	if !ok {
		return u.submitRead()
	}
	readReq, readBuf, err := u.prepareReadRequest()
	if err != nil {
		return err
	}
	reqs := [2]transport.IORequest{writeReq, readReq}
	u.writePending = true
	u.readPending = true
	if err := batcher.SubmitBatch(reqs[:]); err != nil {
		u.writePending = false
		u.readPending = false
		readBuf.Release()
		return err
	}
	return nil
}
