package channel

import "errors"

var (
	ErrDuplicateHandler  = errors.New("gnalloy/channel: duplicate handler name")
	ErrHandlerNotFound   = errors.New("gnalloy/channel: handler not found")
	ErrNoOutboundSink    = errors.New("gnalloy/channel: no outbound sink")
	ErrInvalidMessage    = errors.New("gnalloy/channel: invalid outbound message")
	ErrInvalidFileRegion = errors.New("gnalloy/channel: invalid file region")
	ErrFileRegionClosed  = errors.New("gnalloy/channel: file region closed")
	ErrNoTimer           = errors.New("gnalloy/channel: no timer scheduler")
	ErrPromiseFailed     = errors.New("gnalloy/channel: promise failed")
)
