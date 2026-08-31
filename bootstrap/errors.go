package bootstrap

import "errors"

var (
	ErrMissingGroup         = errors.New("gnalloy/bootstrap: boss group and worker group are required")
	ErrMissingChildHandler  = errors.New("gnalloy/bootstrap: child handler is required")
	ErrMissingTransport     = errors.New("gnalloy/bootstrap: server transport is required")
	ErrMissingDialTransport = errors.New("gnalloy/bootstrap: client transport is required")
	ErrEmptyAddress         = errors.New("gnalloy/bootstrap: bind address is empty")
)
