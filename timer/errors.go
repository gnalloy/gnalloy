package timer

import "errors"

var (
	ErrInvalidTick      = errors.New("gnalloy/timer: invalid tick duration")
	ErrInvalidWheelSize = errors.New("gnalloy/timer: invalid wheel size")
	ErrNilTimerCallback = errors.New("gnalloy/timer: nil callback")
	ErrTimerWheelClosed = errors.New("gnalloy/timer: timer wheel closed")
	ErrForeignTimerTask = errors.New("gnalloy/timer: timer belongs to another wheel")
)
