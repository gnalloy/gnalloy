package transport

import "gnalloy.org/gnalloy/transport/poller"

var (
	ErrUnsupportedPoller       = poller.ErrUnsupportedPoller
	ErrClosedPoller            = poller.ErrClosedPoller
	ErrInvalidFD               = poller.ErrInvalidFD
	ErrInvalidIORequest        = poller.ErrInvalidIORequest
	ErrSubmissionQueueFull     = poller.ErrSubmissionQueueFull
	ErrCompletionQueueOverflow = poller.ErrCompletionQueueOverflow
)

type EventLoopID = poller.EventLoopID
type EventLoopGroupID = poller.EventLoopGroupID
type ChannelID = poller.ChannelID
type OpID = poller.OpID
type TaskID = poller.TaskID
type FDRef = poller.FDRef
type PollerModel = poller.Model
type IOOp = poller.IOOp
type ReadyMask = poller.ReadyMask
type BackendKind = poller.BackendKind
type SocketFamily = poller.SocketFamily
type SocketAddress = poller.SocketAddress
type PollEvent = poller.Event
type IORequest = poller.IORequest
type Poller = poller.Poller
type BatchSubmitter = poller.BatchSubmitter
type BufferRegistrar = poller.BufferRegistrar
type Config = poller.Config

const (
	PollerReadiness  = poller.Readiness
	PollerCompletion = poller.Completion

	OpAccept = poller.OpAccept
	OpRead   = poller.OpRead
	OpWrite  = poller.OpWrite
	OpClose  = poller.OpClose
	OpWakeup = poller.OpWakeup

	ReadyRead   = poller.ReadyRead
	ReadyWrite  = poller.ReadyWrite
	ReadyHangup = poller.ReadyHangup
	ReadyError  = poller.ReadyError
	// ReadyLevelTriggered 请求 readiness 后端使用水平触发。
	ReadyLevelTriggered = poller.ReadyLevelTriggered

	SocketFamilyIPv4 = poller.SocketFamilyIPv4
	SocketFamilyIPv6 = poller.SocketFamilyIPv6

	BackendMemory  = poller.BackendMemory
	BackendStd     = poller.BackendStd
	BackendEpoll   = poller.BackendEpoll
	BackendKqueue  = poller.BackendKqueue
	BackendIOUring = poller.BackendIOUring
	BackendIOCP    = poller.BackendIOCP
)
