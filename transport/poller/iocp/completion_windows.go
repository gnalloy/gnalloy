//go:build windows

package iocp

import (
	"errors"
	"runtime"
	"sync"
	"syscall"
	"unsafe"

	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/windows"
)

const defaultCompletionBatch = 1024

var (
	kernel32                        = windows.NewLazySystemDLL("kernel32.dll")
	procGetQueuedCompletionStatusEx = kernel32.NewProc("GetQueuedCompletionStatusEx")
	completionBatchProc             completionBatchResolver
)

type completionEntry struct {
	key         uintptr
	overlapped  *windows.Overlapped
	reserved    uintptr
	transferred uint32
}

type completionBatchResolver struct {
	once sync.Once
	addr uintptr
	err  error
}

func completionBatchAddress() (uintptr, error) {
	completionBatchProc.once.Do(func() {
		if err := procGetQueuedCompletionStatusEx.Find(); err != nil {
			completionBatchProc.err = err
			return
		}
		completionBatchProc.addr = procGetQueuedCompletionStatusEx.Addr()
	})
	return completionBatchProc.addr, completionBatchProc.err
}

func getQueuedCompletionStatusEx(addr uintptr, port windows.Handle, entries []completionEntry, timeout uint32) (int, error) {
	if len(entries) == 0 {
		return 0, nil
	}
	if addr == 0 {
		return 0, windows.ERROR_PROC_NOT_FOUND
	}
	var removed uint32
	ret, _, callErr := syscall.SyscallN(
		addr,
		uintptr(port),
		uintptr(unsafe.Pointer(&entries[0])),
		uintptr(uint32(len(entries))),
		uintptr(unsafe.Pointer(&removed)),
		uintptr(timeout),
		0,
	)
	runtime.KeepAlive(entries)
	if ret != 0 {
		return int(removed), nil
	}
	if callErr != syscall.Errno(0) {
		return int(removed), callErr
	}
	return int(removed), syscall.EINVAL
}

func unsupportedCompletionBatch(err error) bool {
	return errors.Is(err, windows.ERROR_PROC_NOT_FOUND) ||
		errors.Is(err, windows.ERROR_CALL_NOT_IMPLEMENTED) ||
		errors.Is(err, windows.ERROR_INVALID_FUNCTION)
}

func completionTimeout(timeoutMillis int) uint32 {
	if timeoutMillis < 0 {
		return windows.INFINITE
	}
	return uint32(timeoutMillis)
}

func completionEventError(req poller.IORequest, ov *windows.Overlapped, transferred *uint32, err error) error {
	if err != nil {
		return err
	}
	if ov == nil {
		return nil
	}
	if ov.Internal == 0 {
		return nil
	}
	switch req.Op {
	case poller.OpAccept, poller.OpRead, poller.OpWrite:
	default:
		return nil
	}
	var bytes uint32
	if transferred != nil {
		bytes = *transferred
	}
	var flags uint32
	resultErr := windows.WSAGetOverlappedResult(windows.Handle(uintptr(req.FD.FD)), ov, &bytes, false, &flags)
	if transferred != nil {
		*transferred = bytes
	}
	return resultErr
}
