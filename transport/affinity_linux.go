//go:build linux

package transport

import (
	"runtime"

	"golang.org/x/sys/unix"
)

func bindOSThreadToCPU(cpu int) (func(), error) {
	runtime.LockOSThread()

	var set unix.CPUSet
	set.Zero()
	set.Set(cpu)
	if err := unix.SchedSetaffinity(0, &set); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	return runtime.UnlockOSThread, nil
}
