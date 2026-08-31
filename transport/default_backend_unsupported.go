//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly && !windows

package transport

func DefaultBackend() BackendKind {
	return BackendStd
}
