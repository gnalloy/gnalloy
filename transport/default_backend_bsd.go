//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package transport

func DefaultBackend() BackendKind {
	return BackendKqueue
}
