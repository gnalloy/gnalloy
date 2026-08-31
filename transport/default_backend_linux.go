//go:build linux

package transport

func DefaultBackend() BackendKind {
	return BackendEpoll
}
