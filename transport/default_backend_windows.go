//go:build windows

package transport

func DefaultBackend() BackendKind {
	return BackendIOCP
}
