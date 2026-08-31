//go:build !linux

package transport

func bindOSThreadToCPU(int) (func(), error) {
	return nil, ErrUnsupportedAffinity
}
