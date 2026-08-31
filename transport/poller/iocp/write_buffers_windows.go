//go:build windows

package iocp

import (
	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/windows"
)

func makeWriteBuffers(req poller.IORequest, dst []windows.WSABuf) ([]windows.WSABuf, error) {
	var err error
	if req.Buf != nil {
		dst, err = appendWriteBuffer(dst, req.Buf)
	} else {
		for _, buf := range req.Bufs {
			dst, err = appendWriteBuffer(dst, buf)
			if err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if len(dst) == 0 {
		return nil, poller.ErrInvalidIORequest
	}
	return dst, nil
}

func appendWriteBuffer(dst []windows.WSABuf, src buffer.ByteBuf) ([]windows.WSABuf, error) {
	if src == nil || src.ReadableBytes() == 0 {
		return dst, nil
	}
	if data, ok := buffer.ContiguousReadableBytes(src); ok {
		return appendWSABuf(dst, data)
	}
	before := len(dst)
	var err error
	buffer.ForEachReadableSlice(src, func(data []byte) bool {
		if len(data) == 0 {
			return true
		}
		next, err := appendWSABuf(dst, data)
		if err != nil {
			return false
		}
		dst = next
		return true
	})
	if err != nil {
		return nil, err
	}
	if len(dst) == before {
		return nil, poller.ErrInvalidIORequest
	}
	return dst, nil
}

func appendWSABuf(dst []windows.WSABuf, data []byte) ([]windows.WSABuf, error) {
	if len(data) == 0 {
		return dst, nil
	}
	if uint64(len(data)) > uint64(^uint32(0)) {
		return nil, poller.ErrInvalidIORequest
	}
	return append(dst, windows.WSABuf{Len: uint32(len(data)), Buf: &data[0]}), nil
}
