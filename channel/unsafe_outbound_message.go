package channel

import "gnalloy.org/gnalloy/buffer"

type outboundMessage struct {
	buf    buffer.ByteBuf
	region FileRegion
	bytes  int64
}

func newOutboundMessage(msg any) (outboundMessage, error) {
	switch m := msg.(type) {
	case buffer.ByteBuf:
		return outboundMessage{buf: m, bytes: int64(m.ReadableBytes())}, nil
	case FileRegion:
		bytes, err := readableFileRegionBytes(m)
		if err != nil {
			return outboundMessage{}, err
		}
		return outboundMessage{region: m, bytes: bytes}, nil
	default:
		return outboundMessage{}, ErrInvalidMessage
	}
}

func readableFileRegionBytes(region FileRegion) (int64, error) {
	if region == nil {
		return 0, ErrInvalidFileRegion
	}
	count := region.Count()
	transferred := region.Transferred()
	if count < 0 || transferred < 0 || transferred > count {
		return 0, ErrInvalidFileRegion
	}
	return count - transferred, nil
}

func releaseWriteMessage(msg any) {
	switch m := msg.(type) {
	case buffer.ByteBuf:
		m.Release()
	case FileRegion:
		_ = m.Close()
	}
}

func (m outboundMessage) release() {
	if m.buf != nil {
		m.buf.Release()
	}
	if m.region != nil {
		_ = m.region.Close()
	}
}
