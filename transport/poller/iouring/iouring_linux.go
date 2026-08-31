//go:build linux

package iouring

import (
	"errors"
	"sync/atomic"
	"unsafe"

	"gnalloy.org/gnalloy/buffer"
	"gnalloy.org/gnalloy/transport/poller"
	"golang.org/x/sys/unix"
)

const (
	defaultEntries = 256
	defaultSQIdle  = 2000

	offSqRing = 0
	offCqRing = 0x8000000
	offSqes   = 0x10000000

	enterGetEvents  = 1
	enterSQWakeup   = 1 << 1
	setupSQPoll     = 1 << 1
	setupSQAffinity = 1 << 2

	featSingleMmap = 1
	sqNeedWakeup   = 1

	registerBuffers   = 0
	unregisterBuffers = 1

	cqeFMore = 1 << 1

	acceptMultishot = 1 << 0

	opNop        = 0
	opWritev     = 2
	opReadFixed  = 4
	opWriteFixed = 5
	opSendMsg    = 9
	opRecvMsg    = 10
	opAccept     = 13
	opClose      = 19
	opRead       = 22
	opWrite      = 23
)

type sqringOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	userAddr    uint64
}

type cqringOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	userAddr    uint64
}

type params struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFD         uint32
	resv         [3]uint32
	sqOff        sqringOffsets
	cqOff        cqringOffsets
}

type sqe struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64
	addr        uint64
	len         uint32
	rwFlags     uint32
	userData    uint64
	bufIndex    uint16
	personality uint16
	spliceFdIn  int32
	addr3       uint64
	pad2        [1]uint64
}

type cqe struct {
	userData uint64
	res      int32
	flags    uint32
}

type sq struct {
	ring        []byte
	sqes        []byte
	head        *uint32
	tail        *uint32
	ringMask    *uint32
	ringEntries *uint32
	flags       *uint32
	dropped     *uint32
	array       *uint32
}

type cq struct {
	ring     []byte
	head     *uint32
	tail     *uint32
	ringMask *uint32
	overflow *uint32
	cqes     *cqe
}

type iovec struct {
	base uintptr
	len  uintptr
}

type msghdr struct {
	name       uintptr
	namelen    uint32
	_pad0      uint32
	iov        uintptr
	iovlen     uint64
	control    uintptr
	controllen uint64
	flags      uint32
	_pad1      uint32
}

type msgContext struct {
	name    unix.RawSockaddrAny
	nameLen uint32
	inline  [inlineWriteVectors]iovec
	iov     []iovec
	hdr     msghdr
}

type Poller struct {
	fd     int
	wakefd int

	params params
	sq     sq
	cq     cq

	entries    map[poller.FDRef]poller.ChannelID
	pending    map[uint64]poller.IORequest
	writev     map[uint64]*writeVectorContext
	msgctx     map[uint64]*msgContext
	nextID     uint64
	closed     atomic.Bool
	freeWritev *writeVectorContext

	multishotAccept   bool
	registeredBuffers bool
	cqOverflowSeen    uint32
}

type Config struct {
	Entries          uint32
	SQPoll           bool
	SQPollAffinity   bool
	SQPollCPU        int
	SQPollIdleMillis uint32
	MultishotAccept  bool
}

type Stats struct {
	Pending           int
	SQAvailable       uint32
	SQDropped         uint32
	CQOverflow        uint32
	RegisteredBuffers bool
	MultishotAccept   bool
}

func New() (poller.Poller, error) {
	return NewWithConfig(Config{})
}

func NewWithEntries(entries uint32) (poller.Poller, error) {
	return NewWithConfig(Config{Entries: entries})
}

func NewWithConfig(cfg Config) (poller.Poller, error) {
	if cfg.SQPollAffinity && !cfg.SQPoll {
		return nil, poller.ErrInvalidIORequest
	}
	if cfg.SQPollAffinity && cfg.SQPollCPU < 0 {
		return nil, poller.ErrInvalidIORequest
	}
	entries := cfg.Entries
	if entries == 0 {
		entries = defaultEntries
	}
	p := &Poller{
		wakefd:          -1,
		entries:         make(map[poller.FDRef]poller.ChannelID, entries),
		pending:         make(map[uint64]poller.IORequest, entries),
		writev:          make(map[uint64]*writeVectorContext, entries),
		msgctx:          make(map[uint64]*msgContext, entries),
		multishotAccept: cfg.MultishotAccept,
	}
	if err := p.setup(entries, cfg); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Poller) Model() poller.Model {
	return poller.Completion
}

func (p *Poller) Backend() poller.BackendKind {
	return poller.BackendIOUring
}

func (p *Poller) RegisterBuffers(buffers [][]byte) error {
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	if len(buffers) == 0 {
		return poller.ErrInvalidIORequest
	}
	iovecs := make([]iovec, len(buffers))
	for i := range buffers {
		if len(buffers[i]) == 0 {
			return poller.ErrInvalidIORequest
		}
		iovecs[i] = iovec{
			base: uintptr(unsafe.Pointer(&buffers[i][0])),
			len:  uintptr(len(buffers[i])),
		}
	}
	if err := p.register(registerBuffers, unsafe.Pointer(&iovecs[0]), uint32(len(iovecs))); err != nil {
		return err
	}
	p.registeredBuffers = true
	return nil
}

func (p *Poller) UnregisterBuffers() error {
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	if !p.registeredBuffers {
		return nil
	}
	if err := p.register(unregisterBuffers, nil, 0); err != nil {
		return err
	}
	p.registeredBuffers = false
	return nil
}

func (p *Poller) Stats() Stats {
	head := atomic.LoadUint32(p.sq.head)
	tail := atomic.LoadUint32(p.sq.tail)
	entries := atomic.LoadUint32(p.sq.ringEntries)
	used := tail - head
	available := uint32(0)
	if entries > used {
		available = entries - used
	}
	var dropped uint32
	if p.sq.dropped != nil {
		dropped = atomic.LoadUint32(p.sq.dropped)
	}
	var overflow uint32
	if p.cq.overflow != nil {
		overflow = atomic.LoadUint32(p.cq.overflow)
	}
	return Stats{
		Pending:           len(p.pending),
		SQAvailable:       available,
		SQDropped:         dropped,
		CQOverflow:        overflow,
		RegisteredBuffers: p.registeredBuffers,
		MultishotAccept:   p.multishotAccept,
	}
}

func (p *Poller) Register(fd poller.FDRef, ch poller.ChannelID, _ poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	p.entries[fd] = ch
	return nil
}

func (p *Poller) Modify(fd poller.FDRef, _ poller.ReadyMask) error {
	if !fd.Valid() {
		return poller.ErrInvalidFD
	}
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	return nil
}

func (p *Poller) Deregister(fd poller.FDRef) error {
	delete(p.entries, fd)
	return nil
}

func (p *Poller) Poll(dst []poller.Event, timeoutMillis int) (int, error) {
	if p.closed.Load() {
		return 0, poller.ErrClosedPoller
	}
	if len(dst) == 0 {
		return 0, nil
	}
	n := p.reap(dst)
	if n > 0 || timeoutMillis == 0 {
		return n, nil
	}
	if p.completionOverflowed() {
		return 0, poller.ErrCompletionQueueOverflow
	}
	woken, err := p.waitReadable(timeoutMillis)
	if err != nil {
		return 0, err
	}
	if woken {
		p.drainWakeup()
	}
	n = p.reap(dst)
	if n == 0 && p.completionOverflowed() {
		return 0, poller.ErrCompletionQueueOverflow
	}
	if n == 0 && woken {
		dst[0] = poller.Event{
			Model: poller.Completion,
			Op:    poller.OpWakeup,
			FD:    poller.FDRef{FD: p.wakefd},
			Ready: poller.ReadyRead,
		}
		return 1, nil
	}
	return n, nil
}

func (p *Poller) Wakeup() error {
	if p.closed.Load() {
		return poller.ErrClosedPoller
	}
	if err := p.signalWakeup(); err != nil {
		if errors.Is(err, unix.EBADF) && p.closed.Load() {
			return poller.ErrClosedPoller
		}
		return err
	}
	return nil
}

func (p *Poller) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	if p.registeredBuffers {
		_ = p.register(unregisterBuffers, nil, 0)
		p.registeredBuffers = false
	}
	for _, req := range p.pending {
		req.ReleaseBuffers()
	}
	clear(p.writev)
	clear(p.msgctx)
	p.writev = nil
	p.msgctx = nil
	p.freeWritev = nil
	p.pending = nil
	err1 := unmapIfPresent(p.sq.sqes)
	if len(p.cq.ring) > 0 && len(p.sq.ring) > 0 && &p.cq.ring[0] == &p.sq.ring[0] {
		p.cq.ring = nil
	}
	err2 := unmapIfPresent(p.cq.ring)
	err3 := unmapIfPresent(p.sq.ring)
	err4 := p.closeWakeup()
	err5 := unix.Close(p.fd)
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	if err3 != nil {
		return err3
	}
	if err4 != nil {
		return err4
	}
	return err5
}

func (p *Poller) setup(entries uint32, cfg Config) error {
	wakefd, err := unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		return err
	}
	p.wakefd = wakefd
	ok := false
	defer func() {
		if !ok {
			_ = p.closeWakeup()
		}
	}()
	if cfg.SQPoll {
		p.params.flags |= setupSQPoll
		idle := cfg.SQPollIdleMillis
		if idle == 0 {
			idle = defaultSQIdle
		}
		p.params.sqThreadIdle = idle
	}
	if cfg.SQPollAffinity {
		p.params.flags |= setupSQAffinity
		p.params.sqThreadCPU = uint32(cfg.SQPollCPU)
	}

	fd, _, errno := unix.Syscall(unix.SYS_IO_URING_SETUP, uintptr(entries), uintptr(unsafe.Pointer(&p.params)), 0)
	if errno != 0 {
		return errno
	}
	p.fd = int(fd)

	sqRingSize := p.params.sqOff.array + p.params.sqEntries*uint32(unsafe.Sizeof(uint32(0)))
	cqRingSize := p.params.cqOff.cqes + p.params.cqEntries*uint32(unsafe.Sizeof(cqe{}))
	if p.params.features&featSingleMmap != 0 && cqRingSize > sqRingSize {
		sqRingSize = cqRingSize
	}

	sqRing, err := unix.Mmap(p.fd, offSqRing, int(sqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		_ = unix.Close(p.fd)
		return err
	}
	cqRing := sqRing
	if p.params.features&featSingleMmap == 0 {
		cqRing, err = unix.Mmap(p.fd, offCqRing, int(cqRingSize), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
		if err != nil {
			_ = unix.Munmap(sqRing)
			_ = unix.Close(p.fd)
			return err
		}
	}
	sqesSize := int(p.params.sqEntries) * int(unsafe.Sizeof(sqe{}))
	sqes, err := unix.Mmap(p.fd, offSqes, sqesSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		if p.params.features&featSingleMmap == 0 {
			_ = unix.Munmap(cqRing)
		}
		_ = unix.Munmap(sqRing)
		_ = unix.Close(p.fd)
		return err
	}

	p.sq = sq{
		ring:        sqRing,
		sqes:        sqes,
		head:        uint32Ptr(sqRing, p.params.sqOff.head),
		tail:        uint32Ptr(sqRing, p.params.sqOff.tail),
		ringMask:    uint32Ptr(sqRing, p.params.sqOff.ringMask),
		ringEntries: uint32Ptr(sqRing, p.params.sqOff.ringEntries),
		flags:       uint32Ptr(sqRing, p.params.sqOff.flags),
		dropped:     uint32Ptr(sqRing, p.params.sqOff.dropped),
		array:       uint32Ptr(sqRing, p.params.sqOff.array),
	}
	p.cq = cq{
		ring:     cqRing,
		head:     uint32Ptr(cqRing, p.params.cqOff.head),
		tail:     uint32Ptr(cqRing, p.params.cqOff.tail),
		ringMask: uint32Ptr(cqRing, p.params.cqOff.ringMask),
		overflow: uint32Ptr(cqRing, p.params.cqOff.overflow),
		cqes:     (*cqe)(unsafe.Pointer(&cqRing[p.params.cqOff.cqes])),
	}
	ok = true
	return nil
}

func (p *Poller) prepare(userData uint64, req poller.IORequest) (uint32, error) {
	tail := atomic.LoadUint32(p.sq.tail)
	return p.prepareAt(userData, req, tail)
}

func (p *Poller) prepareAt(userData uint64, req poller.IORequest, tail uint32) (uint32, error) {
	head := atomic.LoadUint32(p.sq.head)
	if tail-head >= atomic.LoadUint32(p.sq.ringEntries) {
		return 0, poller.ErrSubmissionQueueFull
	}
	index := tail & atomic.LoadUint32(p.sq.ringMask)
	entry := p.sqe(index)
	*entry = sqe{}
	entry.userData = userData

	switch req.Op {
	case poller.OpWakeup:
		entry.opcode = opNop
	case poller.OpAccept:
		entry.opcode = opAccept
		entry.fd = int32(req.FD.FD)
		entry.rwFlags = unix.SOCK_NONBLOCK | unix.SOCK_CLOEXEC
		if p.multishotAccept {
			entry.ioprio |= acceptMultishot
		}
	case poller.OpRead:
		if req.Datagram {
			ctx, err := makeRecvMsgContext(req)
			if err != nil {
				return 0, err
			}
			p.msgctx[userData] = ctx
			entry.opcode = opRecvMsg
			entry.fd = int32(req.FD.FD)
			entry.addr = uint64(uintptr(unsafe.Pointer(&ctx.hdr)))
			entry.len = 1
			break
		}
		view := req.Buf.WritableBytesView()
		if len(view) == 0 {
			return 0, poller.ErrInvalidIORequest
		}
		if req.UseFixedBuffer {
			entry.opcode = opReadFixed
			entry.bufIndex = req.FixedBufferIndex
		} else {
			entry.opcode = opRead
		}
		entry.fd = int32(req.FD.FD)
		entry.addr = uint64(uintptr(unsafe.Pointer(&view[0])))
		entry.len = uint32(len(view))
	case poller.OpWrite:
		if req.Datagram {
			ctx, err := makeSendMsgContext(req)
			if err != nil {
				return 0, err
			}
			p.msgctx[userData] = ctx
			entry.opcode = opSendMsg
			entry.fd = int32(req.FD.FD)
			entry.addr = uint64(uintptr(unsafe.Pointer(&ctx.hdr)))
			entry.len = 1
			break
		}
		if len(req.Bufs) > 0 {
			ctx, err := p.acquireWriteVectorContext(req)
			if err != nil {
				return 0, err
			}
			p.writev[userData] = ctx
			entry.opcode = opWritev
			entry.fd = int32(req.FD.FD)
			entry.addr = uint64(uintptr(unsafe.Pointer(&ctx.vectors[0])))
			entry.len = uint32(len(ctx.vectors))
			break
		}
		if data, ok := buffer.ContiguousReadableBytes(req.Buf); ok {
			if len(data) == 0 {
				return 0, poller.ErrInvalidIORequest
			}
			if req.UseFixedBuffer {
				entry.opcode = opWriteFixed
				entry.bufIndex = req.FixedBufferIndex
			} else {
				entry.opcode = opWrite
			}
			entry.fd = int32(req.FD.FD)
			entry.addr = uint64(uintptr(unsafe.Pointer(&data[0])))
			entry.len = uint32(len(data))
		} else {
			if req.UseFixedBuffer {
				return 0, poller.ErrInvalidIORequest
			}
			ctx, err := p.acquireWriteVectorContext(req)
			if err != nil {
				return 0, err
			}
			p.writev[userData] = ctx
			entry.opcode = opWritev
			entry.fd = int32(req.FD.FD)
			entry.addr = uint64(uintptr(unsafe.Pointer(&ctx.vectors[0])))
			entry.len = uint32(len(ctx.vectors))
		}
	case poller.OpClose:
		entry.opcode = opClose
		entry.fd = int32(req.FD.FD)
	default:
		return 0, poller.ErrInvalidIORequest
	}

	array := (*uint32)(unsafe.Pointer(uintptr(unsafe.Pointer(p.sq.array)) + uintptr(index)*unsafe.Sizeof(uint32(0))))
	atomic.StoreUint32(array, index)
	return tail + 1, nil
}

func (p *Poller) reap(dst []poller.Event) int {
	head := atomic.LoadUint32(p.cq.head)
	tail := atomic.LoadUint32(p.cq.tail)
	out := 0
	for head != tail && out < len(dst) {
		index := head & atomic.LoadUint32(p.cq.ringMask)
		cqe := p.cqe(index)
		req := p.pending[cqe.userData]
		msgctx := p.msgctx[cqe.userData]
		more := cqe.flags&cqeFMore != 0
		if !more {
			delete(p.pending, cqe.userData)
			p.releaseWriteVectorContext(cqe.userData)
			delete(p.msgctx, cqe.userData)
		}

		n := int(cqe.res)
		var err error
		if cqe.res < 0 {
			err = unix.Errno(-cqe.res)
			n = 0
		}
		if req.Buf != nil && req.Op == poller.OpRead && n > 0 {
			if advErr := req.Buf.AdvanceWriter(n); advErr != nil && err == nil {
				err = advErr
			}
		}
		addr := req.Addr
		if req.Op == poller.OpRead && req.Datagram && err == nil && msgctx != nil {
			addr = socketAddressFromRaw(&msgctx.name, msgctx.nameLen)
		}
		acceptedFD := req.AcceptedFD
		if req.Op == poller.OpAccept && err == nil {
			acceptedFD = poller.FDRef{FD: n}
		}
		dst[out] = poller.Event{
			Model:      poller.Completion,
			Op:         req.Op,
			Ready:      poller.CompletionReady(req.Op),
			FD:         req.FD,
			AcceptedFD: acceptedFD,
			ChannelID:  req.ChannelID,
			OpID:       req.OpID,
			Buf:        req.Buf,
			Bufs:       req.Bufs,
			Addr:       addr,
			N:          n,
			Err:        err,
			More:       more,
		}
		out++
		head++
	}
	atomic.StoreUint32(p.cq.head, head)
	return out
}

func validRequest(req poller.IORequest) bool {
	switch req.Op {
	case poller.OpWakeup:
		return true
	case poller.OpAccept, poller.OpClose:
		return req.FD.Valid()
	case poller.OpRead, poller.OpWrite:
		if !req.FD.Valid() || (req.Buf == nil && len(req.Bufs) == 0) {
			return false
		}
		return !req.Datagram || req.Op == poller.OpRead || req.Addr.Valid()
	default:
		return false
	}
}

func makeRecvMsgContext(req poller.IORequest) (*msgContext, error) {
	view := req.Buf.WritableBytesView()
	if len(view) == 0 {
		return nil, poller.ErrInvalidIORequest
	}
	ctx := &msgContext{nameLen: unix.SizeofSockaddrAny}
	ctx.iov = ctx.inline[:1]
	ctx.iov[0] = iovec{base: uintptr(unsafe.Pointer(&view[0])), len: uintptr(len(view))}
	ctx.hdr = msghdr{
		name:    uintptr(unsafe.Pointer(&ctx.name)),
		namelen: ctx.nameLen,
		iov:     uintptr(unsafe.Pointer(&ctx.iov[0])),
		iovlen:  1,
	}
	return ctx, nil
}

func makeSendMsgContext(req poller.IORequest) (*msgContext, error) {
	name, nameLen, err := makeRawSockaddr(req.Addr)
	if err != nil {
		return nil, err
	}
	ctx := &msgContext{name: name, nameLen: nameLen}
	vectors, err := makeIOVectors(req, ctx.inline[:0])
	if err != nil {
		return nil, err
	}
	ctx.iov = vectors
	ctx.hdr = msghdr{
		name:    uintptr(unsafe.Pointer(&ctx.name)),
		namelen: ctx.nameLen,
		iov:     uintptr(unsafe.Pointer(&ctx.iov[0])),
		iovlen:  uint64(len(ctx.iov)),
	}
	return ctx, nil
}

func makeRawSockaddr(addr poller.SocketAddress) (unix.RawSockaddrAny, uint32, error) {
	var rsa unix.RawSockaddrAny
	switch addr.Family {
	case poller.SocketFamilyIPv4:
		raw := (*unix.RawSockaddrInet4)(unsafe.Pointer(&rsa))
		raw.Family = unix.AF_INET
		raw.Port = htons(uint16(addr.Port))
		copy(raw.Addr[:], addr.IP[:4])
		return rsa, unix.SizeofSockaddrInet4, nil
	case poller.SocketFamilyIPv6:
		raw := (*unix.RawSockaddrInet6)(unsafe.Pointer(&rsa))
		raw.Family = unix.AF_INET6
		raw.Port = htons(uint16(addr.Port))
		raw.Scope_id = addr.ZoneID
		copy(raw.Addr[:], addr.IP[:])
		return rsa, unix.SizeofSockaddrInet6, nil
	default:
		return rsa, 0, poller.ErrInvalidIORequest
	}
}

func socketAddressFromRaw(rsa *unix.RawSockaddrAny, n uint32) poller.SocketAddress {
	if rsa == nil || n == 0 {
		return poller.SocketAddress{}
	}
	switch rsa.Addr.Family {
	case unix.AF_INET:
		raw := (*unix.RawSockaddrInet4)(unsafe.Pointer(rsa))
		var addr poller.SocketAddress
		addr.Family = poller.SocketFamilyIPv4
		addr.Port = int(ntohs(raw.Port))
		copy(addr.IP[:4], raw.Addr[:])
		return addr
	case unix.AF_INET6:
		raw := (*unix.RawSockaddrInet6)(unsafe.Pointer(rsa))
		var addr poller.SocketAddress
		addr.Family = poller.SocketFamilyIPv6
		addr.Port = int(ntohs(raw.Port))
		addr.ZoneID = raw.Scope_id
		copy(addr.IP[:], raw.Addr[:])
		return addr
	default:
		return poller.SocketAddress{}
	}
}

func htons(v uint16) uint16 {
	return v<<8 | v>>8
}

func ntohs(v uint16) uint16 {
	return htons(v)
}

func (p *Poller) enter(toSubmit uint32, minComplete uint32, flags uint32) error {
	_, _, errno := unix.Syscall6(unix.SYS_IO_URING_ENTER, uintptr(p.fd), uintptr(toSubmit), uintptr(minComplete), uintptr(flags), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func (p *Poller) submitEnterFlags() uint32 {
	if p.sq.flags != nil && atomic.LoadUint32(p.sq.flags)&sqNeedWakeup != 0 {
		return enterSQWakeup
	}
	return 0
}

func (p *Poller) register(op uint32, arg unsafe.Pointer, nrArgs uint32) error {
	_, _, errno := unix.Syscall6(unix.SYS_IO_URING_REGISTER, uintptr(p.fd), uintptr(op), uintptr(arg), uintptr(nrArgs), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func (p *Poller) completionOverflowed() bool {
	if p.cq.overflow == nil {
		return false
	}
	current := atomic.LoadUint32(p.cq.overflow)
	if current == p.cqOverflowSeen {
		return false
	}
	p.cqOverflowSeen = current
	return true
}

func (p *Poller) nextUserData() uint64 {
	p.nextID++
	return p.nextID
}

func (p *Poller) sqe(index uint32) *sqe {
	size := unsafe.Sizeof(sqe{})
	return (*sqe)(unsafe.Pointer(&p.sq.sqes[uintptr(index)*size]))
}

func (p *Poller) cqe(index uint32) *cqe {
	size := unsafe.Sizeof(cqe{})
	return (*cqe)(unsafe.Pointer(uintptr(unsafe.Pointer(p.cq.cqes)) + uintptr(index)*size))
}

func uint32Ptr(mem []byte, off uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&mem[off]))
}

func unmapIfPresent(mem []byte) error {
	if len(mem) == 0 {
		return nil
	}
	return unix.Munmap(mem)
}
