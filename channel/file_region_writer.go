package channel

import "gnalloy.org/gnalloy/transport"

// FileRegionWriter 抽象 FileRegion 到 fd 的原生传输能力。
//
// 实现必须在成功写出字节后推进 region.Transferred，again 表示非阻塞 fd 暂不可写。
type FileRegionWriter interface {
	WriteFileRegion(fd transport.FDRef, region FileRegion) (n int64, again bool, err error)
}
