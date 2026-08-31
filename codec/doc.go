// Package codec 提供可组合到 ChannelPipeline 的编解码 Handler。
//
// 当前公共面覆盖 Netty codec 的基础层：ByteToMessageDecoder、Composite/Merge
// cumulator、MessageToByteEncoder、MessageToMessageDecoder/Encoder/Codec、组合式双工
// Handler、长度字段、固定长度、行分隔符、任意分隔符、长度字段前置编码器、line/fixed/
// delimiter 出站编码器、string 与 []byte 编解码器。
// Frame decoder 通过 ByteBuf.Slice 传递零拷贝视图。下游 Handler 负责在消费完帧后 Release。
package codec
