# 测试与性能

[English](testing.md) | [文档索引](README.zh-CN.md)

## 必跑检查

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -count=1
GOWORK=off GOTOOLCHAIN=local go vet ./...
gofmt -l .
git diff --check
```

## 聚焦行为检查

处理小范围行为变更时先跑聚焦测试：

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run 'TestName' -count=1
```

## 已发现测试入口

本清单从当前仓库的 `_test.go` 文件生成。这里刻意保持完整，用于在代码变化时发现 test、benchmark、fuzz 与 example 覆盖说明是否过期。

已发现入口总数：254。

### Tests（217）
- `TestAttributeMapTypedAccess`
- `TestBase64DecoderEmitsDecodedBuffer`
- `TestBase64DecoderReportsInvalidInput`
- `TestBase64EncoderWritesEncodedBufferAndReleasesInput`
- `TestBase64URLDialect`
- `TestByteBufEqualAndCompare`
- `TestByteSliceEncoderDecoder`
- `TestByteSliceEncoderReleasesOutputOnWriteError`
- `TestByteToMessageDecoderMergeCumulatorProducesContiguousFrame`
- `TestByteToMessageDecoderRejectsNilCumulator`
- `TestByteToMessageListDecoderEmitsMultipleMessages`
- `TestChannelOptionGetIfSetDistinguishesExplicitZero`
- `TestChannelOptionsTypedDefaultsAndSet`
- `TestChannelPoolMapCreatesOnePoolPerKey`
- `TestChunkedByteBufInputRejectsInvalidChunkSizeAndReleasesInput`
- `TestChunkedWriteHandlerReleasesChunkWhenWriteFails`
- `TestChunkedWriteHandlerWritesChunksAndFlushes`
- `TestCombinedChannelDuplexHandler`
- `TestCompletedPromiseListenerUsesConfiguredExecutor`
- `TestCompletionBatchHelpers`
- `TestCompositeAppendAfterReleaseDropsInput`
- `TestCompositeAppendRetainedKeepsCallerOwnership`
- `TestCompositeIndexFindsValuesAcrossComponents`
- `TestCompositeReadableSlicesAreViews`
- `TestCompositeReadableSlicesPreservePartialViews`
- `TestCompositeReadableSpan`
- `TestCompositeSliceAcrossComponents`
- `TestContiguousReadableBytesDetectsDirectAndSingleComposite`
- `TestCopyReadableBytesCopiesCompositeWithoutAdvancing`
- `TestCopyReadableBytesCopiesDirectWithoutAdvancing`
- `TestDelimiterBasedFrameDecoderAcrossBuffers`
- `TestDelimiterBasedFrameDecoderChoosesEarliestFrame`
- `TestDelimiterBasedFrameDecoderKeepsDelimiterWhenConfigured`
- `TestDelimiterBasedFrameEncoderAppendsDelimiterWithoutCopyingByteBufPayload`
- `TestDialerDialInitializesChannel`
- `TestDialerValidate`
- `TestDirectByteBufSliceRetainsParent`
- `TestEmbeddedChannelCapturesInboundMessages`
- `TestEmbeddedChannelCapturesOutboundAndFlush`
- `TestEmbeddedChannelCloseRejectsNewMessages`
- `TestEventLoopCloseClosesEventLoopLocalValues`
- `TestEventLoopCoalescesTaskWakeups`
- `TestEventLoopDispatchesPollEvent`
- `TestEventLoopGroupCPUAffinityMapping`
- `TestEventLoopGroupInvoke`
- `TestEventLoopGroupRegisterNext`
- `TestEventLoopGroupRejectsNegativeCPUAffinity`
- `TestEventLoopGroupRoundRobin`
- `TestEventLoopLocalCloseClosesValuesOnce`
- `TestEventLoopLocalCreatesOnceUnderConcurrency`
- `TestEventLoopLocalRejectsInvalidInputsAndClosedLocal`
- `TestEventLoopLocalReturnsOneValuePerLoop`
- `TestEventLoopSubmitTask`
- `TestFileRegionEncoderIgnoresEmptyRegion`
- `TestFileRegionEncoderWritesChunks`
- `TestFileRegionExposesNativeSourceAndAdvance`
- `TestFileRegionReadsConfiguredRange`
- `TestFillCompletionEventRecyclesPendingState`
- `TestFixedLengthFrameDecoderSplitAndSticky`
- `TestFixedLengthFrameEncoderForwardsExactByteBuf`
- `TestFixedLengthFrameEncoderRejectsWrongSizeAndReleasesInput`
- `TestFixedPoolLifecycleHandler`
- `TestFixedPoolQueuesAcquireUntilChannelReturned`
- `TestFixedPoolRejectsExcessPendingAcquire`
- `TestForEachReadableSliceVisitsCompositeWithoutAdvancing`
- `TestFrameDecodersStayAllocationFreeOnHotPath`
- `TestGroupAddRemoveAndSnapshot`
- `TestGroupFutureKeepsPerChannelErrors`
- `TestGroupHandlerTracksLifecycle`
- `TestGroupWriteEachFlushAndClose`
- `TestHeapAllocatorAcquireReleaseAllocationBudget`
- `TestHexDumpUsesReadableSlices`
- `TestIndexOfByteForwardAndReverse`
- `TestIndexOfBytesAndBytesBefore`
- `TestIORequestRetainBuffersKeepsDefaultRetainSemantics`
- `TestIORequestRetainBuffersRespectsOwnershipTransfer`
- `TestJsonObjectDecoderHandlesArrayAndStickyFrames`
- `TestJsonObjectDecoderHandlesSplitNestedObject`
- `TestJsonObjectDecoderRejectsInvalidStart`
- `TestJsonObjectDecoderReportsTooLongFrame`
- `TestLeakDetectorTracksSliceAndCompositeOwnership`
- `TestLeakDetectorTracksUnreleasedDirectBuffer`
- `TestLengthFieldDecoderFailFastReportsImmediately`
- `TestLengthFieldDecoderFailSlowReportsAfterDiscardingWholeFrame`
- `TestLengthFieldDecoderLengthIncludesHeaderViaAdjustment`
- `TestLengthFieldDecoderMergesFragmentedDefaultCumulation`
- `TestLengthFieldDecoderSplitFrame`
- `TestLengthFieldDecoderStickyFrames`
- `TestLengthFieldDecoderThreeByteLittleEndianWithOffset`
- `TestLengthFieldDecoderTooLong`
- `TestLengthFieldPrependerIncludesHeaderWidth`
- `TestLengthFieldPrependerRejectsOverflowAndReleasesInput`
- `TestLengthFieldPrependerReleasesPayloadWhenBodyWriteFails`
- `TestLengthFieldPrependerWritesHeaderThenPayload`
- `TestLineBasedFrameDecoderFailSlowReportsAfterDelimiter`
- `TestLineBasedFrameDecoderKeepsDelimiterWhenConfigured`
- `TestLineBasedFrameDecoderSplitCRLFAndLF`
- `TestLineBasedFrameDecoderTooLong`
- `TestLineEncoderWriteAndFlushPropagatesFlush`
- `TestLineEncoderWritesStringWithLineSeparator`
- `TestLoadDefaultMatrix`
- `TestLocalChannelWriteAndFlushFutureRunsOnBoundEventLoop`
- `TestLocalChannelWriteFutureReleasesFileRegionWhenOwnerLoopRejectsTask`
- `TestLocalChannelWriteFutureReleasesMessageWhenOwnerLoopRejectsTask`
- `TestMakeIOVectorsExpandsCompositeWithoutCopy`
- `TestMakeWriteBuffersExpandsCompositeWithoutCopy`
- `TestMakeWriteBuffersUsesInlineStorageForSingleBuffer`
- `TestMatrixRejectsDuplicateTargets`
- `TestMatrixRejectsEmptyGateCommand`
- `TestMessageToByteEncoder`
- `TestMessageToByteEncoderReleasesOutputOnWriteError`
- `TestMessageToMessageCodec`
- `TestMessageToMessageDecoder`
- `TestMessageToMessageEncoder`
- `TestMmapAllocatorAcquireReleaseAllocationBudget`
- `TestMmapAllocatorCloseRejectsInUseBuffers`
- `TestMmapAllocatorDirectDoubleReleaseDoesNotCorruptFreeList`
- `TestMmapAllocatorExposesFixedBuffers`
- `TestMmapAllocatorFixedBufferPointerStable`
- `TestMmapAllocatorRejectsInvalidAndOverflowSizes`
- `TestMmapAllocatorStatsTracksInUseAndClosed`
- `TestMmapAllocatorUnsupportedPlatform`
- `TestMPSCMultipleProducers`
- `TestMPSCOfferPoll`
- `TestMsgContextUsesInlineVectors`
- `TestNewOwnedBufferReleasesOwnerOnce`
- `TestNewOwnedBufferRetainedSliceKeepsOwner`
- `TestNewPollerSupportsBackendStd`
- `TestNewSharedBufferReadsWithoutCopying`
- `TestNewWithConfigRejectsInvalidSQPollAffinity`
- `TestNormalizeWriteBufferWatermark`
- `TestPendingRequestFreelistResetsState`
- `TestPendingRequestOverlappedRoundTrip`
- `TestPipelineAddBeforeAfterAndReplaceKeepOrder`
- `TestPipelineInboundPropagation`
- `TestPipelineLifecycleHandlers`
- `TestPipelineOutboundSink`
- `TestPipelineTailReleasesByteBuf`
- `TestPipelineWriteAndFlushDisablesDirectSinkAfterInboundReplace`
- `TestPipelineWriteAndFlushPreservesOutboundHandlers`
- `TestPipelineWriteAndFlushRestoresDirectSinkAfterOutboundRemove`
- `TestPipelineWriteAndFlushRestoresDirectSinkAfterOutboundReplace`
- `TestPipelineWriteAndFlushUsesDirectSinkWithoutOutboundHandlers`
- `TestPollRespectsModifyAndDeregister`
- `TestPollReturnsRegisteredReadiness`
- `TestPoolCloseAndDiscard`
- `TestPoolClosesUnhealthyAndExcessIdle`
- `TestPooledAllocatorCloseRejectsAcquireAndDropsRelease`
- `TestPooledAllocatorDropsWhenClassCacheFull`
- `TestPooledAllocatorIgnoresForeignBufferRelease`
- `TestPooledAllocatorOversizedBuffersAreNotCached`
- `TestPooledAllocatorReusesNearestSizeClass`
- `TestPooledAllocatorValidatesSizeClasses`
- `TestPoolGetPutReusesHealthyChannel`
- `TestPromiseListenersRunBeforeAndAfterCompletion`
- `TestPromiseListenersUseConfiguredExecutor`
- `TestPromiseTimeoutSuccessAndRemoveListener`
- `TestReadableStringAtCopiesCompositeRangeWithoutAdvancing`
- `TestReadUnsignedWithoutCopy`
- `TestRegisterBuffersStatsAndUnregister`
- `TestRegisterMmapAllocatorFixedBuffers`
- `TestReleaseHandlesBoolReturningRelease`
- `TestReleaseHandlesByteBuf`
- `TestReleaseHandlesPlainRelease`
- `TestReplayingDecoderDoesNotConsumeOnReplay`
- `TestReplayingDecoderWaitsForCompleteFrame`
- `TestServerBootstrapBindInitializesChild`
- `TestServerBootstrapValidate`
- `TestSharedBufferContiguousReadableBytes`
- `TestSharedBufferRejectsWrites`
- `TestSharedBufferSliceHasIndependentIndexes`
- `TestShouldStopAfterShortRead`
- `TestSimplePoolLifecycleHandler`
- `TestStatsReflectsMultishotAccept`
- `TestStringDecoderReleasesInput`
- `TestStringEncoderAndLengthPrependerOutboundOrder`
- `TestSubmitAcceptClosesAcceptedSocketOnImmediateFailure`
- `TestSubmitBatchCompletesPipeReadAndWrite`
- `TestSubmitBatchRejectsDuplicateOperationID`
- `TestSubmitBatchRollsBackRetainedBuffersOnPrepareError`
- `TestSubmitEnterFlagsWakeSQPollThread`
- `TestUnsafeCloseFiresInactiveOnce`
- `TestUnsafeCloseFutureCompletesOnInactive`
- `TestUnsafeCompletionAutoReadFalseRequiresManualRead`
- `TestUnsafeCompletionBatchesEchoWriteAndFollowUpRead`
- `TestUnsafeCompletionCloseBeforeRegisterFiresInactiveSynchronously`
- `TestUnsafeCompletionCloseFiresInactiveOnCloseCompletion`
- `TestUnsafeCompletionReadKeepsPendingBufferAliveUntilEvent`
- `TestUnsafeCompletionSubmitsGatheringWrite`
- `TestUnsafeCompletionWriteKeepsPendingBufferAliveUntilEvent`
- `TestUnsafeCompletionWriteUsesFixedBufferIndexForIOUring`
- `TestUnsafeFileRegionPartialWriteKeepsFuturePendingUntilDrain`
- `TestUnsafeFlushFastPathFiresCompleteWithoutWaiter`
- `TestUnsafeOptionCacheTracksSetAndRemove`
- `TestUnsafeOutboundPartialWrite`
- `TestUnsafeReadinessAutoReadFalseIgnoresReadableEventUntilManualRead`
- `TestUnsafeReadinessContinuesAfterTinyShortRead`
- `TestUnsafeReadinessHonorsMaxMessagesPerRead`
- `TestUnsafeReadinessHonorsWriteSpinCount`
- `TestUnsafeReadinessSkipsGatheringForSingleDirectBuffer`
- `TestUnsafeReadinessStopsAfterShortRead`
- `TestUnsafeReadinessUsesGatheringWrite`
- `TestUnsafeReadinessWriteInterestRespectsAutoReadFalse`
- `TestUnsafeWriteAndFlushBelowWatermarkDoesNotFireWritabilityChanged`
- `TestUnsafeWriteAndFlushDirectPartialQueuesRemainingBytes`
- `TestUnsafeWriteAndFlushDirectSmallBufferSkipsOutboundEntry`
- `TestUnsafeWriteAndFlushFileRegionUsesFileRegionWriter`
- `TestUnsafeWriteAndFlushUseNoPromiseFastPath`
- `TestUnsafeWriteFutureCompletesAfterDrain`
- `TestUnsafeWriteStaticBytesAndFlushDirectDrains`
- `TestUnsafeWriteStaticBytesAndFlushPartialQueuesRemainder`
- `TestWakeupConcurrent`
- `TestWakeupProducesWakeupEvent`
- `TestWheelCancel`
- `TestWheelScheduleAdvance`
- `TestWriteReadableBytesCopiesCompositeWithoutAdvancing`
- `TestWriteVectorContextReusesInlineStorage`

### Benchmarks（34）
- `BenchmarkBase64DecoderComposite`
- `BenchmarkBase64EncoderComposite`
- `BenchmarkByteSliceDecoderComposite`
- `BenchmarkByteToMessageListDecoder`
- `BenchmarkChannelPoolMapGet`
- `BenchmarkCompositeGetByteFragmented`
- `BenchmarkCompositeIndexByteFragmented`
- `BenchmarkCompositeReadableSlicesFullComponents`
- `BenchmarkCompositeReadableSlicesPartialComponents`
- `BenchmarkCopyReadableBytesComposite`
- `BenchmarkDelimiterBasedFrameDecoder`
- `BenchmarkDelimiterBasedFrameDecoderFragmented`
- `BenchmarkEventLoopSubmitBurst`
- `BenchmarkFileRegionEncoderChunks`
- `BenchmarkFixedLengthFrameDecoder`
- `BenchmarkFixedPoolGetPut`
- `BenchmarkHeapAllocatorAcquireRelease`
- `BenchmarkLengthFieldDecoder`
- `BenchmarkLineBasedFrameDecoder`
- `BenchmarkLineBasedFrameDecoderFragmented`
- `BenchmarkMakeIOVectorsComposite`
- `BenchmarkMakeWriteBuffersComposite`
- `BenchmarkMmapAllocatorAcquireRelease`
- `BenchmarkMPSCOfferPoll`
- `BenchmarkPipelineInboundNoop`
- `BenchmarkPipelineWriteAndFlushDirectSink`
- `BenchmarkPooledAllocatorAcquireRelease`
- `BenchmarkPooledAllocatorParallelAcquireRelease`
- `BenchmarkStringDecoderComposite`
- `BenchmarkUnsafeFileRegionDirectWriterDrained`
- `BenchmarkUnsafeVectorWriteAndFlushSingleDirectBufferDrained`
- `BenchmarkUnsafeWriteAndFlushDrained`
- `BenchmarkWheelScheduleAdvance`
- `BenchmarkWriteReadableBytesComposite`

### Fuzz Targets（3）
- `FuzzDelimiterBasedFrameDecoder`
- `FuzzLengthFieldBasedFrameDecoder`
- `FuzzLineBasedFrameDecoder`

### Examples（0）
- 当前没有声明 Example 函数。

## Race 检查

```bash
GOWORK=off GOTOOLCHAIN=local go test -race ./... -count=1
```

Race 检查对 core、transport、handler、resolver、observability、examples 和 benchmark 模块最有价值。平台相关 transport 可能需要原生主机能力。

## 基准测试

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run '^$' -bench . -benchmem -benchtime=1s -count=5
```

`ns/op`、`B/op`、`allocs/op`、throughput 和 p99 latency 要分开报告。每份结果都要包含 host 与 OS 信息。

## 压测

压测应针对真实装配栈运行。使用 `gnalloy.org/benchmarks` 维护可重复矩阵，使用 `gnalloy.org/examples` 运行客户端。warmup 和 measurement 阶段必须分离。

## CI

仓库 validation workflow 会在 Linux、macOS 与 Windows 上为 push 和 pull request 运行格式检查、测试和 vet。
