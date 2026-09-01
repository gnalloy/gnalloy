# Testing and Performance

[简体中文](testing.zh-CN.md) | [Docs Index](README.md)

## Required Checks

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -count=1
GOWORK=off GOTOOLCHAIN=local go vet ./...
gofmt -l .
git diff --check
```

## Focused Behavior Checks

Run focused tests while working on a small behavior change:

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run 'TestName' -count=1
```

## Discovered Test Entry Points

This inventory is generated from the current `_test.go` files in this repository. It is intentionally complete so documentation review can catch stale test, benchmark, fuzz, and example coverage when code changes.

Total discovered entry points: 254.

### Tests (217)
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

### Benchmarks (34)
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

### Fuzz Targets (3)
- `FuzzDelimiterBasedFrameDecoder`
- `FuzzLengthFieldBasedFrameDecoder`
- `FuzzLineBasedFrameDecoder`

### Examples (0)
- No Example functions are currently declared.

## Race Checks

```bash
GOWORK=off GOTOOLCHAIN=local go test -race ./... -count=1
```

Race checks are most valuable for core, transport, handler, resolver, observability, examples, and benchmark modules. Platform-specific transports may require native host capabilities.

## Benchmarks

```bash
GOWORK=off GOTOOLCHAIN=local go test ./... -run '^$' -bench . -benchmem -benchtime=1s -count=5
```

Report `ns/op`, `B/op`, `allocs/op`, throughput, and p99 latency separately. Include host and OS details with every result.

## Pressure Testing

Pressure tests should run against a realistic assembled stack. Use `gnalloy.org/benchmarks` for repeatable matrices and `gnalloy.org/examples` for runnable clients. Keep warmup and measurement phases separate.

## CI

The repository validation workflow runs formatting, tests, and vet on Linux, macOS, and Windows for pushes and pull requests.
