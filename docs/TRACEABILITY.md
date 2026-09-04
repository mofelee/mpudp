# MPUDP v0.1 Requirement Traceability

This matrix maps every numbered section in [MPUDP_REQUIREMENTS.md](MPUDP_REQUIREMENTS.md)
to the implementation surface, executable verification, and GitHub tracking history.
It is deliberately mechanism-oriented: exact promoted commit SHAs and hosted run IDs
belong in the delivery issue so this document does not become false after every commit.

Status meanings:

- `verified`: the implementation and named automated coverage exist in the repository;
- `implemented`: the persistent repository mechanism exists; event-based delivery evidence is recorded
  in the tracking issues rather than frozen into this versioned table;
- `deferred`: the behavior is deliberately outside v0.1 and has an explicit follow-up tracker.

<!-- mpudp-traceability:start -->
| Section | Requirement | Implementation | Verification | Tracking | Status |
|---:|---|---|---|---|---|
| 1 | 目标 | `Peer`/`Session`, `internal/fec`, `internal/scheduler`, `internal/transport` | `TestPeerLoopbackBidirectionalDatagrams`; canonical RS and NAT cases | #2, #4, #5, #6, #7, #8, #9, #11 | verified |
| 2 | 功能边界 | Public Datagram-only `Session`, FEC/scheduler/transport layers, and intentionally absent reliability/stream adapters | `TestPeerLoopbackBidirectionalDatagrams`; `TestPublicPeerDirectExchangePreservesBoundariesAndCloses`; `TestSendBlockContinuesAfterFailureAndReportsPartial`; `TestDocumentationIndexAndHygiene` | #2, #3, #4, #5, #6, #7, #9, #10 | verified |
| 3 | 最小配置 | `config.Parse`, `config.Decode`, `Config.Validate`, `config.Default` | `TestParseValidModesAndDefaults`; `TestParseRejectsMalformedAndUnknownYAML`; `TestPeerModesExposeOnlyConfiguredLifecycle` | #2, #11 | verified |
| 4 | Carrier | `transport.OpenCarrier`; `Carrier.Send`, `Rebuild`, and `Close` | `TestLoopbackCarriersUseDistinctLongLivedSourcePorts`; `direct-single-carrier` | #5, #7, #9 | verified |
| 5 | Peer 和 Session | `NewPeerContext`, `Peer.NewSession`, `Peer.Listener`, `Listener.Accept` | `TestPeerModesExposeOnlyConfiguredLifecycle`; `TestPeerDualModeRunsBothDirections`; `TestPeerMultipleConcurrentSessions` | #2, #6, #7 | verified |
| 6 | 动态地址和反向通信 | `listenerReplyPath` and the authenticated Endpoint pool | `TestLoopbackListenerReplyKeepsListeningSource`; `transparent-nat-reverse-path`; `endpoint-rebinding-and-expiry` | #5, #6, #8, #9 | verified |
| 7 | Endpoint Learning | Session Listener registry, Endpoint cap, and TTL expiry | `TestListenerAuthenticatesBeforeCreatingSessionOrEndpoint`; `endpoint-rebinding-and-expiry`; `auth-and-state-pollution` | #6, #7, #9 | verified |
| 8 | Keepalive | `Session.Advance` and per-path probe state | `TestPerCarrierKeepalivePONGMatchingAndRTT`; `endpoint-rebinding-and-expiry` | #6, #9 | verified |
| 9 | Reed-Solomon FEC | `fec.Params`, `Encoder`, and `Decoder` | `TestDecoderRS5_3RecoversEveryZeroOneOrTwoShardLoss`; `TestValidateParams` | #4, #6, #9 | verified |
| 10 | 一个 Datagram 一个 FEC Block | `fec.Encoder.Encode` and public `Session.WritePacket` | `TestEncoderEncodesCanonicalShards`; `TestPublicPeerDirectExchangePreservesBoundariesAndCloses` | #4, #7, #9 | verified |
| 11 | FEC 与 Carrier 数量独立 | `scheduler.Assign` | `TestAssignRSFiveAcrossTwoPathsRotatesThreeTwo`; `rs53-two-carrier-rotation` | #5, #9 | verified |
| 12 | Shard Scheduler | `scheduler.Assign` and `transport.SendBlock` | `TestAssignIsBalanced`; `TestAssignRotatesPathCoverage`; `TestSendBlockContinuesAfterFailureAndReportsPartial` | #5, #9 | verified |
| 13 | Shard 容错与 Carrier 容错 | FEC decoder threshold plus scheduler path mapping | `TestDecoderRS5_3RecoversEveryZeroOneOrTwoShardLoss`; `TestTwoCarrierRotationAndLossBoundaries` | #4, #5, #9 | verified |
| 14 | Packet ID 和解码 | FEC encoder/decoder PacketID and completion cache | `TestEncoderPacketIDExhaustionDoesNotWrap`; `TestDecoderKeysIsolateSessionsAndPacketIDs`; `TestDecoderLateShardsDoNotRedeliverWithinCompletionTTL` | #4, #6, #9 | verified |
| 15 | Datagram API 和语义 | Public `Session.WritePacket`, `ReadPacket`, and `Close` | `TestPeerLoopbackBidirectionalDatagrams`; `TestPeerDeliversDuplicatePacketIDOnlyOnce`; `TestLifecycleMethodsAreConcurrentSafe` | #2, #7, #9 | verified |
| 16 | Packet Types | `wire.PacketType`, constructors, and message bodies | `TestProtocolSizes`; `TestGoldenVectorsAndRoundTrips`; `TestRejectsInvalidMessages` | #3, #6 | verified |
| 17 | Wire Header 最低字段 | `wire` constants, encoder, and authenticated decoder | `TestNetworkByteOrderAndOffsets`; `TestGoldenVectorsAndRoundTrips`; `FuzzDecodeArbitrary`; `FuzzRoundTripBounded`; `FuzzSingleBitTamper` | #3, #11 | verified |
| 18 | PSK 认证 | `config.Secret`, full wire HMAC-SHA-256, and auth-first dispatch | `TestSecretNeverAppearsInFormattingErrorsOrYAML`; `TestAuthenticationPrecedesBodySemantics`; `auth-and-state-pollution` | #2, #3, #6, #7, #9 | verified |
| 19 | Session Bootstrap | Internal initiator/listener state machines and public Peer runtime | `TestInitiatorFirstACKEstablishesAndLaterACKAddsEndpoint`; `TestPeerLoopbackNegotiates1200And1000Bidirectionally`; canonical direct/NAT cases | #6, #7, #9, #11 | verified |
| 20 | Decode Timeout 和资源限制 | Config limits, bounded FEC/runtime queues, negotiated budget, and Linux PMTU | `TestDecoderHighCardinalityStateRemainsBounded`; `TestBoundedQueuesDropNewest`; `TestWritePacketExactDerivedLimitAndPMTUPathIsolation`; `mtu-budget-no-fragment` | #2, #4, #7, #8, #9, #11 | verified |
| 21 | 并发和关闭 | Peer dispatcher, synchronized Session/Listener/Carrier close paths | race suite; `TestPeerCloseStopsWorkerTimerAndSockets`; `TestPeerConcurrentCloseUnblocksReadAndAccept`; `shutdown-cleanup` | #5, #6, #7, #9, #11 | verified |
| 22 | MVP 测试 | CI workflow, case manifest, namespace harness, and nine canonical rows | `TestCICanonicalCasesMatchRunnableManifest`; all nine `integration / ...` checks | #2, #3, #4, #5, #6, #7, #8, #9, #11 | verified |
| 23 | v0.1 非目标 | Fixed public API, version-1 wire format, and deterministic scheduler surface | `TestDocumentationIndexAndHygiene`; incompatible extensions remain absent from v0.1 | #1, #2, #3, #5, #7, #11; follow-ups [#13](https://github.com/mofelee/mpudp/issues/13) and [#14](https://github.com/mofelee/mpudp/issues/14) | deferred |
| 24 | 完成标准 | CI, documentation contracts, redaction checks, and cleanup audit mechanisms | `TestRequirementsTraceabilityContract`; `TestDocumentedCIChecksAndCanonicalCasesMatchWorkflow`; `TestDocumentationIndexAndHygiene`; exact branch/promoted-main run evidence is recorded in #10 and #1 | #9, #10, #11 | implemented |
| 25 | 核心模型 | Aggregate public/runtime packages and their fixed ownership boundaries | `TestPeerLoopbackBidirectionalDatagrams`; `TestPublicPeerExchange`; canonical public Peer cases | #2, #3, #4, #5, #6, #7, #8, #9, #10, #11 | verified |
<!-- mpudp-traceability:end -->
