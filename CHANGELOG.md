# Changelog

All notable changes to this project are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added — financial-substrate primitives (Hedgehog FR-1 … FR-13)

A coordinated set of money-correctness, deterministic-replay, hard-safety,
regulatory-storage, real-world-I/O, and observability primitives requested by
downstream financial consumers (see `docs/feature-requests/requests-from-hedgehog.md`).
All changes are additive — core type changes are field-additive with defaults
or use `#[non_exhaustive]`, so existing code continues to compile.

- **FR-1 — `atomr-money` crate.** New exact-decimal `Money` / `Price` / `Qty` /
  `Currency` over `rust_decimal`, with **no** defaulted `f64` constructor
  (lossy `f64` ingestion is gated behind the off-by-default `f64-lossy`
  feature), **string** (never float) serde, and fully checked arithmetic
  (currency-mismatch / overflow return `MoneyError`). Property-tested for
  associativity and zero penny-drift.
- **FR-2 — Clock-gated streams.** New `atomr_core::time::{Clock, LogicalTime,
  ManualClock, SystemClock}` foundation; `atomr_streams::clock_gated` /
  `step_locked` gate emission on a logical-time watermark for lookahead-free
  point-in-time replay.
- **FR-3 — Real-world I/O + rate limiting.** New `atomr-streams-io` crate with
  `HttpPollSource` (conditional GET / ETag) and `WsSource` (reconnecting) behind
  `http` / `ws` features; `atomr_streams::rate::{token_bucket,
  token_bucket_keyed, respect_retry_after}` token-bucket operators.
- **FR-4 — `SignedSumMap` CRDT.** Signed i128-payload exposure CRDT in
  `atomr-distributed-data` with convergent per-node-max merge, replicated via
  the existing `Replicator`.
- **FR-5 — `ClusterKillSwitch`.** Latched, monotone (Flag-backed) cluster halt
  in `atomr-cluster-tools` with per-party acknowledged `await_quiescence`,
  `HaltGuarded`, and a two-person-authorized epoch `reset`. Quiescence spans the
  cluster: attach a `HaltTransport` (via `ClusterKillSwitch::with_transport`) and
  `await_quiescence` broadcasts a `HaltPdu::Halt` to every peer and collects
  remote `HaltPdu::Ack`s — peers engage their own latch and quiesce their local
  parties on receipt (feed inbound PDUs in via `ClusterKillSwitch::apply_pdu`).
  `QuiescenceReport` gains `remote_acked` / `remote_total`; with no transport the
  switch stays purely local.
- **FR-6 — Graded supervisor directives.** `Directive::{Throttle, Suspend,
  ResumeFrom}` + `SuspendMode`, an `Actor::on_directive` hook, and
  `ActorRef::tell_directive` to degrade a running actor without restart.
- **FR-7 — Role-weighted SBR + singleton fencing.** `RoleWeightedQuorum`
  downing strategy, `QuorumObserver` degradation hook, monotonic `FenceToken`,
  and a `SingletonHandoff` trait for clean external-session migration.
- **FR-8 — Bitemporal as-of queries.** `ReadJournal::events_as_of` /
  `events_valid_as_of`; `atomr-persistence-sql` adds `system_time` / `valid_time`
  columns + indexes (no-lookahead: later restatements invisible to earlier
  as-of reads).
- **FR-9 — WORM + transactional outbox.** `SqlJournal::with_worm` hash-chained
  tamper-evident journal mode + `IntegrityVerify::verify_chain`, and a
  same-transaction `TxOutbox` in `atomr-patterns` (distinct from the existing
  journal-tailing relay).
- **FR-10 — Trace/baggage propagation.** `MessageEnvelope` now carries
  `Metadata`; `ActorRef::tell_with_meta`, `Context::metadata()`, and a
  `Props::with_interceptor` (`MessageInterceptor`) hook propagate trace context
  without touching domain message types.
- **FR-11 — OTel span model.** `atomr-telemetry` gains `OtelTracerExporter` +
  `TraceContextInterceptor` for per-message-handle / supervision / stream-element
  spans correlated by trace_id, alongside the existing metrics exporter.
- **FR-12 — `atomr-fix` crate.** FIX session-layer FSM (logon, heartbeat /
  test-request, sequence management, resend / gap-fill, logout) over
  `atomr-streams` Tcp + Framing, with a pluggable `FixSeqStore`.
- **FR-13 — Record-and-replay determinism.** `SeededRng` (snapshot / restore /
  split), `EntryKind` provenance tagging, `RunPin` governance metadata, and a
  `ReplayHarness` in `atomr-persistence`.

## [0.3.1] — 2026-05-05

### Added
- **Native aarch64-Linux wheels.** PyPI now ships pre-built wheels
  for `aarch64-unknown-linux-gnu` and `aarch64-unknown-linux-musl`,
  built natively on GitHub-hosted ARM runners (`ubuntu-22.04-arm`).
  This closes the gap where ARM Linux users had to install from the
  source distribution and required a local Rust toolchain. The build
  is native (no cross-compile) so the prior `ring` / `aws-lc-rs`
  cross-build issue inside the manylinux container no longer applies.

  PyPI wheel coverage as of v0.3.1:

  | Platform              | Wheel |
  |-----------------------|-------|
  | linux-gnu x86_64      | ✓     |
  | linux-musl x86_64     | ✓     |
  | linux-gnu aarch64     | ✓ (new) |
  | linux-musl aarch64    | ✓ (new) |
  | macOS universal2      | ✓     |
  | windows-msvc x86_64   | ✓     |

## [0.3.0] — 2026-05-05

### Repositioned
- atomr is now framed as a standalone Rust + Python actor runtime.
  Migration / port framing has been removed from the codebase. The
  `migrating-from-akka-net.md` doc has been deleted, `PORTING.md` is
  now `docs/alignment-ledger.md`, and `PORTING_TODO.md` is now
  `docs/depth-roadmap.md`. Source-code attribution comments
  referring to specific `Akka.X.Y` types have been stripped from
  every `.rs` file (245 files swept). Wire-format identifiers
  (`AkkaPdu`, `AkkaProtocolTransport`, `akka.tcp://` URLs) are
  retained — those are protocol strings, not attributions.
- `xtask sync-upstream`, `scripts/sync-upstream.py`, and the
  `upstream-diff` CI job have been removed.

### Added — depth wave (phases A → FFF)
- **atomr-core.** `FsmBuilder` closure-DSL; `DispatcherConfig` with
  `throughput` / `throughput-deadline-time` knobs;
  `SingleThreadDispatcher`; bounded mailbox overflow strategies
  (`DropHead` / `DropTail` / `DropNew` / `Fail`); `ControlAwareQueue`;
  `ListenerRouter`; `ResizerConfig`; `DeadLetterReason` +
  `DeadLetterFilter`; coordinated-shutdown phase config + idempotent
  `run_from`; `TcpManager::Connect` outbound IO command.
- **atomr-testkit.** `expect_msg_eq`, `expect_msg_all_of_in_order`,
  `within(timeout, fn)` matchers; out-of-process
  `MultiNodeOopController` / `MultiNodeOopNode` TCP-rendezvous
  harness; `TestScheduler::cancel` returns false on re-cancel.
- **atomr-config.** HOCON `+=` array append; `Config::extract<T>` /
  `extract_root<T>` typed deserialize bridge.
- **atomr-cluster.** `LeaderHandover` watcher emitting
  `LeaderHandoverEvent`; `MemberWeaklyUp` event +
  `ClusterEvent::from_status_transition`; `Member::age_ordering`;
  monotonic `Reachability::Terminated`.
- **atomr-cluster-tools.** Distributed pubsub + cluster-singleton +
  cluster-client spec sweeps.
- **atomr-cluster-sharding.** Allocation + handoff spec sweep.
- **atomr-cluster-metrics.** `Ewma` (with `from_half_life`),
  `MetricsSelector` (Cpu / Heap / Mix), `WeightedRoutees`,
  `MetricsPdu` gossip transport, optional `sysinfo-probe` feature.
- **atomr-distributed-data.** `PruningState`, `WriteAggregator` /
  `ReadAggregator`, `OrSet::iter`, three-node convergence /
  CRDT-laws / map-CRDT / replicator-subscribe specs.
- **atomr-distributed-data-lmdb (NEW crate).** `RedbDurableStore`
  — a redb-backed `DurableStore` with single-writer / multi-reader
  / mmap semantics, durable across reopen.
- **atomr-persistence.** `Journal::events_by_tag` and
  `all_persistence_ids` defaults; in-memory backend overrides;
  Eventsourced integration / ALOD / PersistentFSM specs.
- **atomr-persistence-tck.** `journal_replay_edge_cases`,
  `snapshot_extended_suite`. Every storage backend now invokes the
  full TCK in CI.
- **atomr-persistence-query.** Backend-indexed `events_by_tag` with
  per-pid scan fallback; `all_persistence_ids` round-trip.
- **atomr-streams.** `split_after`, `prefix_and_tail`, `keep_alive`,
  `initial_delay`, `recover_with_retries`, `select_error`,
  `conflate`, `expand`, `merge_sorted`, `merge_prioritized`. Spec
  sweeps for flow / graph / hub / queue+restart / substream / rate.
- **atomr-remote.** `LruCache::peek` / `iter`; Reassembler stale-
  partial GC; endpoint-state + failure-detector specs.
- **atomr-discovery.** `AggregateDiscovery` provider chain.
- **atomr-coordination / atomr-di / atomr-hosting.** Full lease /
  service-container / builder-API spec coverage.
- **atomr-telemetry.** `TelemetryBus::subscribe_topic`,
  `TelemetryEvent::ALL_TOPICS`, full probe spec.
- **CI.** New real-service Postgres + MySQL jobs in the persistence
  integration matrix; new redb durable-store job.

### Added — Python coverage parity
Every Rust public surface added in the depth wave is now reachable
from Python via `atomr._native` extensions and `python/atomr/*.py`
modules. New Python modules:
- `atomr.core` — `DispatcherConfig`, `OverflowStrategy`,
  `BoundedStash`, `ControlAwareQueue`, `ResizerConfig`,
  `DeadLetterFilter`, Python-driven `FsmBuilder` / `Fsm`.
- `atomr.cluster_metrics` — `NodeMetrics`, `ClusterMetrics`, `Ewma`,
  `MetricsSelector`, `WeightedRoutees`, `AdaptiveLoadBalancer`.
- `atomr.ddata_lmdb` — `RedbDurableStore`.
- `atomr.telemetry` — `TelemetryBus`, `TopicSubscriber`, `all_topics`.

Existing modules also gained: cluster `LeaderHandover` /
`LeaderHandoverEvent` / `Member.age_ordering`; cluster-tools
`ClusterSingletonManager` / `ClusterReceptionist` /
`ClusterClientSettings`; ddata `PruningState` / `WriteAggregator` /
`ReadAggregator`; streams `keep_alive` / `initial_delay` /
`conflate` / `expand` / `merge_sorted` / `merge_prioritized` /
`split_after` / `prefix_and_tail` / `recover_with_retries` /
`select_error`; discovery `AggregateDiscovery`; testkit
`MultiNodeOopController` / `MultiNodeOopNode` /
`expect_msg_eq` / `expect_msg_all_of_in_order` / `within`;
persistence `events_by_tag` / `all_persistence_ids`; config
`Config.extract` / `extract_root`.

`Extensions` (TypeId-keyed) and `ListenerRouter` (typed-Rust
generic) are intentionally Rust-only — Python cannot satisfy the
required type-identity / generic constraints.

### Test totals
- 546 workspace lib tests.
- 200+ integration / spec tests across 30+ spec files.
- 76 Python tests (45 new + 31 pre-existing).

### Fixed
- `pyproject.toml`: explicitly include `LICENSE` in the maturin sdist so
  PyPI's strict `License-File` metadata check passes. The `0.1.0` PyPI
  upload published all four wheels but rejected the sdist with
  `400 License-File LICENSE does not exist in distribution file`.
  Wheels cover Linux x64 (manylinux + musllinux), macOS universal2, and
  Windows x64 — sdist install (e.g. aarch64 Linux) requires the next
  release.

## [0.1.0] — 2026-05-03

### Changed
- **Project rename: `rakka` → `atomr`.** The umbrella crate, every
  `rakka-*` workspace member, the Python package, the binary names
  (`atomr-dashboard`, `atomr-profiler`), and the AI-skills plugin all
  ship under the `atomr` name. The repository moves from
  `github.com/rustakka/rakka` to `github.com/rustakka/atomr` (the GitHub
  redirect keeps old links working). The publish-name reset to `0.1.0`
  reflects that this is a new identity on crates.io and PyPI.

### Added
- `ai-skills/` — vendor-neutral skill bundle for AI coding assistants
  working on **projects that depend on atomr**. Six skills covering
  actor design, testing, troubleshooting, cluster, persistence, and
  Python bindings. Distributed alongside the repo; does not affect
  atomr's internal development workflow.
- Project hygiene: `LICENSE` (Apache-2.0), `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, `SECURITY.md`, GitHub issue + PR templates,
  Dependabot configuration.

### Pre-rename history (published as `rakka-rs` / `rakka-*`)

The releases below were published under the prior `rakka-rs` and
`rakka-*` crate names, and as the `rakka` PyPI package. They remain
installable from the registries but are not maintained going forward;
new development happens under the `atomr` name.

#### [0.2.1] — 2026-04

##### Changed
- Renamed umbrella crate to `rakka-rs` on crates.io (Cargo's `package =`
  alias kept the import name `rakka`). The short name `rakka` was owned
  by an unrelated, dormant crate.
- Developer-experience polish — umbrella feature flags, crate metadata,
  CI docs flow.

##### Fixed
- CI release pipeline: throttle crates.io publishes and retry on
  rate-limit 429s.
- Correct full crates.io publish order in dependency order.
- Restrict PyPI upload to Python artifacts only.

#### [0.2.0]

##### Added
- Full Akka.NET parity sweep — every subsystem the umbrella claims has
  a working Rust implementation: core, supervision, dispatch, mailboxes,
  FSM, event stream, coordinated shutdown, remote, cluster,
  cluster-tools, cluster-sharding, cluster-metrics, distributed-data,
  persistence (with sql / redis / mongodb / cassandra / aws / azure
  adapters), persistence-query, streams, coordination, discovery, di,
  hosting, telemetry, dashboard.
- First-class Python bindings (`pip install rakka`) — Actor base class,
  async + blocking ask/tell, dispatcher strategies (`python-pinned`,
  `python-subinterpreter-pool`, `python-nogil`, `python-subprocess`),
  C-extension compatibility registry.
- Cross-runtime profiler — `cargo run -p rakka-profiler` and
  `python -m rakka.profiler` emit a shared JSON schema so Rust and
  Python paths can be compared directly.
- Release pipeline: tag-driven publishes to crates.io and PyPI, GitHub
  Releases with `rakka-dashboard` + `rakka-profiler` binaries for Linux
  x86_64 and macOS aarch64.

##### Changed
- Project rename from `rustakka` → `rakka` across crates, modules,
  documentation, and the published Python package.

[Unreleased]: https://github.com/rustakka/atomr/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rustakka/atomr/releases/tag/v0.1.0
[0.2.1]: https://github.com/rustakka/atomr/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/rustakka/atomr/releases/tag/v0.2.0
