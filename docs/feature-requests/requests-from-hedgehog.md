# Upstream Feature Requests — rustakka/atomr

> Filed by the Hedgehog agentic-hedge-fund project. hedgehog (a regulated, auditable, real-money agentic hedge fund) sits on atomr as its L1 substrate: entity actors, event-sourced journals/snapshots, clustering + sharding, distributed-data CRDTs, reactive streams, and telemetry. These requests were verified against the actual **atomr 0.9.2** source extracted in the local cargo registry (`atomr-core`, `atomr-cluster` (+tools/sharding), `atomr-streams`, `atomr-distributed-data`, `atomr-persistence` (+sql/query), `atomr-patterns`, `atomr-telemetry`, `atomr-testkit`) — not just design assumptions. The dominant theme: atomr is an excellent generic actor/streaming substrate but lacks the money-correctness, deterministic-replay, hard-safety (latched halt, graded breaker, quorum-gated singletons), regulatory-storage (WORM/tamper-evidence/bitemporal), real-world-I/O (HTTP/WS/poll/FIX), and end-to-end observability (trace propagation + span model) primitives that a books-and-records financial system requires. Several originally-suspected gaps were refined by reading source: a metrics-only OTel exporter exists but no span/trace model; a journal-tailing outbox relay exists but not an atomic same-transaction outbox; a virtual-time `TestScheduler` exists but only in testkit; SBR has five strategies but none role-weighted; `PNCounter` is u64-delta/i64-value with no decimal payload.

> **Context:** Hedgehog (a downstream consumer's internal implementation plan) is a fully-agentic, regulated, real-money hedge fund. Each FR below cites the exact current API, proposes a concrete addition, and gives a hedgehog-side fallback if upstream declines — so **no FR is a hard blocker**.

---

## FR-1: Canonical exact-decimal `Money`/`Decimal` primitive in atomr-core (+ decimal-safe persistence column) — **P0**

**Motivation (hedge-fund use case).** hedgehog needs exact monetary arithmetic for `CapitalMandate` notionals, PnL/Greeks measures feeding `GateCriterion`s, NAV/GL, OMS fills, fees and financing accrual. Six of the design's gaps reduce to "no Money type". Multiple L4 crates (`hedgehog-domain`, `hedgehog-risk`, `hedgehog-deploy`, `hedgehog-persist`) would each reinvent it inconsistently, which is a regulatory and P&L-correctness defect.

**Current behavior / gap.** atomr-core has NO monetary or decimal type at all. The only money-ish type in the whole atomr/atomr-agents ecosystem is `atomr-agents-core`'s `MoneyBudget { remaining_micro_usd: u64 }`, constructed via `MoneyBudget::from_usd(usd: f64)` = `(usd * 1_000_000.0) as u64` — a lossy f64→u64 conversion, single-currency, budget-only, and not in the substrate crate. `atomr-persistence-sql` stores event payloads as opaque serialized bytes with no decimal/NUMERIC column helper; nothing prevents a consumer from serde-serializing f64 money into the journal.

**Proposed API / behavior.** New `atomr-money` crate (or `atomr_core::money`) wrapping `rust_decimal::Decimal` (128-bit) behind a domain-safe API, with optional i128 minor-units backend:
```rust
pub struct Currency { pub code: [u8;3], pub minor_units: u8 } // ISO 4217
pub struct Money { amount: Decimal, currency: Currency } // NOT Copy; no f64 ctor
impl Money {
  pub fn new(amount: Decimal, ccy: Currency) -> Self;
  pub fn from_minor(units: i128, ccy: Currency) -> Self;
  pub fn checked_add(self, o: Self) -> Result<Self, MoneyError>; // errors on ccy mismatch / overflow
  pub fn checked_sub(self, o: Self) -> Result<Self, MoneyError>;
  pub fn checked_mul_scalar(self, q: Decimal) -> Result<Self, MoneyError>;
  pub fn round(self, mode: RoundingMode) -> Self; // Banker's/HalfUp/etc
  pub fn to_minor(self) -> i128;
}
pub struct Price(Decimal); pub struct Qty(Decimal); // tick/lot-aware helpers
impl Price { pub fn round_to_tick(self, tick: Decimal) -> Self; }
```
No `From<f64>`; only an explicit `try_from_f64_lossy` gated behind a feature, never in the default path. serde uses string serialization (never float). Plus an `atomr-persistence-sql` helper: `Column::decimal(name)` mapping to NUMERIC/DECIMAL, and a `DecimalCodec` so money aggregates store as exact text/NUMERIC, never BINARY-float.

**Acceptance criteria.**
- A `Money`/`Decimal` type exists in an atomr substrate crate with no defaulted f64 constructor and serde string (not float) (de)serialization.
- All arithmetic is checked: currency-mismatch and overflow return errors, never panic or silently truncate.
- Property tests demonstrate associativity/round-trip and absence of penny drift over 10^6 random ops.
- `atomr-persistence-sql` exposes a NUMERIC/DECIMAL column helper and a Money codec; an example persists and recovers a money aggregate bit-exactly.
- PyO3 binding exposes Money/Price/Qty with Decimal-backed (string) interop, no float coercion.

**Fallback if declined.** hedgehog builds `hedgehog-domain` Money over `rust_decimal` itself and a custom `DecimalCodec` for `atomr-persistence-sql`. Workable but duplicated across every fund and not shareable; the substrate remains f64-unsafe for any other financial consumer.

---

## FR-2: Logical-clock-gated atomr-streams Source operator for deterministic point-in-time replay — **P1**

**Motivation (hedge-fund use case).** `hedgehog-backtest` replays historical data through agents whose LLM inference is async and variable-latency. Replay MUST be throttled by the DES logical clock so agents cannot outrun the clock and peek at future bars (lookahead bias). This is the core determinism invariant of the backtester.

**Current behavior / gap.** Every time-based stream operator is wall-clock: `Source::throttle(interval)`, `Source::delay`, `Source::tick`, `timed::grouped_within`, `timed::idle_timeout`, `timed::keep_alive` all use `tokio::time::{Instant, sleep_until}`. A virtual-time clock exists ONLY in `atomr-testkit` (`TestScheduler::{advance_by, advance_to, now}`, which drives Tokio's paused clock) — there is no production path to drive a Source from an external/logical clock, and no way to gate emission on a consumer-acknowledged sim-time watermark.

**Proposed API / behavior.** A pluggable clock trait plus a clock-gated source operator in atomr-streams:
```rust
pub trait Clock: Send + Sync { fn now(&self) -> LogicalTime; }
pub struct ManualClock { /* advanced explicitly */ }
impl ManualClock { pub fn advance_to(&self, t: LogicalTime); pub fn watermark(&self) -> LogicalTime; }

// Emits element e only once clock.watermark() >= e.event_time(); applies
// normal backpressure, and will NOT pull the next element until the
// downstream has signalled completion of the current logical instant.
pub fn clock_gated<T, F>(
    src: Source<T>,
    clock: Arc<dyn Clock>,
    event_time: F,           // Fn(&T) -> LogicalTime, must be monotonic non-decreasing
) -> Source<T> where F: Fn(&T) -> LogicalTime + Send + 'static;

// Coupling so async consumers ACK an instant before the clock advances:
pub fn step_locked<T>(src: Source<T>, clock: Arc<ManualClock>) -> (Source<(T, InstantToken)>, AckSink);
```
Key guarantee: with `ManualClock`, no element with `event_time > watermark` is ever observable downstream, regardless of consumer async latency.

**Acceptance criteria.**
- A `Clock` trait + `ManualClock` are exposed from atomr-streams (or atomr-core) usable in production, not just testkit.
- `clock_gated` provably never emits an element whose event_time exceeds the current watermark (test with artificially slow async consumer).
- Backpressure still composes: a slow downstream halts upstream pull without busy-spinning.
- An example wires a ManualClock-driven source through a `map_async` agent step and shows the agent cannot observe future elements.

**Fallback if declined.** hedgehog builds a clock-gated source in `hedgehog-backtest` using `Source::unfold` + an `Arc<ManualClock>` it advances from the DES Simulator, plus an explicit per-instant ack channel. Feasible (testkit's TestScheduler shows the pattern) but every reactive-finance user re-implements the lookahead guard.

---

## FR-3: Reusable atomr-streams I/O Source/Sink adapters (HTTP poll, WebSocket) + token-bucket rate-limit operator — **P1**

**Motivation (hedge-fund use case).** `hedgehog-sources` must ingest broker/market-data feeds and fair-access-limited HTTP endpoints (e.g. EDGAR: 10 req/s/IP, mandatory User-Agent, 403+ban on breach). Every connector and rate limiter is currently net-new; a reusable streams adapter + token-bucket belongs upstream so backpressure and limiting are uniform and correct.

**Current behavior / gap.** atomr-streams ships only `Tcp` (`OutgoingConnection`/`IncomingConnection`) and `FileIO`. There is no HTTP/WS/long-poll Source adapter. Rate control is limited to `Source::throttle(interval)` (fixed inter-element delay) and `rate::conflate`/`rate::expand` — there is no token-bucket / leaky-bucket operator with burst capacity, no per-key limiter, and no 429/Retry-After-aware backoff.

**Proposed API / behavior.** Optional features `streams-http` / `streams-ws` plus a rate operator:
```rust
pub struct HttpPollSource;
impl HttpPollSource {
  pub fn new(req: RequestSpec, every: Duration) -> Source<Result<HttpResponse, HttpError>>;
  pub fn with_etag(self) -> Self; // conditional GET / If-None-Match
}
pub struct WsSource;
impl WsSource { pub fn connect(url: Url, on_reconnect: Backoff) -> Source<Result<WsFrame, WsError>>; }

// Token-bucket / leaky-bucket operator with burst + per-key support:
pub fn token_bucket<T>(src: Source<T>, rate_per_sec: f64, burst: u32) -> Source<T>;
pub fn token_bucket_keyed<T, K, F>(src: Source<T>, key: F, rate_per_sec: f64, burst: u32) -> Source<T>
    where K: Eq + Hash, F: Fn(&T) -> K;
// Honors upstream-supplied limits:
pub fn respect_retry_after<T>(src: Source<Result<HttpResponse,HttpError>>) -> Source<Result<HttpResponse,HttpError>>;
```
Reconnect/backoff reuses the existing `RestartSource`/`RestartSettings`.

**Acceptance criteria.**
- HTTP-poll and WebSocket Sources exist behind feature flags, integrate with existing backpressure (`buffer`, `OverflowStrategy`) and `RestartSource` reconnect.
- `token_bucket` enforces sustained rate + burst with a property test that never exceeds rate over any window; keyed variant limits per key independently.
- A `respect_retry_after` (or equivalent) operator parses 429/Retry-After and pauses the affected key without dropping elements.
- An example demonstrates a 10 req/s EDGAR-style limited poller with a mandatory User-Agent header that never breaches the limit.

**Fallback if declined.** `hedgehog-sources` wraps reqwest/tokio-tungstenite in `Source::from_receiver`/`unfold` and writes its own token-bucket. Straightforward but every atomr user rebuilds connectors and limiters with subtly different backpressure semantics.

---

## FR-4: Signed, decimal/i128-payload exposure CRDT with bounded merge (`SignedSumMap`) — **P1**

**Motivation (hedge-fund use case).** hedgehog needs firm-wide live net signed exposure (longs minus shorts), per-factor and per-sector exposures, computed convergently across sharded `StrategyActor`s without a synchronous cross-shard read, for pre-trade aggregate-limit checks and the risk dashboard.

**Current behavior / gap.** `atomr-distributed-data` `PNCounter` takes `u64` deltas (`increment/decrement(node, delta: u64)`) and yields `value() -> i64`; `PNCounterMap` is a map of these. There is no decimal or i128 payload, no per-entry signed value type, and no bounded/clamped merge (e.g. saturating at a configured ceiling). Money exposures expressed through a u64-delta counter risk overflow and cannot carry currency/decimal precision.

**Proposed API / behavior.** A new CRDT alongside `PNCounterMap`:
```rust
// Convergent signed sum keyed by node, payload is i128 minor-units or Decimal.
pub struct SignedSum { /* per-node {pos:i128, neg:i128} */ }
impl SignedSum {
  pub fn add(&mut self, node: &str, delta: i128); // delta may be negative
  pub fn value(&self) -> i128;                     // pos.sum - neg.sum, checked
}
pub struct SignedSumMap<K: Ord> { /* K -> SignedSum */ }
impl<K: Ord + Clone> SignedSumMap<K> {
  pub fn add(&mut self, key: K, node: &str, delta: i128);
  pub fn get(&self, key: &K) -> i128;
  pub fn entries(&self) -> impl Iterator<Item=(&K, i128)>;
}
impl CrdtMerge for SignedSumMap<K> { /* per-node max of pos/neg legs => monotonic, commutative */ }
```
Payload generic over `i128` (default) or `Decimal` for currency-precise exposures; merge is the standard PN per-node-max so it is associative/commutative/idempotent.

**Acceptance criteria.**
- A signed-sum CRDT with i128 (and optionally Decimal) payload merges convergently (property test: any interleaving of adds across N replicas yields identical value).
- Per-key map variant supports per-factor/per-sector exposures with O(keys) merge.
- Overflow is checked (returns/clamps rather than wraps); negative deltas are first-class.
- Replicator integration: works through the existing `Replicator`/`ReplicatorActor` like `PNCounterMap`.

**Fallback if declined.** hedgehog represents each exposure as a pair of PNCounters scaled to fixed minor-units and reconstructs the signed value, accepting u64 overflow risk and loss of currency tagging; or implements `CrdtMerge` for a custom `SignedSumMap` in `hedgehog-risk` (the trait is public).

---

## FR-5: Cluster-wide latched, acknowledged kill-switch with delivery guarantees and unified halt semantics — **P0**

**Motivation (hedge-fund use case).** A firm-wide emergency stop must reach EVERY `StrategyActor` and the Execution gateway reliably, stay latched through split-brain/rebalance, and have ONE authoritative semantics. This is the load-bearing safety primitive for a real-money fund; the current design has multiple uncoordinated mechanisms (streams `KillSwitch` vs a `LWWMap`/`Flag`) with contradictory targets.

**Current behavior / gap.** Three unrelated mechanisms exist and none is a cluster-wide latched halt: (1) atomr-streams `KillSwitch` is per-materialized-graph local (`shutdown()/abort()` on one stream); (2) `atomr-cluster-tools` `DistributedPubSub::publish_msg -> usize` and `ClusterPubSub` fan-out are best-effort with no acks, no redelivery, no late-subscriber catch-up; (3) `atomr-distributed-data` `Flag` latches monotonically true but has no delivery/ack guarantee and no enforcement that side-effecting actors observe it before acting. Nothing ties these into one "halt the firm" with confirmed reach.

**Proposed API / behavior.** A first-class latched cluster halt built on the `Flag` CRDT + acked delivery + barrier:
```rust
pub struct ClusterKillSwitch { /* backed by a monotonic Flag in ddata */ }
impl ClusterKillSwitch {
  pub fn engage(&self, reason: HaltReason) -> HaltToken; // latches; survives rebalance/SBR
  pub fn is_engaged(&self) -> bool;
  pub async fn await_quiescence(&self, timeout: Duration) -> QuiescenceReport; // all guarded actors ACKed
  pub fn reset(&self, authz: ResetAuthorization);       // explicit two-person reset only
}
// Actors opt into the barrier; guarded sends are rejected once engaged:
pub trait HaltGuarded { fn on_halt(&mut self, reason: &HaltReason); }
pub fn guard_sink<T>(sink: Sink<T>, ks: ClusterKillSwitch) -> Sink<T>; // drops/rejects after engage
```
Guarantees: (a) monotone latch (engaged state never lost on merge), (b) per-node ACK so `await_quiescence` confirms every reachable guarded actor saw it, (c) a minority partition that loses quorum auto-engages locally (compose with FR-7), (d) one type is THE halt — streams `KillSwitch` becomes a downstream subscriber.

**Acceptance criteria.**
- Engaging the switch latches firm-wide and remains engaged across a simulated rebalance and split-brain heal.
- `await_quiescence` returns only after every reachable guarded actor/sink has acknowledged and stopped emitting orders (test with injected slow node).
- A single documented type is the authoritative halt; streams `KillSwitch` and execution gateway both derive from it (no contradictory targets).
- Reset requires explicit authorization and is itself observable/auditable (emits an event).

**Fallback if declined.** `hedgehog-walls` builds the latch on the existing `Flag` CRDT + a hand-rolled ack protocol over `DistributedPubSub`, and wraps the execution sink with `guard_sink` locally. Achievable but the ack/quiescence and partition-auto-engage guarantees are exactly the error-prone safety code best owned upstream.

---

## FR-6: Graded supervisor directives — `Throttle` and `Suspend` (risk-reducing-only) beyond Resume/Restart/Stop/Escalate — **P1**

**Motivation (hedge-fund use case).** Risk-as-circuit-breaker needs graded responses: Throttle (reduce order rate/size) and Suspend-to-flat-only (accept only risk-reducing actions), not just kill or restart a `StrategyActor`. The breaker must degrade behavior, not bounce the actor.

**Current behavior / gap.** atomr-core `supervision::Directive` is exactly `{ Resume, Restart, Stop, Escalate }`. `SupervisorStrategy`/`OneForOneStrategy`/`AllForOneStrategy` deciders map an error string to one of those four. There is no Throttle/Suspend directive and no supported lifecycle hook for a supervisor to push a graded operating-mode change into a still-running child.

**Proposed API / behavior.** Extend the directive set and add a mode-change hook:
```rust
pub enum Directive {
  Resume, Restart, Stop, Escalate,
  Throttle { factor: f32, window: Duration }, // e.g. 0.25 = quarter rate/size
  Suspend  { mode: SuspendMode },             // FlatOnly | RiskReducingOnly | FullHalt
  ResumeFrom(SuspendMode),                     // step back up the ladder
}
// Child observes graded changes without restart:
#[async_trait]
pub trait Actor {
  async fn on_directive(&mut self, ctx: &mut Context<Self>, d: &Directive) {} // default no-op
}
```
The child's `on_directive` is invoked when the supervisor decides Throttle/Suspend/ResumeFrom, letting it gate its own outbound sends; Resume/Restart/Stop/Escalate keep current semantics. This maps cleanly to hedgehog's six-phase transition ladder and to atomr-orgs Gate rollbacks.

**Acceptance criteria.**
- `Directive` gains Throttle/Suspend/ResumeFrom variants without breaking existing deciders (additive enum; matches stay exhaustive via a documented migration).
- A supervisor can push a graded mode change to a running child via `on_directive` with no actor restart (state preserved).
- An example circuit-breaker throttles then suspends-to-flat-only a worker on rising error/latency signals and resumes on recovery.
- Directive transitions are observable for audit (emit a supervision event).

**Fallback if declined.** hedgehog encodes breaker state as ordinary domain messages (e.g. `RiskMode::FlatOnly`) sent to `StrategyActor`s and ignores supervision for grading, using Restart/Stop only for crashes. Works but splits breaker logic from the supervision tree and loses the unified lifecycle/audit point.

---

## FR-7: Role-weighted quorum split-brain resolution with side-effecting-singleton fencing and application degradation hook (+ singleton failover handoff) — **P0**

**Motivation (hedge-fund use case).** With real money, a partition must provably prevent two minority sides from both owning the broker session or running the limit engine. hedgehog pins risk+ledger to `core` nodes and needs (a) core-weighted quorum, (b) a guarantee that a minority side halts its side-effecting singletons, (c) a hook to drive fail-safe-flat on quorum loss, and (d) an application-level failover-counterparty handoff when a singleton (prime-broker session) moves.

**Current behavior / gap.** `atomr-cluster` `sbr` offers KeepMajority/StaticQuorum/KeepOldest/KeepReferee/LeaseMajority/DownAll implementing `DowningStrategy::decide -> DowningDecision`. None is role/weight-aware (all members count equally), there is no documented "minority must stop side-effecting cluster singletons" guarantee, and no callback to degrade the application on quorum loss. `atomr-cluster-tools` `ClusterSingletonManager` has a `begin_handover`/`begin_starting`/`set_active_*` state machine but exposes no application hook to hand off external-counterparty session state (e.g. drain/port a broker connection) during failover.

**Proposed API / behavior.** Role-weighted SBR + fencing + degradation hook, plus a singleton handoff trait:
```rust
pub struct RoleWeightedQuorum { pub weights: HashMap<Role, u32>, pub min_quorum_weight: u32 }
impl DowningStrategy for RoleWeightedQuorum { /* survive iff sum(weights of reachable up) >= min */ }

pub trait QuorumObserver: Send + Sync {
  fn on_quorum_lost(&self);  // hedgehog: engage ClusterKillSwitch + fail-safe-flat
  fn on_quorum_regained(&self);
}
impl SplitBrainResolver { pub fn with_observer(self, o: Arc<dyn QuorumObserver>) -> Self; }

// Documented guarantee: a side downed by SBR stops its cluster singletons
// BEFORE the surviving side starts them (fencing token monotonic).
pub trait SingletonHandoff: Send + 'static {
  async fn prepare_handoff(&mut self) -> HandoffState; // drain/serialize external session
  async fn assume(&mut self, prior: Option<HandoffState>, fence: FenceToken);
}
impl ClusterSingletonManager { pub fn with_handoff<H: SingletonHandoff>(self, h: H) -> Self; }
```

**Acceptance criteria.**
- A role/weight-aware downing strategy exists; a partition where the minority holds more raw nodes but less core weight correctly downs the minority.
- Documented and tested guarantee: a downed side stops its side-effecting singletons (fenced) before the survivor starts them — no double-ownership window.
- A `QuorumObserver`/degradation callback fires on quorum loss/regain and can drive an application halt.
- `SingletonHandoff` lets the prime-broker singleton drain/serialize and the new holder assume with a monotonic fence token; example shows clean session port.

**Fallback if declined.** hedgehog implements `DowningStrategy` for `RoleWeightedQuorum` itself (trait is public) and polls cluster membership events to trigger its own kill-switch and broker-session drain. The fencing-before-start guarantee is hard to achieve correctly without runtime cooperation, so this remains the riskiest workaround.

---

## FR-8: Bitemporal / as-of query semantics on the Journal (valid-time + system-time, no-lookahead replay) — **P2**

**Motivation (hedge-fund use case).** Point-in-time correctness — no lookahead, correct handling of restatements/late-arriving corrections — is the single most important data-plane invariant for a hedge fund. hedgehog must answer "what did we know about time T as of decision time D" for every input feeding a strategy or risk calc.

**Current behavior / gap.** `atomr-persistence` `PersistentRepr` carries `{ payload, sequence_nr: u64, manifest, writer_uuid, tags: Vec<String> }` — a single sequence axis, no valid-time or system-time. `atomr-persistence-query` `Offset` is sequence-only (`as_sequence -> Option<u64>`); `ReadJournal` exposes `current_events_by_*`/tail-following variants but no `as_of(system_time)` or `valid_as_of(valid_time)` query. Restatements can only be appended, with no first-class way to query the world-state as known at a past instant.

**Proposed API / behavior.** Add optional temporal axes to `PersistentRepr` and as-of reads:
```rust
pub struct PersistentRepr {
  pub payload: Vec<u8>, pub sequence_nr: u64, pub manifest: String,
  pub writer_uuid: String, pub tags: Vec<String>,
  pub valid_time: Option<Timestamp>,  // when the fact is true in the world
  pub system_time: Timestamp,         // when atomr recorded it (set by backend)
}
pub enum Offset { Sequence(u64), SystemTime(Timestamp), ValidTime(Timestamp) }
#[async_trait] pub trait ReadJournal {
  async fn events_as_of(&self, pid: &str, system_time: Timestamp) -> Result<Vec<EventEnvelope>, JournalError>;
  async fn events_valid_as_of(&self, pid: &str, valid_time: Timestamp, system_time: Timestamp)
      -> Result<Vec<EventEnvelope>, JournalError>; // bitemporal slice
}
```
`atomr-persistence-sql` adds `valid_time`/`system_time` columns + indexes; `events_as_of` reconstructs aggregate state excluding anything recorded after `system_time` (the no-lookahead guarantee).

**Acceptance criteria.**
- `PersistentRepr`/`EventEnvelope` optionally carry valid_time + system_time; system_time is backend-assigned and monotonic.
- `events_as_of(system_time)` returns exactly the events recorded at or before that instant — a later-recorded restatement is invisible to an earlier as-of query (lookahead-free, tested).
- Bitemporal slice (`events_valid_as_of`) distinguishes a corrected value from the originally-known value at a past decision time.
- `atomr-persistence-sql` ships the temporal columns/indexes and an as-of query path.

**Fallback if declined.** `hedgehog-data-core` layers bitemporality on top by encoding (valid_time, system_time) inside event payloads and tags and doing as-of filtering in application code over `current_events_by_tag`. Correct but every query must hand-roll the temporal filter; index support is lost.

---

## FR-9: WORM / tamper-evident hash-chained journal option + true transactional outbox for atomic checkpoint↔ledger linkage — **P0**

**Motivation (hedge-fund use case).** Regulatory books-and-records (SEC 17a-4, MiFID II) require immutable, tamper-evident storage and atomic linkage between an agent checkpoint and its Ledger Decision. hedgehog must prove the journal wasn't altered and that a HITL approval and its resulting state change committed together or not at all.

**Current behavior / gap.** `atomr-persistence-sql` journals events as opaque bytes with monotonic `sequence_nr` per persistence_id and a `created_at`, but offers NO WORM/append-only constraint, no hash-chaining/tamper-evidence, and no documented integrity-verification API. `atomr-patterns` `outbox` is a journal-tailing, at-least-once RELAY (poll a `ReadJournal`, track an offset, publish) — it does NOT provide an atomic same-transaction outbox that commits a domain event and an outbound/Ledger record in one DB transaction; cross-aggregate atomic writes must be hand-rolled. `atomr-patterns` `cqrs` has AuditProjection/AuditLog but no tamper-evidence.

**Proposed API / behavior.** Two additions in `atomr-persistence-sql` / `atomr-patterns`:
```rust
// (a) Tamper-evident WORM journal mode:
pub struct WormConfig { pub hash_chain: bool, pub deny_update_delete: bool }
impl SqlJournal { pub fn with_worm(self, cfg: WormConfig) -> Self; }
// Each row stores prev_hash + row_hash = H(prev_hash || canonical(payload, seq, system_time)).
pub trait IntegrityVerify {
  async fn verify_chain(&self, pid: &str) -> Result<ChainProof, IntegrityError>; // detects any edit/gap
}
// (b) Transactional outbox (same-tx, not tailing relay):
pub struct TxOutbox;
impl TxOutbox {
  // Persist the aggregate event AND an outbox/ledger record atomically in ONE tx.
  pub async fn persist_with<E, O>(&self, tx: &mut Transaction, event: E, outbox: O)
      -> Result<(), OutboxError>;
}
```
WORM mode emits DDL (or documents grants) to deny UPDATE/DELETE on the journal table; `verify_chain` recomputes the hash chain and reports the first divergence.

**Acceptance criteria.**
- A WORM/append-only journal mode exists; UPDATE/DELETE on committed rows is denied (DB-enforced) and documented.
- Optional hash-chaining: `verify_chain` detects any post-hoc payload edit, reorder, or deletion and identifies the first tampered sequence.
- A same-transaction outbox commits a domain event and its Ledger/outbound record atomically (partial commit impossible — tested with injected mid-tx failure).
- The existing tailing outbox relay continues to work and is documented as distinct from the transactional outbox.

**Fallback if declined.** `hedgehog-persist` enforces WORM via Postgres triggers/grants and computes its own hash chain in the aggregate, and wraps writes in explicit sqlx transactions for the outbox. Doable since payload is opaque bytes, but tamper-evidence and atomic linkage are exactly the security-sensitive plumbing best standardized upstream.

---

## FR-10: First-class trace/baggage propagation on actor messages (`MessageEnvelope` metadata + Props interceptor) — **P1**

**Motivation (hedge-fund use case).** End-to-end tracing from a harness trigger through every actor hop to the Ledger requires causal context (W3C TraceContext) to propagate automatically across tell/ask, mailboxes, and streams/JoinSet boundaries. Wrapping every domain message in `Traced<M>` by hand is invasive and leaks tracing into domain types.

**Current behavior / gap.** atomr-core `MessageEnvelope<M>` is exactly `{ message: M, sender: Sender }` — it carries a typed sender but NO metadata/baggage map. `tell(msg)` / `tell_from(msg, sender)` / `ask_with` provide no context slot. `Props` supports `with_dispatcher/with_mailbox/with_supervisor_strategy/with_deploy` but exposes NO mailbox/message interceptor hook, so there is no Props-level place to inject/extract trace context around message handling.

**Proposed API / behavior.** Add a metadata map to the envelope and a Props-level interceptor:
```rust
pub struct MessageEnvelope<M> { pub message: M, pub sender: Sender, pub metadata: Metadata }
pub struct Metadata { /* small typed map; trace_id/span_id/baggage live here */ }
impl<M> ActorRef<M> {
  pub fn tell_with_meta(&self, msg: M, meta: Metadata);
}
impl<A: Actor> Context<A> { pub fn metadata(&self) -> &Metadata; } // current message's context

pub trait MessageInterceptor: Send + Sync {
  fn before_handle(&self, meta: &Metadata) -> SpanGuard; // open span from incoming context
  fn outgoing(&self, parent: &Metadata) -> Metadata;     // inject child context on sends
}
impl<A: Actor> Props<A> { pub fn with_interceptor(self, i: Arc<dyn MessageInterceptor>) -> Self; }
```
The runtime carries `metadata` across local tell/ask, serializes it on remote hops, and a default TraceContext interceptor auto-creates child spans — domain message types stay clean.

**Acceptance criteria.**
- `MessageEnvelope` carries an extensible metadata map propagated across local and remote (serialized) hops without changing domain message types.
- `Context` exposes the current message's metadata so handlers can read/extend trace context.
- A Props-level interceptor hook fires before/after handle and on outgoing sends; a default TraceContext interceptor links parent→child spans across actor hops.
- An example shows a single trace_id flowing from a harness trigger through ≥3 actor hops and a stream boundary into a Ledger write.

**Fallback if declined.** hedgehog defines `Traced<M>{ ctx: TraceContext, inner: M }` and wraps every message, threading context manually. It works but pollutes every actor's Msg enum and is easy to drop at stream/JoinSet boundaries — the exact invasiveness this FR removes.

---

## FR-11: OpenTelemetry span model for actors and streams (span per message-handle / per stream-element, trace_id-correlated) — **P2**

**Motivation (hedge-fund use case).** Ops dashboards and a trace explorer need actor/stream spans — mailbox latency, handle duration, backpressure stalls, supervision restarts — correlated by trace_id, not just aggregate counters. This is how hedgehog operators diagnose latency and causality in the live fleet.

**Current behavior / gap.** `atomr-telemetry`'s OTel exporter is METRICS ONLY: it builds an `SdkMeterProvider` and emits u64/i64 counters (`atomr.actors.spawned/stopped/live`, `atomr.dead_letters`, `atomr.streams.started/finished/running`, `atomr.persistence.events_written`, `atomr.ddata.updates`, etc). There is no `Tracer`, no `Span`, no `trace_id` — `StreamsProbe`/`ShardingProbe`/etc emit counts via a `TelemetryBus`, not spans. So per-message and per-element latency/causality is not observable as spans.

**Proposed API / behavior.** Add a tracing/span exporter and lifecycle spans (composes with FR-10 context):
```rust
pub struct OtelTracerExporter { /* SdkTracerProvider + OTLP span exporter */ }
impl OtelTracerExporter { pub fn new(cfg: OtlpConfig) -> Result<Self,String>; }
// Span emitted per message handle, parented by incoming Metadata trace context:
//   span name = "actor.handle", attrs: actor.path, msg.type, mailbox.wait_ms, handle_ms
// Span per stream element (sampled): "stream.element", attrs: graph.name, backpressure_ms
// Span on supervision events: "actor.restart", attrs: directive, error
pub struct SpanProbeConfig { pub sample_ratio: f64, pub stream_elements: bool }
impl TelemetryBus { pub fn with_span_exporter(self, e: OtelTracerExporter, cfg: SpanProbeConfig) -> Self; }
```
Spans inherit the FR-10 Metadata trace context so a request's spans across actors/streams share one trace_id.

**Acceptance criteria.**
- `atomr-telemetry` can export OTLP spans (not only metrics) via a tracer provider.
- A span is emitted per message handle with mailbox-wait and handle-duration attributes, parented by the incoming trace context (FR-10).
- Optional sampled per-stream-element spans capture backpressure stalls; supervision restarts emit spans.
- An example shows a single trace_id spanning a harness trigger → actor hops → stream → persistence write in a trace viewer.

**Fallback if declined.** `hedgehog-observability` emits its own spans from FR-10 interceptors using the opentelemetry crate directly, treating atomr's metrics exporter as complementary. Viable once FR-10 lands; without FR-10 there is no propagated context to parent spans by.

---

## FR-12: FIX session-layer crate (or session-state actor template) for exchange/broker connectivity — **P2**

**Motivation (hedge-fund use case).** Execution cannot reach most real or paper venues without a FIX session/transport layer. Correctness here (logon, heartbeat, sequence-number management, resend/gap-fill) is directly tied to exactly-once order semantics; building it net-new in hedgehog is large, latency-sensitive, and error-prone.

**Current behavior / gap.** atomr has no broker/exchange/market-data connectivity and no FIX engine. atomr-streams provides Source/Sink/backpressure and `Tcp` (Incoming/Outgoing), plus `Framing`/`FramingError` for delimiter/length framing, but there is no FIX 4.2/4.4/5.0 session layer (logon/heartbeat/seq-num/resend/gap-fill) and no reference REST/WS broker connectors.

**Proposed API / behavior.** An optional `atomr-fix` crate built on atomr-streams Tcp + Framing + an actor session FSM:
```rust
pub enum FixVersion { Fix42, Fix44, Fix50Sp2 }
pub struct FixSessionConfig { pub version: FixVersion, pub sender_comp_id: String,
    pub target_comp_id: String, pub heartbeat: Duration, pub reset_on_logon: bool }
pub struct FixSession; // an Actor managing one session FSM
impl FixSession {
  pub fn props(cfg: FixSessionConfig, store: Arc<dyn FixSeqStore>) -> Props<FixSession>;
}
pub trait FixSeqStore: Send + Sync { // persistent in/out seq nums for resend
  async fn next_out(&self) -> u64; async fn observed_in(&self, n: u64);
}
// Inbound app messages -> Source<FixMessage>; outbound -> Sink<FixMessage>.
// Session FSM handles Logon/Heartbeat/TestRequest/ResendRequest/SequenceReset/Logout
// and gap-fill automatically; integrates with atomr-persistence for seq durability.
```
At minimum, if a full engine is out of scope, ship a documented session-state actor TEMPLATE (FSM + Framing + persistent seq store) so the wire protocol isn't reinvented from zero.

**Acceptance criteria.**
- A FIX session actor (or fully-documented template) performs logon, heartbeat/test-request, and orderly logout against a reference acceptor.
- Sequence-number recovery works: on reconnect it issues/honors ResendRequest and gap-fill, with seq numbers persisted via a pluggable store (atomr-persistence-backed).
- Inbound/outbound messages surface as atomr-streams Source/Sink with backpressure.
- An interop test exchanges NewOrderSingle/ExecutionReport with a simulator and recovers cleanly from a mid-session disconnect.

**Fallback if declined.** `hedgehog-execution` builds the FIX FSM directly on atomr-streams Tcp + Framing + a persistent seq store (a session-state actor). The framing/streams primitives make this tractable, but it is the largest single piece of net-new latency-sensitive code and benefits most from upstream ownership.

---

## FR-13: Sanctioned record-and-replay determinism — persist `SeededRng` state alongside journal events and external-feed commands — **P1**

**Motivation (hedge-fund use case).** hedgehog's paper-trading and live runs must replay bit-exactly for audit and drift diagnosis. The journal mixes external feed commands (non-deterministic arrivals) with RNG draws (fill simulation, jitter). Replaying faithfully requires the RNG state to be checkpointed in lockstep with feed events; this is not a documented atomr capability.

**Current behavior / gap.** atomr-persistence event-sourcing + snapshots exist, and atomr-testkit has a virtual-time `TestScheduler`, but there is no sanctioned mechanism to (a) persist a `SeededRng`'s state as part of the journal/snapshot so a replay reproduces the same draws, nor (b) mark journal entries as 'external command' (replay as recorded) vs 'derived' (recompute). serde defaults would happily store derived/float state, making replay non-faithful. There is also no documented model/provider-version pin recorded with the run for governance.

**Proposed API / behavior.** A determinism module spanning persistence + a seeded RNG type:
```rust
pub struct SeededRng { /* counter-based, splittable; state is serializable */ }
impl SeededRng {
  pub fn from_seed(seed: u64) -> Self;
  pub fn snapshot(&self) -> RngState;       // exact resumable state
  pub fn restore(state: RngState) -> Self;
  pub fn split(&mut self) -> Self;          // independent substream per actor
}
// Journal entries tagged by provenance so replay knows what to recompute vs replay:
pub enum EntryKind { ExternalCommand, DerivedEvent }
impl PersistentRepr { pub fn with_kind(self, k: EntryKind) -> Self; } // stored in tags/manifest
// Replay harness: feed ExternalCommand entries as recorded, restore RngState from the
// nearest snapshot, recompute DerivedEvents, and assert state equality.
pub struct ReplayHarness;
impl ReplayHarness { pub async fn replay<A: Eventsourced>(&self, pid: &str) -> ReplayReport; }
// Run metadata pin (governance): record model/provider/version with the run snapshot.
pub struct RunPin { pub model: String, pub provider: String, pub version: String, pub seed: u64 }
```

**Acceptance criteria.**
- A serializable seeded RNG type whose snapshot/restore reproduces an identical draw sequence; splittable per actor.
- Journal entries can be tagged ExternalCommand vs DerivedEvent so replay replays the former and recomputes the latter.
- A documented replay path restores RNG state from a snapshot + replays feed commands and yields bit-identical aggregate state (tested end-to-end).
- Run metadata (model/provider/version/seed) is recordable with a snapshot so a replay can assert the pin matches.

**Fallback if declined.** `hedgehog-backtest`/`hedgehog-paper` builds `SeededRng` over `rand_chacha` with manual state serialization, tags entries via the existing `tags: Vec<String>`, and writes its own replay harness over `Eventsourced::replay`. Fully feasible (the building blocks exist) but the "faithful journal" guarantee is subtle and would benefit from an upstream-sanctioned pattern.

---

## Summary

| FR | Title | Priority |
|---|---|---|
| 1 | Exact-decimal `Money`/`Decimal` primitive + decimal-safe persistence column | **P0** |
| 5 | Cluster-wide latched, acked kill-switch with unified halt semantics | **P0** |
| 7 | Role-weighted quorum SBR + singleton fencing + handoff | **P0** |
| 9 | WORM / tamper-evident hash-chained journal + transactional outbox | **P0** |
| 2 | Logical-clock-gated streams `Source` (lookahead-free PIT replay) | P1 |
| 3 | HTTP-poll/WebSocket Source/Sink + token-bucket rate-limit operator | P1 |
| 4 | Signed decimal-payload exposure CRDT (`SignedSumMap`) | P1 |
| 6 | Graded supervisor directives (`Throttle`/`Suspend`) | P1 |
| 10 | Trace/baggage propagation on actor messages | P1 |
| 13 | Record-and-replay: persist `SeededRng` state + tag external-vs-derived | P1 |
| 8 | Bitemporal / as-of journal query semantics | P2 |
| 11 | OpenTelemetry span model for actors/streams | P2 |
| 12 | FIX session-layer crate | P2 |
