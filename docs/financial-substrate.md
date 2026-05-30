# Financial-substrate primitives

atomr ships a coordinated set of primitives for building **regulated, real-money,
books-and-records** systems on top of the generic actor/streaming substrate.
These close the gaps documented in
[`docs/feature-requests/requests-from-hedgehog.md`](feature-requests/requests-from-hedgehog.md):
money-correctness, deterministic replay, hard safety, regulatory storage,
real-world I/O, and end-to-end observability.

Everything here is **additive** — core type changes are field-additive with
defaults or `#[non_exhaustive]`, so existing applications keep compiling.

## Money-correctness

### Exact decimal money — `atomr-money`

```rust
use atomr_money::{Money, Currency, RoundingMode};

let notional = Money::from_str_amount("1250000.00", Currency::USD)?;
let fee       = Money::from_minor(125, Currency::USD);        // $1.25
let net       = notional.checked_sub(&fee)?;                  // checked: errors on ccy mismatch/overflow
assert_eq!(net.round(RoundingMode::BankersRounding).to_minor(), 124_999_875);
```

- **No defaulted `f64` constructor.** Lossy `f64` ingestion exists only behind
  the off-by-default `f64-lossy` feature.
- **String serde, never float** — `Money`/`Price`/`Qty` serialize their amount
  as a decimal string, so nothing is lost in transit or at rest.
- `Price::round_to_tick` / `Qty::round_to_lot` are tick/lot aware.

### Decimal-safe persistence

`atomr-persistence-sql` maps decimal aggregates to `NUMERIC`/`DECIMAL` columns
(never binary float) behind the `money` feature.

## Deterministic replay

### Logical clock + clock-gated source (FR-2)

`atomr_core::time::{Clock, LogicalTime, ManualClock, SystemClock}` is a pluggable
clock. `atomr_streams::clock_gated(src, clock, event_time)` emits an element only
once `clock.now() >= event_time(e)` — so a backtest agent **cannot observe future
bars** regardless of async inference latency. `step_locked` additionally requires
the consumer to acknowledge each logical instant before the clock advances.

### Record-and-replay (FR-13)

`atomr_persistence::determinism` provides a serializable, splittable `SeededRng`
(`snapshot`/`restore`/`split`), `EntryKind` provenance tags (ExternalCommand vs
DerivedEvent), `RunPin` governance metadata (model/provider/version/seed), and a
`ReplayHarness` that reproduces bit-identical aggregate state.

## Hard safety

### Cluster kill-switch (FR-5)

`atomr_cluster_tools::ClusterKillSwitch` is the single authoritative firm-wide
halt: a monotone `Flag`-backed latch (survives rebalance/split-brain heal),
`await_quiescence` that returns only after every reachable `HaltGuarded` party
acknowledges, and a two-person-authorized epoch `reset` that emits an auditable
event.

### Graded supervisor directives (FR-6)

`Directive::{Throttle, Suspend, ResumeFrom}` (+ `SuspendMode`) let a supervisor
degrade a **running** actor via `Actor::on_directive` — no restart, state
preserved — driving a risk circuit breaker. Push proactively with
`ActorRef::tell_directive`.

### Role-weighted quorum + singleton fencing (FR-7)

`RoleWeightedQuorum` downs a partition by role weight (not raw node count);
`QuorumObserver` fires on quorum loss/regain (wire it to `ClusterKillSwitch`);
`SingletonHandoff` + a monotonic `FenceToken` migrate an external session (e.g. a
prime-broker connection) without a double-ownership window.

## Regulatory storage

### WORM + tamper-evidence + transactional outbox (FR-9)

`SqlJournal::with_worm(WormConfig)` enables an append-only, hash-chained journal;
`IntegrityVerify::verify_chain` detects any edit/reorder/deletion and reports the
first tampered sequence. `atomr_patterns::TxOutbox::persist_with` commits a domain
event and its ledger/outbound record in **one** transaction (distinct from the
existing journal-tailing relay).

### Bitemporal as-of queries (FR-8)

`ReadJournal::events_as_of(pid, system_time)` and `events_valid_as_of(..)` answer
"what did we know about time T as of decision time D" — a later-recorded
restatement is invisible to an earlier as-of query (no lookahead).

## Real-world I/O

### HTTP/WebSocket sources + rate limiting (FR-3)

`atomr-streams-io` provides `HttpPollSource` (conditional GET/ETag) and `WsSource`
(reconnecting) behind `http`/`ws` features. `atomr_streams::rate::token_bucket` /
`token_bucket_keyed` / `respect_retry_after` enforce fair-access limits (e.g.
EDGAR's 10 req/s) with burst and per-key support.

### FIX session layer (FR-12)

`atomr-fix` implements a FIX session FSM — logon, heartbeat/test-request,
sequence-number management, resend/gap-fill, orderly logout — over
`atomr-streams` Tcp + Framing, with a pluggable `FixSeqStore` for sequence
durability.

## Observability

### Trace propagation + span model (FR-10, FR-11)

`MessageEnvelope` carries `Metadata` (trace context + baggage) propagated across
hops without touching domain types; install a `MessageInterceptor` via
`Props::with_interceptor`. `atomr-telemetry`'s `OtelTracerExporter` +
`TraceContextInterceptor` emit per-message-handle / supervision / stream-element
spans correlated by trace_id, alongside the existing metrics exporter.
