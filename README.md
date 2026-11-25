> **AI agent instructions (organization-wide):** Before working on code, issues, or pull requests, read and follow the [organization `AGENTS.md`](https://github.com/atto-lang/.github/blob/main/AGENTS.md), including its **merge-first stealth-development policy**. Honor any repository-local `AGENTS.md`, explicit holds, and GitHub-enforced rules.

# AttoLang Durable Objects

## AttoLang migration and authoritative specification (2026-11-18)

**AttoLang** is the current public name of Oreslang. This `atto-lang/atto-durable-objects.atto` repository is the current development target for this work. Its predecessor is `ores-truffle-oreslang/oreslang-durable-objects.ores`; migration does not establish identical source history or executable parity. Legacy source and commands below reflect this checkout; do not automatically substitute new CLI names or claim feature parity. These repositories are not assumed to have identical Git histories, issues, pull requests, or implementation results.

**The canonical language specification is [atto-lang/atto-spec](https://github.com/atto-lang/atto-spec)**. Portable `.atto` modules belong in [atto-common-source](https://github.com/atto-lang/atto-common-source) and executed cross-backend positive/negative fixtures in [atto-platform-conformance](https://github.com/atto-lang/atto-platform-conformance). The proposed modern rules include `snake_case` methods, ownership `borrow!`/`take!`/`share!`/`copy!`/`copy!!`, [implicit borrowing](https://github.com/atto-lang/atto-spec/blob/main/language/implicit-argument-borrow-v1.md), [file/module constants including nonthrowing launch-time environment reads](https://github.com/atto-lang/atto-spec/blob/main/language/file-module-const-v1.md), and [separate bitwise-shift and directional-stream operators](https://github.com/atto-lang/atto-spec/blob/main/language/bitwise-and-stream-operators-v1.md). These are contract/design directions, not a claim this legacy implementation supports all syntax.

Preserve historic `.ores` paths, command names, ABI/wire names and pinned fixtures until verified replacements exist. Avoid Python support tooling/tests; prefer Rust. Do not infer successful compilation, actor isolation or cross-backend parity from README examples, host-only tests, or zero-step GitHub Actions. Document exact commit/tree, target, toolchain, executed guest tests and explicit failures/unsupported status.

Status: **native host substrate plus draft AttoLang guest integration**, not a production durable-object service.

A durable object is a stable `(tenant_id, namespace, object_id)` identity with actor-owned state, persistent transactional storage, and a single logical owner at a time. An **OresVM** host owns one **IsolateVM per tenant incarnation**. Each IsolateVM owns **multiple bounded FIFO queues keyed by a tenant-scoped LockId**. Actors and durable objects live inside that tenant isolate; each actor retains sealed state and its own arena.

```text
OresVM root
  Tenant A -> IsolateVM (generation gA, tenant heap, quotas)
       LockId 7  -> FIFO [operation 1 -> operation 2 -> operation 3]
       LockId 19 -> FIFO [operation 8 -> operation 9]
       Object (namespace, id) -> actor + persisted state + assigned LockId
  Tenant B -> IsolateVM (generation gB, distinct heaps and queues)
       LockId 7 -> a completely different FIFO and owner
```

## Goal: Cloudflare Durable Objects–style computing

Provide stable named virtual actors with colocated computation and transactional
storage, automatic activation, exclusive turns, durable alarms and idle
passivation. AttoLang intentionally targets non-reentrant actor turns across
suspension rather than replicating all JavaScript event-loop semantics.

```text
authenticated tenant + namespace + object name
    -> virtual actor directory / fenced activation
    -> exclusive turn permit
    -> durable receipt lookup (matching replay returns original reply)
    -> precompiled AttoLang guest proposes next state
    -> SQLite WAL atomic state + receipt + outbox commit
    -> ownership-checked response publication
```

The directory, scheduler and external publication authority are work in
progress; epoch checks alone are not an ownership election protocol.

### Implementation progress (2026-11-24)

| Capability | Evidence / status |
| --- | --- |
| Tenant FIFO queues and SQLite WAL substrate | Existing host code, not a sandbox |
| Epoch, revision, exact command/schema receipts and outbox | [Storage PR #9](https://github.com/atto-lang/atto-durable-objects.atto/pull/9), draft |
| Bounded counter guest and self-tests | [guest/durable_counter.atto](guest/durable_counter.atto), source candidate |
| Commit-gated counter adapter | [counter_turn.hpp](native/include/ores/durable/counter_turn.hpp), earlier host-only CI passed |
| Compile-once scalar Wasm entry | [Compiler PR #566](https://github.com/atto-lang/atto-source.rs/pull/566), draft, not executed |
| Compiled guest to SQLite, hard kill after commit, replay | [Process integration fixture](tests/durable_counter_process_e2e.mjs), not executed |
| Automatic named activation, exclusive FIFO turns, explicit passivation | [Host directory prototype](native/include/ores/durable/counter_directory.hpp), tests authored, not executed |
| Automatic idle passivation, multi-object scheduling | Bounded host worker-pool prototype, tests authored (not executed) |
| Cooperative single-host controller ownership | POSIX advisory lock prototype, competing-controller test authored |
| Durable alarms | Generation-fenced storage + optional automatic FIFO dispatch prototype; tests authored, not executed |
| Persistent mailbox admission and restart dispatch | Wave 9 host regression passed |
| Mailbox retry, dead letters and completion reservation | Wave 9 host regression passed |
| Typed RPC and secure transport | Planned |
| WebSocket hibernation, placement, replication and failover | Planned |

### Virtual actor directory (host prototype)

[CounterDirectory](native/include/ores/durable/counter_directory.hpp) resolves
the full tenant/namespace/type/name tuple. Trusted authorization is checked
at admission, execution and before returning a receipt. A fixed-size worker
pool (1–64 workers, default 1) runs separate FIFO object lanes; a running lane
is never scheduled twice, even if its trusted callback blocks. Rotating
ready-object selection avoids fixed key-order starvation. SQLite transactions
remain serialized through the shared store; distinct guest computations can
progress concurrently. Pending admissions and active epochs have separate
quotas. Worker threads, rather than guest carriers, perform blocking storage.

```text
authorized request -> bounded per-object FIFO -> ready lane rotation
    -> exclusive worker turn -> lazy persisted epoch -> guest transition
    -> atomic state/receipt/outbox -> host future
```

Explicit passivation is a FIFO barrier. Optional monotonic-clock idle
passivation removes only a lane with no executing or queued turn. It never
deletes committed state; the next authorized request allocates a fresh
persisted epoch. Idle passivation is disabled by default; configure a positive
duration on the host. An authorized `is_active` diagnostic is observational
only, not a lease or capability.

A persistent POSIX advisory lock on the canonical database's sibling
`.controller.lock` file prevents cooperating local controllers from opening
the same store simultaneously. The lock spans the full worker and store
lifetime and is never unlinked by the controller. It assumes a trusted local
directory, stable paths, and cooperative raw-store users; not distributed
consensus, hostile-filesystem defense or a tenant sandbox. Authorization and
guest callbacks must be thread-safe and terminating. The host future is not
the AttoLang suspension ABI, and does not grant network publication rights.

[Directory tests](native/tests/counter_directory_test.cpp) now cover 100
ordered turns, dedupe, backpressure, restart, explicit and idle passivation,
cross-object progress during a held turn, no same-object overlap, and
competing-controller rejection. CI also schedules ASan/UBSan. None of this
new directory evidence has executed yet. Compiled guest integration in the
directory, authenticated ingress, unified durable inbox, cancellation, fuel/time
quotas and production alarm policy remain outstanding.

### Persistent mailbox (host prototype)

The durable API is explicitly separate from the transient `send()` path:

```text
admit(key, stable request ID, operation, delta)
    -> authenticated FIFO control turn
    -> SQLite transaction inserts immutable pending mail
    -> admission receipt (sequence, not a state-transition ACK)
    -> restart-safe mailbox poller selects oldest pending record per object
    -> exclusive object turn checks owner, identity, command and schema
    -> atomic snapshot + completion receipt + outbox + inbox removal
```

Retrying the identical request returns its pending sequence or committed
receipt; conflicting commands/schemas reject. Pending rows survive restart
and cannot be consumed out of order or bypassed by a direct commit with
the same ID. The reserved alarm namespace cannot be admitted. Defaults
bound pending mail to 128 records per actor, 4096 per database and 64 MiB
aggregate command bytes; persisted limits are checked again at reopen.
The separate configurable mailbox poller (disabled by default) now has a candidate bounded concurrent dispatch path described in Wave 16. The counter
demo commits a terminal `mail_overflow` receipt with unchanged state.
Explicitly classified stopped-guest failures use the bounded persistent retry policy below; other handler/authorization faults stop dispatch visibly through `mail_fault()`; backpressure retains pending rows.

An admission future is not proof of eventual completion or network
publication. Only requests admitted through this API have restart-safe
admission; transient sends, alarms and controls can interleave between
durable mailbox entries. FIFO is guaranteed within the durable mailbox,
not across these separate admission domains. Host callbacks are not
compiled guest execution. Administrative dead-letter retention/requeue, tenant fairness, authentication of persisted principals and unified durable ingress remain open. The
regression tests cover process exit after admission, reopen, exact replay,
stale epochs, head checks, rollback, quota rejection and drain after
restart; new execution evidence is pending.

### Durable mailbox retries and poison isolation (host prototype)

The trusted runner can signal `RetryableGuestFailure` only after guest
execution has physically stopped without committing. A separate SQLite
retry record tracks the exact mailbox head's classified failures, next
eligible Unix-millisecond deadline and terminal dead-letter status.
The fixed v1 policy is:

| Recorded failure | Outcome |
| --- | --- |
| 1 | Retry after 100 ms |
| 2 | Retry after 200 ms |
| 3 | Terminal dead letter; allow next live FIFO entry |

A backed-off head blocks later entries for that object, but ready heads
for other objects remain discoverable. Epoch, immutable command, schema,
sequence, previous failure count and deadline are checked in the retry
transaction. Dead letters retain original identity/payload and return
`dead_letter=true` on exact admission replay; no success receipt or
state/outbox mutation is manufactured. They remain charged against inbox
count and byte budgets. No automatic requeue or destructive retention
policy is provided.

Admission reserves one completion receipt and outbox slot for each live
pending entry. Other commits cannot consume those reservations. Reopen
checks existing reservations against configured limits. Dead-lettering
releases completion slots but not the retained inbox budget.

Classified stopped-guest failures and abandoned dispatch markers use this
policy. Database, ownership, authorization and unexpected faults stop the
poller; a surviving marker is resolved only under a fresh fenced activation.
The compiled scalar watchdog and fuel worker have separate executed evidence.
 The Wave 16 candidate dispatches distinct objects concurrently, but a nonterminating trusted callback can still occupy a worker. These are not production guarantees of poison isolation or eventual
delivery. Tests cover retry deadlines, restart, stale attempt fencing,
dead-letter replay, healthy object progress and reserved capacity; no
new exact-head execution receipts yet.

### Durable alarms (host prototype)

The storage candidate in [PR #9](https://github.com/atto-lang/atto-durable-objects.atto/pull/9)
persists one Unix-millisecond deadline per logical object, with a monotonic
alarm generation that survives cancel and consume. Setting/replacing or
cancelling requires the current generation and owner epoch; cancellation
retains a tombstone. The reserved receipt identity `@alarm:<generation>`
cannot be submitted as an ordinary request. Legacy conflicting receipts
fail closed on migration. Due scans are indexed and bounded.

The directory provides trusted, asynchronous `set_alarm(key, generation,
due_ms)`, `alarm_generation(key)`, and cancellation via a null deadline.
An optional positive alarm poll interval enables a dedicated supervisor
poller (disabled by default). It scans at most 64 due records per batch,
submits at most one outstanding alarm turn, and uses the same exclusive
FIFO lane as ordinary operations. Dormant objects activate under a fresh
epoch. The worker checks the deadline, alarm generation and epoch again;
it atomically consumes the deadline with state, immutable receipt and
outbox update. Restart scans persisted deadlines; a consumed alarm is not
run twice. Clock deadlines use wall-clock time, while polling and idle
passivation use monotonic elapsed time.

For the bounded counter demo an alarm invokes the existing increment-by-one
transition. At maximum count it records terminal `alarm_overflow` with
unchanged state. Queue/activation saturation delays dispatch; unexpected
faults stop the poller and are exposed through `alarm_fault()` for trusted
supervisor recovery. Shutdown drains a currently dispatched alarm.
This is not a general compiled alarm-handler ABI: scheduling/cancellation
are separate fenced control transactions, not atomic with arbitrary guest
state changes. Durable retry backoff/dead-letter policy, per-tenant fair
alarm dispatch, authenticated capabilities, hard watchdogs, multi-host
ownership and integrated compiled guest execution remain outstanding.

Host regression cases cover cancel/replace, stale owner and generation,
rollback, reopen, auto-activation, consumed receipt replay and terminal
overflow. See the directory and actor-sync recovery tests; new tests remain
unexecuted pending exact-head CI. No exactly-once external effect claim.

### Deterministic fuel and Linux process limits (Wave 11 — executed)

The optional [Rust fuel worker](guest/fuel-worker/src/main.rs) uses pinned
Wasmi 0.31.2 and a fresh 50,000-unit budget per scalar evaluation. Fuel
exhaustion has a distinct status from traps and wall-clock timeouts.
The scalar profile rejects imports, linear memories, tables and start
functions, checks artifact SHA-256 and admits one instance. Linux limits
cap address space at 256 MiB, CPU at two seconds and descriptors at 16;
core dumps are disabled. Parent-death SIGKILL is installed with an identity
check. The parent watchdog and physical reap remain mandatory.
These are engine-specific process limits, not portable AttoLang fuel,
aggregate tenant quotas or a production sandbox.

The same compiled looping fixture tests both execution engines. The fuel
case must report fuel exhaustion, retry three times, dead-letter and permit
healthy mail. Direct versions are pinned; CI resolves a lockfile, records
its digest and graph, then builds --locked. A reviewed committed transitive
lock remains required. [Wave 11 guest run 45536758165](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45536758165)
executed exact head `bd3b79570ced4ad3b5ba9e76c1c9b69049a69bee` and emitted
`COMPILED_COUNTER_FUEL_WORKER_PASS` and `COMPILED_COUNTER_PROCESS_WATCHDOG_PASS`.
The compiled looping artifact SHA-256 was
`66c10e6311e347e92d726945a62937af1798d8d8f9abe33f4e62e0293d00b2a7`.

### Crash-counted mailbox dispatch (Wave 12 candidate)

Storage PR #9 adds a durable dispatch marker bound to the exact FIFO head
and active epoch. The directory commits it before calling the scalar guest.
A successful state/receipt/outbox transaction deletes the inbox and cascades
the marker; a classified stopped-guest failure atomically clears the marker
and advances retry metadata. A surviving marker under an older epoch is
charged once before launching another attempt. Duplicate/stale tokens and
unmarked completion of a marked head are rejected. The third charge dead
letters the original request without inventing a success receipt.

The conservative policy counts a crash after recording the marker even if
guest launch had not occurred. Only the cooperating single-host controller
and pure, authority-free scalar guest are supported. Controller lock release without inherited worker guards
and epoch rotation are not proof of physical retirement of an arbitrary
orphan guest, nor safe recovery of external side effects. General actor
recovery requires a supervisor that establishes physical quiescence.
The storage fork/reopen and directory controller-exit regressions are new;
Wave 12 exact-head execution passed (receipt below).

### Controller retirement and scoped deployment (Wave 13 candidate)

Wave 12 guest and host integration passed on exact head
`b8ca89bd51c691c6b32d13a4c29e606f1921dbf3`:
[run 45653321529](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45653321529).
The new process runner inherits the controller's locked open file description
at reserved fd 3 while a synchronous directory callback executes. All other
nonstandard descriptors are closed. The trusted worker must retain fd 3
without unlocking it, including after controller death; the kernel denies
a new cooperating controller until the worker releases it on exit.
Standalone fixture calls have no controller guard. This is intentionally
fail-closed: a nonterminating or stopped orphan can block takeover until
a trusted supervisor retires it. This assumes a trusted worker and local
filesystem; it is not containment of arbitrary native code or descendants,
nor distributed election. The regression kills the controller, uses a
subreaper to observe a synthetic orphan, verifies lock denial, and checks
that a replacement can run the real compiled guest only after retirement.

The immutable host-side CounterArtifactRegistry selects a pinned artifact
and fuel runner by full tenant/namespace/type scope, never by guest input.
Keyed callbacks receive the authenticated identity after the directory's
authorization checks; the registry requires the scoped controller guard.
Workers independently hash exact bytes before executing. Missing and
duplicate scope bindings fail closed; wrong artifact digests yield no
successful state transition. The tests use real compiled Wasm for two
tenant scopes. This is a trusted static allowlist, not signed distribution,
network principal authentication, durable manifest-version binding,
or an artifact upgrade protocol. Legacy unkeyed callbacks remain host
fixtures; production ingress must compose principal authorization with
the registry check. General actor ABI proposal extends
[atto-spec #63](https://github.com/atto-lang/atto-spec/pull/63);
the scalar implementation is not that ABI.

Wave 13 guest and host regressions passed in run 45653915725 (see below).

### Signed immutable deployment identity (Wave 14 candidate)

The trusted artifact registry canonicalizes the complete scope set using
length-framed exact bytes sorted by tenant/namespace/type. Its v1 identity
includes each artifact SHA-256, the scalar engine/fuel profile and wall
budget, rather than the machine-local path. The new SQLite deployment
record binds those exact bytes transactionally before any worker starts.
Reopening with reordered identical bindings is allowed; changed artifact,
scope, budget or absent manifest is denied, even for a receipt replay.
A populated legacy store cannot be silently adopted. No in-place upgrade,
key revocation, or removal of pending mail is offered.

The host-only SignedCounterDeployment path verifies a detached Ed25519
signature over that exact canonical manifest via OpenSSL EVP and an
operator-provisioned trusted public key. No private signing key is required
at runtime. An immutable verified registry supplies both dispatch and
scope admission, composed with an independent trusted ingress authorizer.
The registry checks its controller-bound deployment before executing, and
the fuel worker independently checks the actual artifact digest. Host
fixture constructors remain trusted low-level APIs; they are not a secure
network boundary. The trusted runner executable and filesystem are still
operator-controlled, not independently attested by this manifest. Signed
manifest verification is not principal authentication, revocation or
anti-rollback across a restored/deleted database. Persisted artifact
upgrades require an explicit quiescent migration and receipt policy.

The compiled-fuel regression generates an ephemeral test-only Ed25519 key,
checks signature/key/manifest tampering, validates scoped dispatch, verifies
restart deduplication and pending-mail recovery under identical deployment,
and rejects changed configuration and legacy fallback. These are authored
tests, not a Wave 14 execution receipt.

### Operator dead-letter administration (Wave 20 candidate)

The trusted CounterDeadLetterAdmin adapter requires an upstream-authenticated
operator and a separate exact-object/action policy. Inspection pages are
bounded to 256 records and expose identity, principal, terminal denial flag
and replacement sequence, not command payload or raw guest errors.
Strict-policy redrive is explicit, never automatic. It refuses
authorization-denied mail and requires the original principal's current
persisted grant. The old message remains terminal. Redrive copies the
immutable command, schema and principal to a new reserved operation ID and
new FIFO sequence, with ordinary admission quotas and completion reservations.
Admission and source/replacement/operator audit are committed atomically.
One redrive per source row; exact retry by the same operator returns its
original audit result even after replacement completion. Different operators
cannot silently adopt it. Recovery still checks the current grant at
attempt and commit. The new audit is bounded to 65,536 rows. Retained
originals and replacement messages count against quotas; no unsafe purge,
identity reuse or administrative bypass of the authorization fence.

Operator policy is a trusted callback, not a network authentication system
or a transactional revocation guarantee. It is checked before and after
handoff; revocation after admission cannot undo it. Redrive is an explicit
new execution, not exactly-once replay of a previous success. The audit is
a local SQLite record, not a tamper-evident external log. Retention/archive
with preserved deduplication tombstones remains open.

Storage and host directory tests cover scope and operator action denial,
revoked grants, exact redrive retry, original terminal result, restart,
fresh FIFO execution and refusal of authorization-denied redrive. New
tests are authored, not execution-verified. Storage pin:
`1560ceba4eedfcc5c8be8f95902987f0366261a8`.

Wave 16 [run 45660631104](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45660631104)
passed both guest and host jobs at head
`78465334d6a1f33821080f938ae7df33e5f5187b`. Its complete resolved fuel-worker
Cargo.lock was recovered and independently SHA-256 checked:
`a4802aa3eb93c0748c2d87a51b766a458c6a2909844ab3eda6054e294d6550e1`.
The exact 23-package lock is now committed; CI requires it and builds
--locked without regenerating it. New-head verification is outstanding.

### Transactional principal authorization (Wave 19 candidate)

An opt-in strict mode adds a persisted, exact-object principal/operation
grant with a monotonic version. The trusted operator sets grants through
`SqliteActorSync::set_principal_policy(key, principal, op, expected_version, allowed)`;
version zero means absent, and each successful update advances the version.
Missing grants deny by default. Revocation retains a versioned tombstone;
regranting cannot recreate a stale attempt's grant version. The policy
table is bounded to 65,536 rows. This API is a trusted administrative
boundary, not exposed to guest code or a network caller.

The same SQLite transaction that admits a message or returns its replay
checks the current grant. Starting an attempt persists its grant version
with the attempt marker. State/receipt/outbox/inbox consumption checks
that exact version and current permission inside BEGIN IMMEDIATE.
A revocation committed first therefore prevents the old attempt's
state commit, even if the grant was restored before guest return.
A state commit ordered first remains committed; later revocation denies
future replay through durable admission until a current grant permits it.
An invalidated attempt becomes a retained terminal authorization denial
in a subsequent fenced transaction. A crash between failed commit and
terminal denial is recovered through the existing durable attempt protocol;
it never retroactively commits that rejected attempt.

Strict mode is irreversible for the database and must be enabled on
a drained inbox. Configure a signed principal gateway with transactional
policy enabled and recovery disabled, provision exact grants through a
trusted operator, then restart with recovery enabled. Transient/raw/alarm
state commits are rejected in strict mode; network transport and
transactional alarm/control grants are not implemented. Existing non-strict
mode retains its earlier external-policy race limitation. Only the
persisted grant is serialized with commit, not an arbitrary external
policy callback, identity-provider revocation, transport handoff or
external side effect. Use exclusive deployment and do not reopen the
database with older binaries that do not enforce the new mode.

Regression tests cover default denial, stale grant updates, revocation
followed by regrant during a held guest turn, commit-before-revocation,
replay denial, strict-mode downgrade rejection, and subsequent authorized
FIFO progress. A compiled scalar guest test covers strict grant recovery
after restart. Tests are authored, not execution-verified at this head.
CI pins storage head `cf147f32d1ef6e1924b3d40377ae0a68309d752d`.

### Durable principal-bound ingress (Wave 18 candidate)

The signed-deployment gateway now provides `admit(capability, request, delta)`
and an opt-in durable recovery poll interval. The live capability must
belong to the current gateway, authorize the exact principal/object/op,
and be unexpired during submission and admission handoff. SQLite commits
the trusted stable principal binding with the immutable inbox row in the
same transaction. The request identity remains the principal-scoped
digest used by transient ingress; exact replay requires a fresh authorized
grant. Stable principal names must not be reassigned by the upstream
identity provider.

Capability expiration limits new admissions, not the lifetime of a
previously accepted message. Recovery re-evaluates current trusted
scope and principal policy before dispatch, before guest entry, and
after guest return but before state commit. An explicit policy denial
terminally dead-letters the exact fenced head with
`authorization_denied=true`, without a successful receipt, state change
or outbox publication. The dead sentinel shares the existing retry table
(its internal count of three is not evidence of three guest executions);
the principal record distinguishes policy denial. The retained entry
keeps its tenant quota and prevents automatic resurrection on regrant.
The next authorized FIFO head remains eligible. A policy exception or
missing binding fails closed rather than being classified as denial.

Principal-mode startup rejects unbound legacy inbox rows; it never
retroactively assigns them a principal. Raw admission is disabled in
gateway mode. Principal recovery uses the same ownership/attempt fencing,
bounded guest runner and tenant-aware discovery. The trusted policy
provider must be thread-safe, current and available across restarts.
The gateway still does not authenticate network credentials, persist the
policy itself, provide linearizable revocation with the SQLite commit,
or grant alarms/control capabilities. Policy change racing the commit
can suppress handoff but cannot undo a committed transition. The scalar
guest has no external effects; general effectful actors need stronger
transactional authorization semantics.

Storage tests cover immutable principal bindings, exact-attempt forgery
rejection, raw admission denial and terminal denial after reopen.
The compiled guest integration admits Alice and Bob with the same client
request ID, restarts under a revoked Alice policy, requires Bob to commit,
and verifies that regranting Alice returns the terminal denial rather
than replaying her mutation. These tests are authored, not executed
on the new head. CI pins storage head
`71d0ec81d534888b9b6b1ddcc0f6c37c7f0919aa`.

### Tenant-aware durable admission and dispatch (Wave 17 candidate)

Storage PR #9 bounds retained inbox records across all objects and
namespaces of a tenant: default 1,024 records and 16 MiB of aggregate
command payload, in addition to existing actor/database limits. Dead
letters remain charged until a future explicit retention API; exact
pending/terminal replay is checked before admission quotas. Reopen
rejects a configuration that would silently abandon retained mail.
These are inbox payload/count limits, not total database size, actor
activation quota, aggregate CPU/memory quota or a network rate limit.

The scheduler uses a rotating tenant cursor and ranks eligible heads
within each tenant before applying the 256-head scan limit. A sparse
tenant's head need not wait behind all earlier hot-tenant objects.
Within one tenant older eligible heads come first; a backed-off head
still blocks later live mail for its own object. Existing per-object
FIFO, durable attempt/epoch fencing and worker bounds remain in force.
The cursor is volatile and this is best-effort discovery fairness, not
a guaranteed tenant resource share, preemption or latency SLA. The
ordinary transient scheduler and alarm poller are not tenant-fair yet.

Regression cases include 260 hot objects plus a sparse tenant, cursor
wraparound, single-worker integration (quiet tenant second), retry
backoff, retained dead-letter capacity, exact replay under saturation,
independent byte limit rejection and fail-closed quota reduction.
The new tests are authored and the workflow pins storage head
`9ab67c38f5b9c889daf69ac8c73061018310e36d`; exact-head execution is pending.

Wave 18 introduces candidate durable principal binding and terminal denial; see the dedicated section above. No existing transient receipt is reinterpreted as durable authorization.

### Concurrent durable mailbox recovery (Wave 16 candidate)

The mailbox supervisor no longer waits synchronously for a single object
before admitting another. It tracks at most one outstanding future per
full logical object identity and at most the configured worker count
overall (1–64). Ready futures are collected independently; a bounded
scan of up to 256 eligible heads fills available slots. Exclusive per-object
lanes, epoch/attempt fencing, durable FIFO and completion reservations
are unchanged. A slow or backed-off head does not necessarily block an
independent ready object when a worker slot is available. An unexpected
fault stops new dispatch when observed, publishes the fault and drains
already submitted turns before the supervisor exits. Shutdown drains
accepted work; nonterminating trusted callbacks remain unsupported.
This is bounded cross-object concurrency, not a guaranteed per-tenant
fairness or admission latency policy.

A host regression holds the first durable turn open with a promise,
requires an independent object's commit while the held object's revision
stays zero, then releases it and requires both same-object turns to
complete in FIFO order. This test is authored, not yet execution-verified.

Fuel-worker CI now emits the complete non-secret resolved Cargo.lock
between explicit markers, plus its digest and dependency graph. A
committed lock is never regenerated; the build uses --locked. The
previous executed Wave 11 lock digest was
`a4802aa3eb93c0748c2d87a51b766a458c6a2909844ab3eda6054e294d6550e1`.
Wave 20 retrieved and committed the exact lock bytes after digest verification. A fresh exact-head locked build remains required.

### Principal-scoped ingress (Wave 15 candidate)

Wave 13 exact-head `5fedf9fe3589b1d856f118d79245232209af5f62`
guest and host jobs passed in [run 45653915725](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45653915725).
Wave 14 signed deployment and Wave 15 principal tests still require their
own exact-head receipts.

CounterPrincipalGateway issues opaque, process-local, exact-object and
operation capabilities to a **previously authenticated** stable principal.
Trusted policy is checked at issue, submission, worker entry, before and
after guest execution, and before host receipt handoff. Grants expire in
at most five minutes; a capability from a previous gateway instance is
not accepted. Principal and caller request ID are canonically length-framed
and SHA-256 scoped in a reserved receipt namespace, so two principals
using the same client request ID cannot replay each other's receipt.
A fresh valid grant may replay that principal's original committed request
after restart. Conflicting command replay remains rejected by storage.

Gateway startup reserves the namespace transactionally and rejects legacy
inbox/receipt collisions; ordinary raw controller startup is denied once
gateway mode is enabled. The raw store and authorized-send hook remain
trusted host APIs, not network boundaries. A policy change racing the final
SQLite transaction cannot undo a commit; post-commit revocation can
suppress reply handoff, and external transport must recheck publication
authority. This does not provide linearizable revocation or network token
authentication. Wave 15 exposed transient send only. Wave 18 adds candidate durable principal admission; persisted policy/revocation, authenticated transport, alarm/control grants and key rotation remain future work. Tests cover per-principal receipt isolation, scope denial, operation
bounds, expiry, policy change, restart grants, replay and reserved
namespace collision. No Wave 15 execution claim.

General compiled durable actor lowering remains a separate compiler task:
coordinate [Rust/Wasm #167](https://github.com/atto-lang/atto-source.rs/pull/167)
(sealed state), [#402](https://github.com/atto-lang/atto-source.rs/pull/402)
(typed Job ingress), and [#566](https://github.com/atto-lang/atto-source.rs/pull/566)
(scalar transition) with [spec #63](https://github.com/atto-lang/atto-spec/pull/63).
None of those independently establishes the general durable actor ABI.

### Bounded compiled guest process runner (Wave 10 — executed)

[Exact-head run 45534083712](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45534083712)
executed commit `60950899a13b6b1a7d407c6bc48eceed58fa6c78`, tree
`66d1c3039e7a4a6bd47e63399374a71f93bc7ef4`. Both guest and host jobs
passed. The guest emitted `COMPILED_COUNTER_PROCESS_WATCHDOG_PASS` with
one compilation, a 1500 ms deadline and four expected reaped timeout
children. This verifies the previous head, not the new fuel engine.


[CounterProcessGuest](native/include/ores/durable/counter_process_guest.hpp)
is a Linux/glibc host runner compatible with the existing directory's scalar
guest callback. It launches a fixed trusted Node/Wasm worker without a shell,
with an empty environment, redirected standard streams and closed extra file
descriptors. The worker verifies the expected SHA-256, reads at most 1 MiB of
Wasm, rejects host imports and unexpected exports, and returns one canonical
bounded scalar. The parent bounds stdout and waits for the child to exit.
A wall-clock deadline triggers SIGKILL followed by waitpid/reap **before**
reporting a classified failure; a Wasm trap is classified only after exit.
Digest, protocol and unexpected process errors fail closed.

The new [compiled watchdog fixture](tests/durable_counter_watchdog_e2e.mjs)
compiles an AttoLang transition once, including a deliberately looping branch,
then exercises the directory with actual Wasm children. Tests require a deadline
timeout, no remaining child, terminal dead-letter status after three classified
failures, subsequent healthy execution, and artifact digest rejection.
This is a candidate process-per-turn reference, not a high-performance
persistent worker pool. It assumes trusted immutable script/artifact paths,
a sole waitpid owner, and a cooperating local controller. It is not a full
hostile-tenant sandbox: OS/cgroup memory and CPU limits, deterministic Wasm
worker isolation and authenticated artifact
provenance remain outstanding. A wall deadline includes startup and scheduling;
an uninterruptible OS process cannot be treated as stopped until actually reaped.

### Executed Wave 9 evidence (2026-11-24)

- [Runtime run 45522504636](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45522504636)
  executed exact head `d18f26d328b618b056120c0a863b81bb9c71be0d`,
  root tree `6ff280af9f08119ec852a17e05882a273f6117dc`.
  The compiled scalar SQLite process fixture passed 13 process turns, seven
  actual guest evaluations, one compiler invocation and a single repeated
  Wasm SHA-256 `8ac38b5b4ea76e6e0b8c9df56d72353c2e7739f6f1ab5e3e341c7dd3ae69ec64`.
  Host-only counter, directory, ASan/UBSan and storage alarm/mailbox/retry
  recovery tests passed separately.
- [Storage run 45522468879](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45522468879)
  executed GCC TSan and GCC/Clang ASan/UBSan jobs on
  `e4133470f18d7c2c9a0bf250df6b86096488b9b3` (tree
  `9873db34a026ad3a48269d1d42f0677d129fe9f2`).
- [Compiler ABI run 45520132964](https://github.com/atto-lang/atto-source.rs/actions/runs/45520132964)
  emitted `DURABLE_SCALAR_ABI_V1_PASS` at
  `1d6e8e31453064b6530c5c82679aef3cc9db489b` (tree
  `89f29eb33650b69d3ab8a12da73a28ea26f4b42f`). Broader compiler
  CI remains red: the native checked-i64 fixture imports an unavailable
  legacy crate `oreslang_source_rs`; full corpus, formatting and repository
  policy gates require independent remediation. No cross-backend parity claim.

These receipts supersede the older pending statuses for Wave 9 and earlier
features covered by those tests. Wave 10 and Wave 11 watchdog/fuel evidence is recorded above; Wave 12 passed; Wave 13 remains unverified.

### Compile-once candidate ABI

The opt-in Rust/Wasm compiler mode in PR #566 (not available on all backends)
keeps the existing browser main ABI unchanged. It requires a checked
`durable_transition(int,int,int)->int` and exposes
`atto_durable_transition_v1(i64,i64,i64)->i64`. It rejects heap/state,
actors, generators and host imports in this restricted scalar profile.
This is a prototype transition ABI, not a public durable actor API.

```sh
# Build attoc from the pinned compiler candidate, not an arbitrary installed CLI.
attoc --durable-scalar-v1 counter.atto -o counter.wasm
# counter.atto must define durable_transition and a zero-argument main.
```

The fixture compiles one Wasm artifact and creates a fresh instance per
evaluation. State is loaded from the fenced store; a guest cannot commit
or publish replies directly. Guest execution must ultimately be bounded
by fuel/time and memory quotas, with authenticated artifact admission.

### Security and evidence

- Authenticate tenant/object identity before resolving a capability. No
  cross-tenant mutable pointer sharing. Reject mismatched request IDs,
  schemas, stale epochs and revisions.
- Atomically commit state, immutable receipt and outbox intent before
  publishing success. External effects are at least once, not exactly once.
- A lost reply must be resolved before rerunning a guest (including an
  increment that would now overflow). Ambiguous outcomes require recovery.
- Use isolated bounded storage workers; never block an actor carrier with
  SQLite or hold a mutable state borrow across suspension.
- [Historical exact-head CI](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45519080501):
  at `b480200054c1ac34a055355b80637247197ea49e`, compiler build and host-only
  counter integration passed; guest compilation failed with E_FI.
  The syntax was corrected, but later guest/kill-recovery runs remain
  unverified. [Compiler candidate CI](https://github.com/atto-lang/atto-source.rs/actions/runs/45520132964)
  and [wave 4 CI](https://github.com/atto-lang/atto-durable-objects.atto/actions/runs/45520213102)
  were queued at this review. Do not promote historical host passes to
  current-head guest or cross-backend evidence.
- See [guest parity and implementation notes](docs/ATTO_GUEST_PARITY.md)
  and [PR #15](https://github.com/atto-lang/atto-durable-objects.atto/pull/15)
  for exact dependency pins, tests, limits and current work. Java/GraalVM,
  LLVM, .NET NativeAOT, Wasm and Cranelift durable actor parity remains unverified.

## Contract

Read [spec/DURABLE_OBJECTS_V1.md](spec/DURABLE_OBJECTS_V1.md) for exact queue, transactional storage and isolation invariants. See [spec/TEST_MATRIX.md](spec/TEST_MATRIX.md) for acceptance tests.

The public native-Oreslang API is **proposed**, not yet compiler-confirmed:

- `DurableNamespace<T>.id_from_name(name)`, `get(id) -> DurableStub<T>`
- `DurableStub<T>.send(message)` / `fetch(request)` for authenticated async delivery
- `DurableObjectContext.storage.get/put/delete/transaction`
- `DurableObjectContext.with_lock(lock_id, operation)` for FIFO admissions
- `DurableObjectContext.set_alarm(deadline)`, `delete_alarm()`

No implicit thread blocking, actor handler reentrancy, nested `Future` flattening, or cross-tenant native pointer transfer. State changes use Oreslang actor `state` and sealed `handle_state` under `critical` semantics.

## Backend and verification

The native queue substrate is stacked on [LLVM isolate PR #1158](https://github.com/ores-truffle-oreslang/oreslang-source.llvm/pull/1158), because that branch supplies the private noncopyable tenant IsolateVM and generation-fenced handles. The LLVM isolate integration remains unverified here; this repository now includes a separate trusted-host **persisted** SQLite keyed-queue runtime, but does not yet expose a compiler-verified Oreslang guest API.

Current limitations: LLVM #1158 still serializes *trusted synchronous scalar guest turns* per tenant, rather than running general actor turns per lock concurrently. A genuine durable service needs a persisted journal/database, single-owner epoch fencing, replica recovery, authenticated ingress, scheduler binding, crash injection and AOT guest lowering. Trusted native in-process isolates are **not security sandboxes**; untrusted tenants require an actual isolated OS worker and authenticated IPC. No cross-organization private-source CI transfer without independent authorization.

## New host-only storage reference
A SQLite/WAL storage adapter with atomic state+dedup+alarm transactions and monotonic epoch checks lives in [src/native/SqliteDurableStore.hpp](src/native/SqliteDurableStore.hpp). It is **not** the Oreslang guest API or a distributed consensus service. The [reference test](tests/SqliteDurableStoreTest.cpp) exercises reopen, rollback, tenant separation, epoch fencing, and concurrent duplicate submissions. Details: [spec/STORAGE_REFERENCE.md](spec/STORAGE_REFERENCE.md).

## Persisted keyed FIFO tenant runtime (PR #2)

The second native reference at [native/tenant_runtime.cpp](native/tenant_runtime.cpp)
implements per-tenant SQLite WAL admission, immutable deduplication, FIFO work
queues by `lock_id`, generation/fence-protected leases, atomic state+ACK,
and crash recovery. Distinct locks may progress concurrently, while one lock
has at most one active lease. It also uses POSIX `flock` for local-process
single leadership; this is **not** distributed consensus or hostile-tenant
sandboxing. Delivery remains **at least once**, not exactly-once external
effects. This host substrate is separate from the storage reference above;
neither should be represented as the production public guest API.

Build with C++17, CMake >= 3.20, SQLite >= 3.35 and POSIX locking:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

The merged CMake project registers three tests: `sqlite_durable_storage_test`,
`durable_isolate_queues`, and `durable_persisted_fence_guard`. They still
require execution on the merged tree before CI/merge readiness can be claimed.
See [native API proposal](docs/API_V0.md) and
[LLVM integration gates](docs/LLVM_INTEGRATION.md). The optional LLVM adapter
has not been compiled against the private LLVM integration branch.
