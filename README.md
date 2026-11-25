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
| Persistent mailbox admission and restart dispatch | Bounded host prototype; tests authored, not executed |
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
The separate configurable mailbox poller (disabled by default) scans up to
64 object heads and dispatches one outstanding turn at a time. The counter
demo commits a terminal `mail_overflow` receipt with unchanged state.
Unexpected handler/authorization faults stop dispatch visibly through
`mail_fault()`; backpressure retains pending rows.

An admission future is not proof of eventual completion or network
publication. Only requests admitted through this API have restart-safe
admission; transient sends, alarms and controls can interleave between
durable mailbox entries. FIFO is guaranteed within the durable mailbox,
not across these separate admission domains. Host callbacks are not
compiled guest execution. Durable retry/dead-letter handling, quota
reservation for completion receipts, tenant fairness, authentication of
persisted principals and a unified durable ingress remain open. The
regression tests cover process exit after admission, reopen, exact replay,
stale epochs, head checks, rollback, quota rejection and drain after
restart; new execution evidence is pending.

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
