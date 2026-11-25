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
| Automatic idle passivation, multi-object scheduling | Planned |
| Durable alarms, typed RPC and secure transport | Planned |
| WebSocket hibernation, placement, replication and failover | Planned |

### Virtual actor directory (host prototype)

[CounterDirectory](native/include/ores/durable/counter_directory.hpp) resolves
the complete tenant/namespace/type/name tuple; a trusted authorization
callback checks access before admission, at execution and before returning
the receipt. A dedicated single worker drains a bounded FIFO and lazily
allocates a durable monotonic activation epoch. Callers receive a host
future without blocking on SQLite; the queue rejects overflow. Explicit
passivation is an ordered barrier, so active turns are never evicted and
the next call loads the committed snapshot under a fresh epoch.

This is a **single-controller** host prototype, not a production tenant
sandbox or distributed lease manager. The external controller must own the
store exclusively and provide thread-safe authorization and a terminating
trusted guest runner. One worker serializes all objects; it does not yet
prove cross-object concurrency or integrate AttoLang suspensions. Futures
are host-only and do not grant external publication rights. Idle eviction,
cancellation, authenticated ingress, durable inbox admission and watchdogs
remain outstanding. [Directory test](native/tests/counter_directory_test.cpp)
covers FIFO, quota rejection, replay, explicit passivation and reopen;
execution is not yet verified.

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
