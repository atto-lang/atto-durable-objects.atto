> **AI agent instructions (organization-wide):** Before working on code, issues, or pull requests, read and follow the [organization `AGENTS.md`](https://github.com/ores-truffle-oreslang/.github/blob/main/AGENTS.md), including its **merge-first stealth-development policy**. Honor any repository-local `AGENTS.md`, explicit holds, and GitHub-enforced rules.

# Oreslang Durable Objects (LLVM)

## AttoLang migration and authoritative specification (2026-11-18)

**AttoLang** is the current public name of Oreslang. This `ores-truffle-oreslang/oreslang-durable-objects.ores` repository retains its historical context. **No verified one-to-one successor:** consult [atto-lang](https://github.com/atto-lang) before linking or migrating this project. Legacy source and commands below reflect this checkout; do not automatically substitute new CLI names or claim feature parity. These repositories are not assumed to have identical Git histories, issues, pull requests, or implementation results.

**The canonical language specification is [atto-lang/atto-spec](https://github.com/atto-lang/atto-spec)**. Portable `.atto` modules belong in [atto-common-source](https://github.com/atto-lang/atto-common-source) and executed cross-backend positive/negative fixtures in [atto-platform-conformance](https://github.com/atto-lang/atto-platform-conformance). The proposed modern rules include `snake_case` methods, ownership `borrow!`/`take!`/`share!`/`copy!`/`copy!!`, [implicit borrowing](https://github.com/atto-lang/atto-spec/blob/main/language/implicit-argument-borrow-v1.md), [file/module constants including nonthrowing launch-time environment reads](https://github.com/atto-lang/atto-spec/blob/main/language/file-module-const-v1.md), and [separate bitwise-shift and directional-stream operators](https://github.com/atto-lang/atto-spec/blob/main/language/bitwise-and-stream-operators-v1.md). These are contract/design directions, not a claim this legacy implementation supports all syntax.

Preserve historic `.ores` paths, command names, ABI/wire names and pinned fixtures until verified replacements exist. Avoid Python support tooling/tests; prefer Rust. Do not infer successful compilation, actor isolation or cross-backend parity from README examples, host-only tests, or zero-step GitHub Actions. Document exact commit/tree, target, toolchain, executed guest tests and explicit failures/unsupported status.

Status: **draft contract and native LLVM host substrate**, not a production durable-object service.

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

## Legacy migration provenance and gates

The original `ores-truffle-oreslang/oreslang-durable-objects.ores` repository is archived; the current repository is `atto-lang/atto-durable-objects.atto`. This is a distinct repository identity, not proof of an original-ID transfer or equivalent feature support. Preserve historical `.ores` source, CLI, package, ABI and API identifiers until separately verified replacements exist. Guest durable object identity, actor execution/storage contracts and tenant-fencing fixtures belong here; commercial hosted control planes, billing and storage administration do not.

[atto-lang](https://github.com/atto-lang) owns portable language libraries, compilers, tooling, tests and examples; [atto-co](https://github.com/atto-co) owns hosted deployment, tenant infrastructure, identity and billing. A future original-ID transfer or product carve-out requires separate source/target owner and independent security approval. Inventory exact SHA/tree, numeric ID, visibility, permissions, protections, PRs, releases, packages, dependencies, runners, secrets, applications, webhooks, Pages and rollback owner; verify preservation and rerun exact-head tests after an actual transfer. Never mirror private source without authorization. Zero-step jobs are NOT_EXECUTED; host-only checks do not establish native guest/compiler execution or production safety.
