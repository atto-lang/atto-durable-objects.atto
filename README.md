# Oreslang Durable Objects (LLVM)

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

## Repository ownership and migration (2026-11-08)

**Public language:** AttoLang; **current legacy home:** `ores-truffle-oreslang/oreslang-durable-objects.ores`. Preserve old `.ores` files, `oreslang-*` repository, CLI, package and API names as compatibility surfaces pending a separately validated change.

- **Belongs in this repository:** guest-level durable object identity, actor execution/storage contract, tenant-fencing fixtures.
- **Does not belong here:** commercial hosted durable-object control plane, billing or storage administration.
- **Organization boundary:** [atto-lang](https://github.com/atto-lang) hosts portable language semantics, compiler targets, standard libraries, SDKs, tooling, conformance and examples. [atto-co](https://github.com/atto-co) separately owns managed cloud deployment, identity, billing, tenant hosting, registries and commercial services. A pre-existing similarly named `atto-*` repo is not assumed to be a transferred copy of this Git repository.
- **Initial migration proposal:** transfer this repository under the same name/ID to `atto-lang/oreslang-durable-objects.ores` **only after** approval; no direct move from this old org into `atto-co` is currently authorized. Any future AttoCo product carve-out requires its own owner/security/consumer review.

**Maintainer migration checklist:** coordinate ownership and exact source head in [issue #26](https://github.com/ores-truffle-oreslang/.github/issues/26); follow the private [migration runbook draft #25](https://github.com/ores-truffle-oreslang/.github/pull/25). Inventory numeric repo ID and exact SHA/tree, PRs/releases, branch protections/rulesets, target visibility and permissions, Actions runners, secrets, webhooks, GitHub Apps, packages, Pages and consumer URLs; record source/target admin, independent security review and rollback owner. Never cross-org mirror private source without separate authorization. Perform only an authorized GitHub identity-preserving transfer; verify original ID/history/tree and restore controls afterward, update references incrementally and rerun **executed exact-head tests**. `steps=[]`/`null` is `NOT_EXECUTED`; host/static checks are not guest execution evidence or production safety proof.
