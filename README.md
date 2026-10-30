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

The native queue substrate is stacked on [LLVM isolate PR #1158](https://github.com/ores-truffle-oreslang/oreslang-source.llvm/pull/1158), because that branch supplies the private noncopyable tenant IsolateVM and generation-fenced handles. The follow-up PR implements **host-only** bounded in-memory admission and release; it does **not** persist the queue or expose the proposed Oreslang API.

Current limitations: LLVM #1158 still serializes *trusted synchronous scalar guest turns* per tenant, rather than running general actor turns per lock concurrently. A genuine durable service needs a persisted journal/database, single-owner epoch fencing, replica recovery, authenticated ingress, scheduler binding, crash injection and AOT guest lowering. Trusted native in-process isolates are **not security sandboxes**; untrusted tenants require an actual isolated OS worker and authenticated IPC. No cross-organization private-source CI transfer without independent authorization.

## New host-only storage reference
A SQLite/WAL storage adapter with atomic state+dedup+alarm transactions and monotonic epoch checks lives in [src/native/SqliteDurableStore.hpp](src/native/SqliteDurableStore.hpp). It is **not** the Oreslang guest API or a distributed consensus service. The [reference test](tests/SqliteDurableStoreTest.cpp) exercises reopen, rollback, tenant separation, epoch fencing, and concurrent duplicate submissions. Details: [spec/STORAGE_REFERENCE.md](spec/STORAGE_REFERENCE.md).
