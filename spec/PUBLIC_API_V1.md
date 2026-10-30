# Native Oreslang Durable Objects API — proposed v1 surface

Status: **normative design proposal**, not yet accepted/compiled by the LLVM source compiler. Native API definitions should ultimately reside in `ores.std` / `ores.alloc` modules in `.ores`; the host code supplies only the minimal versioned native primitives.

## Types

```text
DurableObjectId                  // immutable, stable, namespace-qualified ID
DurableNamespace<T>              // authenticated namespace handle
DurableStub<T>                   // serializable remote object capability
DurableObjectContext             // actor-owned context, no global access
DurableStorage                   // transactional object-scoped KV store
DurableTransaction               // confined transaction capability
DurableLockId                    // opaque tenant-scoped lock key
DurableQueueTicket               // opaque generation-fenced scheduler ticket
DurableAlarmId                   // persisted alarm identity
```

## Proposed signatures (pseudocode, not executable .ores)

```text
namespace.id_from_name(name: string): DurableObjectId
namespace.id_from_string(id: string): Result<DurableObjectId, IdError>
namespace.get(id: DurableObjectId): DurableStub<T>

stub.send(message: T.Message, idempotency_key: string): Future<Result<T.Reply, DeliveryError>>
stub.fetch(request: HttpRequest): Future<Result<HttpResponse, DeliveryError>>

context.id: DurableObjectId
context.storage: DurableStorage
context.with_lock<T>(lock: DurableLockId, action: suspends fnc(): T): Future<Result<T, LockError>>
context.set_alarm(deadline: Instant, idempotency_key: string): Future<Result<DurableAlarmId, AlarmError>>
context.delete_alarm(alarm: DurableAlarmId): Future<Result<void, AlarmError>>

storage.get<T>(key: string): Future<Result<Option<T>, StorageError>>
storage.put<T>(key: string, value: T): Future<Result<void, StorageError>>
storage.delete(key: string): Future<Result<bool, StorageError>>
storage.transaction<T>(action: fnc(DurableTransaction): Result<T, StorageError>): Future<Result<T, StorageError>>
```

Generics, effects, async syntax and capabilities must be reconciled with actual Oreslang grammar and compile-time effect rules. Nested Future/Option/Result values are **never automatically flattened**. No blocking IO and no implicit actor reentrancy.

## Behavioral guarantees and restrictions

1. A `DurableNamespace<T>` is authorized for a principal. `id_from_name` uses a deterministic collision-resistant and collision-checked namespace encoding. `get` creates a logical stub, not a new process or native pointer.
2. `DurableObjectContext` exists only inside a sealed isolated actor turn, cannot escape its actor lifetime, and does not grant authority to mutate other tenants. The source actor uses `state` and sealed `handle_state` with `critical` for mutation.
3. `with_lock` queues by **tenant-scoped LockId** with FIFO fairness. Cancellation removes only unstarted waiters. It does not execute callback code under any C++ mutex. A suspended invocation cannot reenter its actor; a lock acquired inside a turn cannot outlive the scheduler-owned continuation token.
4. For one actor, a second lock is not a bypass around the non-reentrant handler rule. Execution is serialized by actor ownership even when different lock queues have runnable heads.
5. Storage `transaction` persists data atomically or fails, without arbitrary network calls in an open transaction. External side effects require a transactional outbox and replay-safe idempotency.
6. Stub message delivery is **at least once**, with persisted deduplication and retry policy, unless a stronger guarantee is independently verified. No implicit exactly-once claim.
7. Timers/alarms persist with an epoch and idempotency key, survive process restart, and are checked against both monotonic scheduler deadlines and recovered wall time.
8. `DurableLockId` is an opaque host/compiler-validated value. Guest data cannot fabricate other tenants' lock capabilities. The queue substrate currently uses `uint64_t` only internally; publishing it as an unchecked user argument is explicitly forbidden.
9. Persistent state is an authoritative database/WAL, not the IsolateVM heap. Isolate destruction and migration must not delete durable state by default.
10. Only `SandboxedProcess` may host hostile LLVM AOT guest code; `TrustedInProcess` provides logical ownership but **not** a native security boundary.

## First implementation milestone

The LLVM host-side `DurableLockQueues` substrate in [LLVM PR #1169](https://github.com/ores-truffle-oreslang/oreslang-source.llvm/pull/1169) implements bounded, tenant-owned queue admission/release/cancel. The native public API above is **not yet compiled or connected**. Required next: typed compiler-lowered handles, authenticated ActorTicket, scheduler dispatch, storage and alarm drivers, OS isolation and exact-head CI.
