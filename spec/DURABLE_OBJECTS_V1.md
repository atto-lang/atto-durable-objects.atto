# Durable Objects v1: normative design contract

## Identity and lifecycle

- Logical key: `(authenticated_tenant, namespace, object_id)`; never infer the tenant from an untrusted payload.
- Runtime execution key: `(tenant, isolate_generation, actor_id, actor_generation)`. Tenant reuse must not revive stale handles.
- Exactly one IsolateVM per admitted tenant principal in one OresVM host instance. Replacements receive a fresh generation. A production distributed cluster must fence old incarnations with a **durable lease/epoch** before activation elsewhere.
- Each isolate contains an independent lock registry `Map<LockId, FIFO>`. `LockId` is an opaque, nonzero tenant-scoped ID; assignment from object ID and user lock name must be collision-free (or collision-checked) and authorized. Identical numeric IDs in different tenants never share a queue.
- Each object is mapped to a lock domain for its sealed state; extra locks for subresources are permitted. No default simultaneous turns on the **same actor**, even if lock IDs differ. Non-reentrant handlers remain mandatory.

## Lock-queue protocol

1. `enqueue(lock_id, operation_id, payload_bytes)` is bounded by tenant-wide queue entries, queued bytes, live lock count and per-lock depth. Admission is transactional: on failure no slot or bytes are charged.
2. Exactly one ticket per lock is runnable, the FIFO head. All later tickets await scheduler wakeups; **no carrier thread blocks** waiting on a logical lock.
3. A ticket identifies isolate generation, lock ID, unique sequence and operation ID. Only the exact head ticket can release its lock. Replays, non-head release, stale generation and foreign tenant capabilities fail closed.
4. Release hands the next head **to the scheduler**, never calls user code inline. Other lock IDs can admit independently; actual concurrent execution requires safe actor ownership and the scheduler, not merely multiple queues.
5. Cancellation can remove an unstarted waiter. The active ticket is retired only after its callback, I/O and continuation have **physically stopped**, before release.
6. Quiescing rejects new admissions; draining requires all queued/active work and externally pinned resources to retire. A lock queue is ephemeral scheduling state and **must not be confused with a durable delivery log**.
7. Operation IDs prevent duplicate *simultaneously pending* work only. Persisted idempotency keys and inbox/outbox entries are mandatory for crash/retry deduplication.

## Persistent storage and alarms (not implemented)

- Storage uses an authoritative durable backing store and per-object serializable transactions or a clearly documented weaker isolation mode. Commit must be atomic across updates and outgoing message intent where transactional outbox is supported.
- On crash, uncommitted mutations roll back; committed ones survive restart. Delivery is at least once with deduplication, unless a stronger end-to-end protocol is proved. No exactly-once network promise.
- Record transaction/lease epoch, schema version, idempotency keys and alarm schedule durably. Do not persist raw native pointers, isolate handles, actor borrows or OS descriptors.
- Alarms are persisted and idempotent, with monotonic runtime scheduling plus wall-clock normalization at recovery. Preserve explicit duplicate/retry behavior.

## Security and resource policy

- Root OresVM authorizes tenant, storage namespace, object identity, capabilities, memory, CPU, queue entries/bytes and IO budgets.
- IsolateVM owns its own native heap/arenas, module globals, actor registry, queues, continuations, and cleanup. Physical isolation of memory **ownership** alone does not contain native memory corruption.
- For mutually untrusted native tenants, `TrustedInProcess` is forbidden; require real sandboxed processes with authenticated IPC, capabilities and per-tenant crash containment. Fail closed if no sandbox is available.
- Cross-tenant messaging is capability-checked and encoded/copy-based by default. No sharing mutable memory or borrowed references.
- Compiler must enforce non-reentrant actor handlers, actor state via sealed `handle_state` / `critical`, no suspension inside hard real-time paths, no implicit conversion or operator overload.
- Critical sections and lock acquisition must be fair and cancellation-safe. Never claim hard-real-time maximum latency without WCET/admission guarantees.

## Versioning

The durable API requires a versioned guest/runtime ABI. LLVM draft #1158 currently uses an experimental scalar guest entry ABI v2. Do **not** pass new durable-queue guest calls over that ABI or reuse its function pointer signature without deliberate ABI negotiation and source compiler integration.
