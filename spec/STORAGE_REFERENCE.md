# SQLite/WAL reference storage adapter (draft)

The `src/native/SqliteDurableStore.hpp` adapter is a **trusted, host-only** prototype. It is not yet a native Oreslang `.ores` API, compiler binding, distributed lease service, process sandbox, or production database.

It implements an authoritative SQLite WAL store with FULL synchronous commits, `(tenant, namespace, object_id)` composite keys, monotonic object epochs, single-key data updates, persisted response deduplication and optional persisted alarm schedule in one SQL transaction. It verifies epoch before transaction changes. A multi-threaded host serializes use of one SQLite connection; different storage connections rely on SQLite locking.

## Local test

```sh
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

The local GCC/Clang C++17 tests and Clang ASan/UBSan/leak test passed before publishing. The tests assert open-close-reopen persistence, rollback on injected failure, stale epoch denial, tenant key separation, replay response consistency, alarm persistence, and concurrent duplicate operation deduplication.

## Critical remaining release blockers

- The epoch is a **fencing token**, not an authenticated distributed lease. A separate authority must issue exclusive ownership epochs; remote replicas and consensus are not implemented.
- The interface accepts host-provided tenant identifiers; authentication, capabilities and secure filesystem path admission must precede calling it.
- No transactional outbox, actual alarm scheduler, message delivery retry, schema migration/version negotiation, ciphertext-at-rest, replication, backup recovery, quota on database growth, process crash while writing, or filesystem fault-injection evidence.
- `fail_before_commit` is a test-only injection hook; must be isolated from production guest-facing APIs.
- Do not connect it directly to untrusted native code or claim exactly-once delivery or guaranteed availability from this demonstration.

## Idempotency identity hardening

A replay is accepted only when the tenant, namespace, object, operation key, data key, value and optional alarm name/deadline exactly match the original committed request. The prior committed **reply** is returned even if a later caller recomputes a different provisional reply. Reusing an operation key for different inputs is a hard error, never a silent deduplication hit.

New requests persist a `dedup_requests` identity row in the same transaction as the reply receipt, data update, and alarm. An older database with legacy dedup receipts but no verifiable request-identity rows is refused at open; there is no automatic, lossy migration. Restore or migrate only with independently verified original request data. This reference implementation still lacks persistent size budgets, outbox and authenticated guest storage capabilities.
