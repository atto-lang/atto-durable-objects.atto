# Durable Objects API v0 — LLVM-first

## Identity and topology

A durable object is identified by `(tenant_id, namespace, object_id)`.
A trusted host authenticates `tenant_id`. A tenant has exactly **one**
LLVM `IsolateVM`; it may contain many FIFO queues, one per `lock_id`.
A queue admits at most one live handler/lease at a time. Distinct lock IDs
may execute concurrently in the same tenant isolate.

A durable object is permanently assigned to one `lock_id` in v0; changing
it requires an explicit migration. The native implementation uses a
canonical string `object_id` representing `namespace/object_id`, and
never exposes the tenant SQLite handle or raw isolate memory to guests.

## Proposed Oreslang facade (NOT YET COMPILED)

```ores
let counter = durable_objects.namespace("counters").get("counter-17")
let result = await counter.enqueue(
  lock_id: "counter-17",
  request_id: "request-unique-991",
  message: #increment,
  payload: 1
)
```

This is proposed API syntax, not an implemented parser/FFI. Source methods
follow `snake_case`, actor state remains sealed, and admission returns a
`Result<AdmissionReceipt, DurableError>` rather than silently blocking.

## Native host API contract

| Method | Semantics |
|---|---|
| `open_tenant(id)` | Issue generation-fenced tenant token; reject duplicate process owner |
| `enqueue(tenant, object, lock_id, request_id, payload)` | Transactional admission plus durable request deduplication |
| `try_claim(tenant, lock_id)` | Issue move-only, fenced lease for FIFO head, if lock idle |
| `read_state(lease)` | Fetch bound object's version and bytes |
| `complete(lease, next_state)` | Atomic object-version update plus queue ACK |
| `retry(lease)` | Return the job to FIFO with a new fencing token |
| `close_tenant(tenant)` | Stop in-process leader; reject active leases but retain queued work |

`Accepted` is **admission**, not completion; `Duplicate` returns the
original sequence for an already accepted or completed identical request.
A request ID reused with different payload, object or lock is rejected.
The dedupe table is retained and bounded; there is no automatic eviction.

Work delivery is at-least-once. Object state update and ACK are atomic
within the tenant SQLite WAL database (`synchronous=FULL`). External side
effects are NOT exactly-once: use idempotent remote APIs or an eventual
outbox. Unacked jobs recover on restart. No dead-letter alarms, transaction
timeout, CPU fairness, process-sandbox proof, cross-host failover, or
compiled `.ores` guest dispatch is implied by v0.
