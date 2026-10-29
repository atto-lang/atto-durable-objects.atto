# Durable Objects acceptance matrix

| Case | Expected invariant | Status |
| --- | --- | --- |
| Same LockId, three operations | First runnable; subsequent FIFO; non-head release rejected | Host-level C++ test |
| Different LockIds in same isolate | Independently admissible | Host-level C++ test |
| Same LockId across tenants | Disjoint state/ownership; foreign tickets rejected | Host-level C++ test |
| Quota exhaustion | No partial mutations; lock/byte/depth caps enforced | Host-level C++ test |
| Cancellation | Only waiting tickets can be canceled; no duplicate dispatch | Host-level C++ test |
| Stale ticket / ABA | Owner sequence + isolate generation fences | Host-level C++ test |
| Tenant shutdown | New admission denied; no destruction before physical drain | Partial; runtime integration needed |
| Tenant restart + restored state | New generation; durable records recover; stale handles rejected | Not implemented |
| Storage transaction crash | Committed survives, uncommitted does not | Not implemented |
| Idempotent message retry | Duplicates cannot repeat side effects | Not implemented |
| Alarm across restart | Replayed with stable identity and deduplication | Not implemented |
| Native sandbox escape | Adjacent tenants and host survive crash/hostile code | Not implemented |
| LLVM .ores actor compilation | Generated AOT native guest code exercises queue/storage API | Not implemented |
| CI executable jobs | Exact commit SHA, nonzero steps, normal + sanitizer receipts | Pending |

Never promote a host-level queue test into evidence of persisted durability, actual actor scheduling, tenant process security, or full LLVM compiler success.
