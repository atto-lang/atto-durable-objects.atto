# Durable Objects acceptance matrix

| Case | Expected invariant | Status |
| --- | --- | --- |
| Same LockId, three operations | First head claimable; subsequent FIFO; unclaimed finish rejected | Host-level C++ test |
| Different LockIds in same isolate | Independently admissible | Host-level C++ test |
| Same LockId across tenants | Disjoint state/ownership; foreign tickets rejected | Host-level C++ test |
| Quota exhaustion | No partial mutations; lock/byte/depth caps enforced | Host-level C++ test |
| Cancellation | Waiting tickets and unstarted heads cancel; a claimed owner cannot be canceled | Host-level C++ test |
| Stale ticket / ABA | Owner sequence + isolate generation fences | Host-level C++ test |
| Tenant shutdown | New admission and claim denied; queued tickets/actors gate destroy | Host-level integrated source test added, not yet run in full LLVM checkout |
| Tenant restart + restored state | New generation; durable records recover; stale handles rejected | Not implemented |
| Storage transaction reopen/rollback | Committed survives reopen; injected pre-commit failure rolls back | SQLite host-reference test (not a process crash test) |
| Idempotent storage operation retry | Duplicate operation returns stored reply without overwriting state | SQLite host-reference test (not network delivery) |
| Alarm stored across reopen | Persisted alarm deadline still queryable after reopening database | SQLite host-reference test; no timer dispatch |
| Native sandbox escape | Adjacent tenants and host survive crash/hostile code | Not implemented |
| LLVM .ores actor compilation | Generated AOT native guest code exercises queue/storage API | Not implemented |
| CI executable jobs | Exact commit SHA, nonzero steps, normal + sanitizer receipts | Pending |

Never promote a host-level queue test into evidence of persisted durability, actual actor scheduling, tenant process security, or full LLVM compiler success.
