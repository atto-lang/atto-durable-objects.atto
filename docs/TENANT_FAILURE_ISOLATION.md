# Tenant storage-failure containment — host reference only

Issue: #6. This patch is a **draft** source-level mitigation, not proof of production tenant isolation or compiler-generated Oreslang execution.

## Contract

* Retry/ACK errors **quarantine only the affected Tenant** with an allocation-free atomic incident bitfield. The failure path does not call a guest callback or terminate the shared host.
* Once quarantined, `check_tenant` and `check_lease` reject enqueue, claims, state reads, ACK, retry and ordinary close. Existing active sequences remain reserved in memory. No successful retry or ACK is fabricated after a storage error.
* Host supervisor code may poll `quarantined_tenants()` or a retained `TenantHandle::incident_flags()`. Quarantine cannot be cleared in-process; a controlled shutdown and inspection/recovery are required. The inspector allocates outside the destructor; destructors only set atomic incident bits and use fixed literal best-effort stderr diagnostics.
* The SQLite DB, WAL and tenant lock file are per-tenant. A healthy tenant is not implicitly stopped or closed by a different tenant's storage fault. This says **nothing** about CPU, memory, hostile-guest or OS sandbox isolation.
* `close_tenant` explicitly checks SQLite close, flock unlock, fd close and surfaces failures, quarantining and retaining the registry entry. The `Tenant` destructor does not throw or call `terminate`, and performs best-effort cleanup. A failed file-descriptor close is never retried because its state may be ambiguous.
* Recovery at subsequent `open_tenant` requeues an in-flight claim with an incremented persistent fence, provided the database is valid and durable. Corrupted databases or exhausted fences must fail to open; **do not** force open, delete WAL/journal, clear a quarantine, or claim recovery was successful without inspection.

## Fault evidence classes

The new native CTest covers **synthetic, synchronous call-boundary failures**, not a patched SQLite engine or VFS:
- SQLITE_IOERR-like error before retry UPDATE; SQLITE_FULL-like error before retry COMMIT.
- Rollback-path diagnostic while a real SQLite ROLLBACK is still attempted (the simulated failure does **not** prove the true VFS rollback failed).
- SQLITE_FULL-like error before ACK COMMIT, with a proof that state/version did not advance.
- Simulated SQLite close, POSIX flock unlock and fd-close errors during explicit host disposal.
- Healthy-tenant progress on another thread; fail-closed operations on the quarantined tenant; restart to pending with a strictly greater fence; and shutdown with an outstanding Lease.

The new test suite forks a native child and calls `_exit(42)` while holding an unacknowledged claim; the parent verifies a pending recovery with a strictly greater fence. This must be **executed on the exact PR head**, with assertions enabled in both Debug and Release. The separate legacy `--crash-child` mode is not itself proof unless a parent launches it.

## Real SQLite WAL VFS fault tests (new, unexecuted in repository)

The opt-in test executable `durable_sqlite_vfs_fault_test` wraps SQLite's
original VFS without modifying the production runtime. It injects actual
`SQLITE_IOERR_WRITE` at WAL `xWrite` and `SQLITE_IOERR_FSYNC` at WAL
`xSync` for **tenant A only**. The test asserts rejected ACK, quarantine,
healthy tenant B completion, no new A admission/retry, and restart with a
strictly higher fence and unchanged state. The VFS is process-global but the
fixture runs in its own executable and restores the original VFS. A single
active wrapper preserves the original VFS callback context.

The VFS **wrapper itself** was locally smoke-tested using stock SQLite 3.46.1
on GCC and Clang C++17 with ASan+UBSan: each injected WAL callback returned a
real SQLite I/O failure without detected sanitizer violations. This does **not**
mean that `TenantRuntime` + wrapper compiled or passed; integrated native
CTest, ASan/UBSan, TSan and runtime recovery results remain **unexecuted** on
the exact GitHub PR head. No private repository source was transferred.

Remaining real-fault coverage includes main-database `xWrite`, checkpoint
and WAL truncation, actual `xClose` failures, partial/failed rollback,
`SQLITE_FULL` from actual storage exhaustion, and failure during initial
schema migration. These are separate gates rather than claimed coverage.

## Remaining mandatory gates

1. Execute the new real WAL `xWrite`/`xSync` fault test against `TenantRuntime` on the exact PR SHA; extend the VFS test to cover `xClose`, rollback, main DB, checkpoint and `SQLITE_FULL` fault paths. Synthetic status injection alone does not meet this gate.
2. Exact-head native CMake/CTest Debug and Release results, GCC/Clang ASan + UBSan, and TSan under supported runner configurations. Preserve the checked-out SHA, job IDs, run attempt and **nonzero executed steps**; `steps=[]` is runner admission failure.
3. Validate crash-before-ACK, DB reopen after injected failures, complete/fence semantics, live-lease shutdown, and healthy tenant progress with the real SQLite fault layer. No success claim from a zero-step job.
4. Independent security review and protected-branch governance before promotion. Private-source transfer into an unrelated CI organization requires independent authorization; do not transfer source to bypass minutes/runner limits.
5. Define and test supervisor incident persistence/acknowledgment outside the in-process diagnostic snapshot. Current incident bits are **volatile**, and best-effort stderr diagnostics are not a durable incident ledger.

The host SQLite reference is not a guest ABI, hostile guest sandbox, distributed lease authority or compiled Oreslang DurableObject implementation.
