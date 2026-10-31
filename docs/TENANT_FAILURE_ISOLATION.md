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
healthy tenant B completion, no new A admission/retry, and restart-time
**atomic state+ACK** verification. A failed fsync can have an **ambiguous
commit outcome**: either the request remains pending with unchanged state
and a strictly higher recovery fence, or the request was fully committed
and both state and ACK must be visible. Partial state/ACK is rejected.
The VFS is process-global but the fixture runs in its own executable
and restores the original VFS. A single
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

## Follow-up: transaction admission and lease-claim COMMIT ambiguity

Synthetic `EnqueueBeforeCommit` and `ClaimBeforeCommit` exceptions now quarantine the relevant tenant, rather than leaving a committed-but-unindexed queue or a claimed-without-issued-lease row accessible to later same-process operations. The preallocated per-lock index is retained on uncertain enqueue COMMIT, and claim has not issued any Lease. The normal recovery pass must resolve persistence and advance in-flight fences on process restart. These synthetic hooks run *before* SQLite COMMIT and do not demonstrate what a failed VFS `xSync` actually committed; separate real VFS injection at enqueue/claim COMMIT is a required next gate.

On fallback `Tenant::~Tenant()` disposal, the destructor no longer writes directly to stderr: a closed pipe can send SIGPIPE or a full pipe can block. If SQLite close or flock unlock fails, the process deliberately retains the OS flock/descriptor until process exit to avoid prematurely admitting another process owner. This may leak resources on a truly failing shutdown path; the explicit host `close_tenant()` path is the observable disposal API. No production-safe durable supervisor incident ledger is yet implemented.

## Integrated VFS test additions (source only; no exact-head execution yet)

The real WAL `xWrite`/`xSync` fault harness now also targets **enqueue admission** and **try_claim**, not only ACK, with a separate tenant-B successful completion and subsequent tenant-A recovery. Runtime catches SQL `std::runtime_error` from BEGIN, prepare, statements, and COMMIT in both operations and sets atomic `AdmissionStorageFailure`/`ClaimStorageFailure` quarantine flags. This avoids continuing a live runtime after an ambiguous write/commit outcome. These tests are intentionally strict about eventual complete/pending queue consistency, but do **not** assume whether an xSync failure committed before error. They are still uncompiled/unexecuted on the repository exact head; zero-step Actions do not validate them.


## POSIX singleton lock release ordering

The `Tenant` destructor and explicit `close_tenant` no longer run `flock(LOCK_UN)` before `close(fd)`. The previous ordering risked unlocking the singleton while retaining an open descriptor if the subsequent close failed. Normal descriptor close now performs release. The legacy `LockUnlock` injection remains a synthetic refusal before release, not an actual failing `flock(LOCK_UN)` syscall; it must not be described as proof of real POSIX unlock-error handling. Ambiguous `close(fd)` failures still require external process-level evidence and review.


## Read-path containment and offline exact-head verifier

`read_state` and `pending` now quarantine the affected tenant when SQLite
throws `std::runtime_error`. A failed read must never be treated as a
reliable actor snapshot from which a subsequent ACK may be constructed.
Synthetic `ReadState` and `PendingQuery` fault fixtures exercise refusal
of further work, healthy-tenant progress, and restart fencing. These are
**not** real SQLite `xRead` failures; real VFS read/corruption cases remain
required. An intentionally missing object binding remains a separate
`std::logic_error`, not a proven disk I/O error.

The script `scripts/verify-durable-native.sh` runs native Debug/Release CTest
under GCC and Clang, plus GCC/Clang ASan+UBSan and GCC TSan. It rejects a
missing/mismatched 40-character commit SHA and dirty source checkout, builds
under a directory outside the source tree, and retains a provenance receipt,
per-profile logs and explicit pass/fail marker. Run only on an independently
authorized machine with a checkout of the exact PR head:

```sh
EXPECTED_HEAD_SHA='<40-character reviewed PR HEAD SHA>' \
  bash scripts/verify-durable-native.sh
```

A TSan runtime failure is a failing gate unless a separately reviewed,
supported-environment rerun supplies legitimate execution receipts. Logs
may contain private local paths; do not upload them to external carriers
without authorization. No compiler/CTest or sanitizer execution on this
complete GitHub tree has been performed in the current session.



## Competing-process tenant ownership regression

`durable_tenant_fault_quarantine_test` now executes two owner-fencing checks
after each synthetic explicit-disposal failure (SQLite close, pre-release
lock fault, and file-close fault). A second in-process runtime must fail to
open tenant A, and a forked child process using a new runtime must fail
**specifically** with the singleton-owner-lock rejection, not an unrelated
SQLite/filesystem error. The child uses `_exit` to avoid unwinding inherited
SQLite state. Tenant B must still complete its queue work, and tenant A's
persisted state is checked on subsequent controlled restart.

The forked contender executes against the same POSIX host/kernel and is not
a multi-host isolation proof. The test source is committed but native CTest
execution is still required; zero-step GitHub Actions are not test receipts.


## Durable unclean-session incident sidecar (unexecuted native patch)

The host now pre-arms a private, fsynced `tenant-*.sqlite.incident`
marker (including parent-directory fsync) before the first SQLite access.
Its presence on restart blocks ordinary `open_tenant`, so an unexpected
shutdown or failed noexcept disposal cannot be mistaken for clean release.
The marker is removed only after successful explicit SQLite/owner-fd close,
and the containing directory is fsynced. A failed close leaves the sidecar.

`recover_tenant` is a trusted host supervisor-only method. Under the
exclusive tenant flock, it requires a valid marker and existing database,
runs `PRAGMA integrity_check`, and only then invokes existing recovery
which fences persisted unfinished leases. The new session retains the
marker until successful close; recovery is never invoked automatically.
An ordinary crash also requires explicit supervisor recovery. This is a
durable unclean-session indicator, *not* a durable reason-code event log,
distributed authority, or proof of OS isolation. Native crash/fsync,
disk-full, sanitizer, and exact-tree testing remain mandatory.


## P0: guard the singleton lock during incident-marker retirement

The original retirement sequence closed the owner fd (releasing `flock`)
**before** removing the `.incident` sidecar. A contender could acquire the
flock and enter supervised recovery while the closing host still intended
to unlink the marker, causing the new session to run without its incident
record. Merely relying on marker presence is insufficient because a trusted
supervisor is allowed to recover a marked tenant.

The corrected path duplicates the locked file descriptor using
`F_DUPFD_CLOEXEC` (same open-file-description `flock`) before closing
the primary fd. It keeps the duplicate until **after** unlink and successful
directory `fsync`. An interruption or failure before unlink retains the
marker and the locked duplicate; a failure after unlink happens only after
SQLite was fully closed and leaves no admitted guest work. It never retries
an fd number after an ambiguous `close()` result. The synthetic
`IncidentUnlink` failure injects an interruption at the critical boundary,
and the existing competing-process test requires rejection specifically at
the POSIX owner fence before permitted supervised recovery.

This is **source-level hardening only**. It has not passed the exact-head
native CTest, sanitizer, or concurrent recovery stress suite, which remain
mandatory release gates. These locks protect processes on the same POSIX
host, not distributed nodes.


## Independent-process lock-gate acceptance

A plain `fork()` does not prove that another host process is fenced:
the child inherits the parent's locked open-file description. The native
`durable_tenant_fault_quarantine_test` therefore forks **and execs**
`/proc/self/exe` (Linux CI target) for its competing-owner checks. The
tenant lock descriptor has `O_CLOEXEC`, so the probe starts without the
parent's inherited flock. During each injected close/incident-retirement
failure, the fresh process must fail **specifically** on the singleton
owner-lock check, not on a SQLite error or an incident marker. After the
original runtime has shut down and a supervisor has explicitly recovered
and cleanly closed the tenant, a second fresh executable must also be
able to open and close that tenant. The negative and positive cases
ensure both that a lock is not released early and that it does not
remain unnecessarily held after clean retirement.

This test is host POSIX/SQLite-only and remains unexecuted until the
exact-head native CTest gate runs; a standalone POSIX reproducer is not
a runtime receipt or Oreslang guest-isolation evidence.

## Incident marker validation and failed retirement sync (2026-10-31)

The incident-marker reader now uses `O_NONBLOCK` with `O_NOFOLLOW`, then
`fstat` checks that the entry is a private, owner-owned, zero-length regular
file. A forged FIFO must be rejected without blocking, and symlinks are not
followed. The lifecycle regression checks clean close, unclean destructor
exit, explicit recovery, missing database, forged symlink, and FIFO marker.

If marker unlink succeeds but the parent-directory fsync fails, the runtime
retains the duplicate singleton-lock guard, quarantines the tenant, and
attempts to recreate and fsync the incident marker before unwinding. A new
`IncidentDirSync` test seam exercises this exact failure interval, including
another process's denied owner claim, healthy-tenant progress, and subsequent
supervisor recovery. This is a compensating **best-effort** action: if the
filesystem also rejects the repair, durable incident persistence cannot be
proven, and promotion must remain blocked.

The final owner-guard close occurs *after* marker retirement. POSIX
`close(2)` failures can be ambiguous; there is no proof here that a failure
after a successfully fsynced unlink is durably recorded across process death.
This still requires a reviewed lifecycle protocol and executable fault
receipts. Do not treat source-only tests, POSIX standalone probes, or zero-step
GitHub Actions as completed native integration verification.


## Effective SQLite durability policy is now fail-closed

At every new tenant admission and supervised recovery, schema initialization
reads and verifies the **actual** `PRAGMA journal_mode=WAL` result before any
schema or queue mutations. It also reads the effective, connection-local
`PRAGMA synchronous` value (must be `2`, SQLite FULL) and
`PRAGMA foreign_keys` (must be `1`). A successful PRAGMA command alone
does not establish that SQLite applied the requested setting; unexpected
values throw before admitting any guest queue work.

`durable_tenant_fault_quarantine_test` independently opens the newly
created tenant database and verifies that the persisted journal mode is
`wal`. The separate connection is intentionally **not** used to test
synchronous FULL, because that setting is connection-local.

A standalone SQLite 3.46.1 C++ smoke probe compiled and ran using GCC 14.2
and Clang 17 with ASan+UBSan and confirmed journal-mode row handling,
synchronous=2, foreign_keys=1, and the second-connection WAL readback.
This is *not* exact-head native-runtime execution; the full CTest and
sanitizer gates remain blocked by GitHub Actions runner admission.


## Final guard release fault seam and remaining durability boundary

The host-only fault point `IncidentGuardRelease` interrupts explicit close
**before** the final duplicated `flock` guard is closed, and **before**
retirement of the already-fsynced `.retiring` witness. At this point the
SQLite connection is closed and the original incident marker has been
unlinked and directory-fsynced. The runtime quarantines the tenant,
attempts to recreate and fsync the incident marker while the singleton
flock is still held, then rejects the close. The witness remains durable
even if this optional marker re-arm also fails. The existing fork-plus-exec
contender checks the exact singleton lock refusal, while tenant B continues;
recovery after host teardown requires the persistent marker and the
integrity/fence checks.

This is intentionally a *pre-close* failure seam. A real error returned
from the final `close(2)` is ambiguous: the descriptor may already be
closed, and recreating a marker after that point could race another owner.
No claim of durable error logging or hard isolation is made for this edge.
An independent supervisor admission authority is required if policy must
distinguish a failed last-close syscall from an otherwise clean handoff
after process restart. Similarly, if both marker directory sync and its
compensating re-arm fail, the local filesystem cannot certify a durable
incident. These remain P0 design/review gates, not runtime tests passed.


## Durable retirement witness — interruption-safe admission

The explicit close path now pre-arms and directory-fsyncs a private
`tenant-*.sqlite.retiring` witness **before** unlinking the durable
`.incident` marker. The singleton flock remains held during both
retirements. A subsequent ordinary `open_tenant` rejects a remaining
witness even when `.incident` was already unlinked; a failed incident
directory fsync or compensating re-arm cannot erase the previously
synchronized witness.

Trusted `recover_tenant` under the singleton flock validates an existing
witness as owner-only regular storage, ensures a durable `.incident`
marker exists, and only then retires the witness. Recovery additionally
requires an existing tenant database, an SQLite integrity check, and
persisted lease-fence advancement. Normal clean close removes and fsyncs
both markers before releasing the final guard. A deterministic fixture
models a lost process after witness creation and incident removal; the
expected outcome is fail-closed ordinary admission and controlled recovery.

**Residual limits:** This is a same-filesystem two-phase recovery protocol,
not an independent fault domain. Its durability depends on successful
marker/witness fsync and normal POSIX filesystem semantics. If the
underlying filesystem corrupts or loses already synchronized data, an
independent supervisor authority is required. A final guard `close(2)`
error remains ambiguous after clean retirements; the new
`IncidentGuardRelease` fault seam operates *before* that syscall and is
not proof of an actual close-error outcome. Native build, exact-head
CTest/sanitizer results, real fault injection, independent security
approval and protected `main` are still required.

## Transactional persisted-metadata recovery gate (2026-10-31)

The queue substrate previously recovered `status=1` rows and advanced fences
outside an atomic schema/recovery transaction. It also lacked the stricter
persisted-job, object-lock and idempotency-receipt validation present in the
separate draft PR #3. The source-only hardening now makes recovery a single
`BEGIN IMMEDIATE` / `COMMIT` transaction after verifying effective SQLite
WAL/FULL/foreign-key settings, and validates persisted counter storage classes,
nonnegative versions/attempts/fences, legal job statuses, object-to-lock binding,
job-to-receipt identity, historical-receipt aliasing and near-overflow fences
**before** applying recovery updates. A failed validation must roll back without
partially requeuing an unrelated in-flight job.

The `durable_persisted_fence_guard` fixture now contains the malformed-record
regressions from draft PR #3 plus a two-job atomic-recovery refusal case: one
in-flight job at fence 7 and another with a missing receipt. The test requires
startup to reject corruption and verifies the first job retains status 1 and
fence 7 after that failed admission. This is a **source-only test addition**;
no native execution is claimed. The transferred validation/test logic should
be reconciled with PR #3 before merging either draft.

New workflow runs were triggered for the preceding commit
`13a29cadcc76f6560e5bcfba62a456771f8403aa` (runs 41231714631,
41231714636 and 41231714554); sampled no-checkout, SQLite, Debug and verifier
jobs returned `steps=[]`. They are runner-admission failures, not evidence
that any source test ran. The final commit after this document update requires
a fresh exact-head, executed-step CI receipt, sanitizer coverage, and independent
review before promotion.
