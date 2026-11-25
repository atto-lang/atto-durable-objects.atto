# Durable Objects guest integration — first vertical slice

Status: source candidate, NOT EXECUTED. The existing C++ SQLite host is not a
compiled AttoLang guest implementation. This slice adds a deterministic
bounded guest transition, not a distributed service or a storage binding.

## Architecture and authority

An authenticated ingress resolves (tenant, namespace, object_id); a trusted
broker owns routing, generation, lease, revision and request-id admission.
A durable virtual actor is lazily activated from committed state and runs
one turn at a time, even across suspensions. Different objects can progress
independently subject to quotas. Never expose host SQLite handles, raw
pointers, or ownership epochs as user-authoritative data.

First application: a counter. The portable guest function
[durable_counter_next](../guest/durable_counter.atto) proposes a next state
from a snapshot, operation and delta; it rejects malformed and overflowing
commands before arithmetic. The scalar rejection sentinel is local to this
fixture, not the public RPC ABI. The production ABI must use typed Result
and versioned bounded serialized envelopes.

The target actor has sealed state, private runtime-only init/receive,
and short critical sections for installing committed snapshots. No await,
network or storage inside a critical section. The durable turn broker owns
the lease across any suspending work; no mutable borrow crosses suspension.
The guest cannot publish a success or mutate the authoritative durable
state directly.

## Commit-before-publication protocol

1. Authenticate and authorize identity and operation; reject oversized
   messages before allocation. Resolve durable object and fenced owner.
2. Admit a request with immutable operation identity, schema and payload.
   A matching retry returns the recorded result; a conflicting retry fails.
3. Read committed snapshot under the lease. Run guest transition. Rejected
   transition cannot mutate state or emit success.
4. Commit using owner epoch and expected revision; atomically persist state,
   request receipt, reply and optional alarm/outbox intent. Read-only requests
   may avoid a revision increment only when their reply is safely recorded
   under the same ownership and consistency contract.
5. Only after commit, install the returned committed snapshot and publish the
   response. If outcome is ambiguous, invalidate the cached state, reload
   authoritative storage and resolve the request identity; never guess.
6. A crash before commit leaves old state; a crash after commit before ACK
   recovers the persisted reply. Outbox dispatch is at least once; consumers
   need idempotency. Stale generations cannot write or release a new lease.
7. Passivation requires no active/suspended turn or pinned resource; discard
   cache and reload on next activation. No correctness dependency on a
   shutdown callback.

## Cloudflare behavior targets

Reference: https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/
and https://developers.cloudflare.com/durable-objects/concepts/durable-object-lifecycle/.

P0: stable name and capability-based stub, activation, strict non-reentrancy,
transactional state/receipt, commit-before-reply, bounded RPC, crash recovery.
P1: durable alarms with generation-tagged delivery and retry; KV and
parameterized SQLite interfaces, transaction rollback, passivation; HTTP
routing; authenticated serialization and tenant process isolation.
P2: host-held WebSocket connections and attachment recovery during
hibernation, placement/replication/lease authority, upgrades and telemetry.
Cloudflare permits certain async interleaving; Atto intentionally chooses
strict non-reentrant actor turns rather than promise exact JS semantics.

## Execution gates

Run the real guest self-test (return 0; 12 assertions) against pinned
Java/GraalVM, LLVM, .NET NativeAOT, Wasm and Cranelift compilers. Until then
all five are NOT EXECUTED. No substitute Rust/C++ evaluation or text scan
counts. Next implement a versioned broker adapter and test:

- Same identity repeated activation; different tenant/namespace isolation.
- 100 concurrent increment calls: final state 100; each unique committed
  operation applied once; same-object handler overlap zero.
- Duplicate same request returns original reply, changed payload/schema
  rejected; no state change on invalid guest operation.
- Crash before commit and after commit before response, restart/retry.
- Stale generation, wrong tenant, stale revision and wrong receipt rejected;
  no success/outbox publication before commit.
- Lease cancellation waits for physical turn retirement; no new same-object
  turn while a suspended turn is active; independent object makes progress.
- Idle passivation reloads committed snapshot; alarms survive restart.
- Negative compiler cases: external call to receive/init, mutable state
  escape, suspension in critical scope, forged host capability.
- Record exact repository and compiler head/tree SHA, commands, output,
  target, nonzero executed steps and negative diagnostics. No merge on
  zero-step CI, no source-only claims.

## Existing PR coordination

PR #9 is the host actor sync/receipt/outbox reference; use it rather than
reimplementing its storage. #5 crash-before-ACK tests, #6 idempotency
binding, #7 quarantine, #8 LLVM drain, #12 exact-head CI are prerequisites
or complementary safety work. #10 and #11 are workflow replay extensions,
not a guest adapter; #13 is migration documentation. This new guest slice
does not replace or merge those branches. Legacy equivalents #3, #5, #7,
#8, #11, #12, #13, #15, #16 were inspected by title and scope; the legacy
repository is archived. Full diff reconciliation and exact-head CI remain
required before merge. Keep draft pending compiler execution and review.

No public ABI is introduced by this fixture. Portable library API belongs
in atto-common-source and canonical multi-backend execution in
atto-platform-conformance; backend bindings belong in atto-source.*.

## Wave 2: executable admission and commit adapter (2026-11-24)

- Storage dependency [PR #9](https://github.com/atto-lang/atto-durable-objects.atto/pull/9)
  at 57c15af3b9dfa31c32dbbb459ee824ebb5b06390 adds read-only fenced
  receipt lookup before guest execution, including schema/command binding.
  This is necessary when an already-applied operation would now overflow.
- `native/include/ores/durable/counter_turn.hpp` connects a scalar guest
  runner to the existing sync store. It checks canonical state encoding and
  transition bounds, rejects stale ownership, returns previously committed
  replies without re-execution, and never returns a success before commit.
  The host caller must already hold the exclusive per-object turn permit,
  authenticate tenant/capability and execute blocking SQLite on a privileged
  isolated IO worker. Recheck the permit at external publication; the
  adapter alone neither authenticates nor holds the scheduler fence.
- `native/tests/counter_turn_test.cpp` uses an explicitly HOST_ONLY callback,
  tests 100 serial turns, malformed returns, rollback, lost reply after process
  exit, maximum-value dedupe, cross-tenant independence and stale epochs.
  This is not a concurrency test and is not a compiled actor or end-to-end
  guest-to-storage test.
- `tests/durable_counter_guest.mjs` invokes the real Rust attoc CLI,
  emits and instantiates Wasm, checks results and panic code, requires no
  host imports, and demands a compiler diagnostic/no artifact for bad types.
  Run `ATTOC=/absolute/path/to/attoc node tests/durable_counter_guest.mjs`.
  This remains scalar guest evidence only, even if it passes.
- Dedicated CI checks out this exact PR head and the public compiler
  a4c2497decbb3782a1a6f810e8078bbee094ccbb, and independently compiles the
  host test against the pinned storage dependency. No private source or
  cross-org credentials. The two lanes are not proof of integrated guest
  persistence. All results are NOT EXECUTED until actual receipts arrive.
- Related design: conformance #62 and #66 distinguish persistent user hooks
  from runtime-owned durable state; reuse their semantics, do not fork
  syntax. Java #1515 explicitly rejects persistent actor lowering and notes
  the isolated asynchronous IO-worker requirement. Rust compiler CLI supports
  a scalar Wasm subset; no durable actor runtime support inferred.

Next: reconcile #9 with main and required safety PRs, execute both CI lanes,
then connect the real Wasm guest to a fenced turn via a bounded asynchronous
host adapter and replace the test callback with actual guest execution.
No distributed ownership, automatic activation, alarm, or passivation
implementation yet.

## Wave 3: process-integrated compiled guest (2026-11-24)

The pinned Rust/Wasm parser requires `fi` even after a braced conditional;
the fixture now uses that form and canonical `eq`/`neq`. Source review
is not execution evidence.

The real compiler currently exports only a zero-argument `main` for scalar
programs. Rather than silently invent a guest ABI, the new test driver
`native/tests/counter_guest_driver.cpp` and
`tests/durable_counter_process_e2e.mjs` use a bounded per-turn fixture:
a fresh trusted host process reads its fenced SQLite snapshot, emits a
scalar guest request via a pipe, and the test runner compiles the checked-in
AttoLang kernel plus a generated main with bounded integer inputs. It
instantiates the actual emitted Wasm without imports and returns its
computed value to the same process for authoritative commit. No host
transition substitute is used in this lane. The host validates the result,
commits or rejects, and returns only the durable receipt. The test records
source and artifact hashes for each of seven expected guest executions.

The thirteen fixture process invocations exercise activation of two
tenant-scoped identities, ordinary commit, precommit rollback and retry,
SIGKILL after successful commit and before publication, a fresh epoch,
stale-owner rejection, same-request replay without guest execution even
at the maximum count, changed-command rejection, overflow rejection, read,
and independent same-name object in another tenant. This is a serial
test-only harness; it does not provide automatic activation, concurrent
mailbox scheduling, asynchronous I/O, a persistent compiler/guest instance,
security sandboxing, or an externally accessible RPC service. Per-turn
compilation is explicitly not a production performance strategy.

CI invokes the actual pinned compiler and host driver in the guest lane.
No guest or host test is a PASS until its exact-head run executes.
Production next step: add a checked argument/result ABI or actor dispatch
ABI in the compiler, then an authenticated single-owner activation manager
that holds a per-object turn permit across suspended work and publication.
Do not use epoch rotation as a substitute for exclusive activation or a
lease authority. SQLite work belongs on isolated bounded I/O workers.
