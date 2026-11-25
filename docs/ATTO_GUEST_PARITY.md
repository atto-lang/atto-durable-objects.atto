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
