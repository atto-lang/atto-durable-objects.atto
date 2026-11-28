# Binding durable queue tenants to LLVM IsolateVM

LLVM source draft PR [#1158](https://github.com/ores-truffle-oreslang/oreslang-source.llvm/pull/1158)
defines `OresVM`, `IsolateVM`, tenant generation and trusted in-process
operation. Resource-lifecycle PR [#1159](https://github.com/ores-truffle-oreslang/oreslang-source.llvm/pull/1159)
adds resource retirement pins. These drafts are independent dependencies.

```text
OresVM host
  tenant A -> IsolateVM(generation A) -> A.sqlite (private WAL)
                lock_id=counter -> FIFO [1, 2], 1 active lease
                lock_id=chat    -> FIFO [3, 4], 1 active lease
                lock_id=timer   -> FIFO [...], 1 active lease
  tenant B -> IsolateVM(generation B) -> B.sqlite
                lock_id=counter -> independent FIFO and state
```

`native/include/ores/durable/llvm_oresvm_adapter.hpp` is an optional
host adapter requiring independently authorized **external** LLVM runtime
headers and libraries. It creates and starts one LLVM isolate per tenant,
then routes all of that tenant's keyed queues through the same durable store.
It must not be mistaken for executed source compiler integration; the
current tests exercise the standalone native queue/state implementation.

## Integration security and correctness gates

1. Versioned opaque guest ABI; only the real actor scheduler may supply
   the tenant, isolate generation and execution-turn authority. No guest
   creates a lease/tenant handle or sees `sqlite3*`/native pointers.
2. Bind queue leases to actual actor turns and #1159 resource-pin retirement;
   `TrustedInProcess` is logical isolation, **not** a hostile-code sandbox.
   Reject untrusted tenants until supervised separate-process isolation exists.
3. Integrate sealed `handle_state(critical-read/critical-write)` ownership,
   permission checks, `Result`, `rt take` invalidation and backpressure.
4. Add recurring alarms, outbox/idempotent external effects, dead-letter
   policy, retention, fair scheduling and bounded time/reductions per turn.
5. Test real `.ores` -> LLVM IR -> native isolate/actor execution at exact
   commit, with crash-before/after-commit, WAL and quota/recovery receipts.
6. Prove equivalent guest semantics in Java/GraalVM, LLVM, WASM, Forge, and
   .NET Native AOT with platform-specific persistence adapters.

The native POSIX process lock ensures one active storage leader per tenant
on a local filesystem, not distributed-leader consensus. Support for
network filesystems/failover is out of scope until separate fencing work.
Never copy private LLVM implementation source into this repository without
its explicit source-export authorization.

## Lifecycle hardening draft (issue #4)

The adapter now proposes the following order for **trusted in-process**
tenant retirement:

1. Under its adapter mutex, close OresVM guest/actor admission with
   `begin_shutdown` (unless already quiescing/stopped).
2. Require `complete_shutdown`: all tracked actor and guest pins must drain.
   An exception keeps the tenant registered; neither the durable store nor
   the isolate is destroyed. Retry may resume from the quiescing phase.
3. Require the stopped phase, then close the durable tenant store. If an
   in-flight durable lease prevents this step, retain registration and reject
   new queue admission; the existing lease can still complete/retry.
4. Persist the **in-memory** per-tenant lifecycle stage `StoreRetired` only
   after a successful storage close. If VM destruction later fails, retry
   that step **without touching the retired store**. After successful VM
   destruction, mark `VmDestroyed` and invalidate the adapter handle.

The lifecycle stages are process-local only; this is not durable supervisor
incident delivery. A host crash between steps must be reconciled against
the filesystem lease, WAL recovery state, and VM generation on restart.
Do not interpret `StoreRetired` as proof that external effects completed.

On activation failure, preserve copies of both generation-fenced handles
until insertion succeeds, then attempt store and VM cleanup independently.
If either cleanup fails, report **cleanup incomplete** instead of claiming
that tenant activation or rollback succeeded. A failed start must enter
this cleanup path as well. The current host has no persistent cleanup-incident
registry, so an incomplete rollback remains a production blocker.

### Opt-in exact-LLVM integration test

Provide an *already authorized local* checkout of LLVM PR #1158 (or its
compatible successor). Do not copy LLVM source into this repository or
mirror private source into another organization.

```sh
cmake -S . -B build-llvm -DCMAKE_BUILD_TYPE=Debug \
  -DORES_DURABLE_TEST_LLVM_ADAPTER=ON \
  -DORESVM_INCLUDE_DIR=/path/to/authorized/oreslang-source.llvm/src/main/cpp
cmake --build build-llvm --parallel
ctest --test-dir build-llvm -R durable_llvm_adapter_lifecycle --output-on-failure
```

The opt-in native test checks retained stores after a rejected close with
an active lease, queue-admission refusal after shutdown begins, independent
tenant progress, an injected interruption **after store close and before VM
destroy**, idempotent retry without a second SQLite close, reopening after a
storage-open failure, and stale handle rejection. It does **not** prove fail-injected OresVM start/create/destroy
exceptions, resource-pin shutdown races, actual Oreslang guest code, worker
sandboxing, or native AOT. Add those tests and record the LLVM header SHA,
adapter SHA, compiler binary, full job steps, and sanitizer outputs before
promoting issue #4.

**Verification status:** this draft is a source-level change. No compiler,
CTest, sanitizer, or exact LLVM integration run has been executed for it.
