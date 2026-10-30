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
