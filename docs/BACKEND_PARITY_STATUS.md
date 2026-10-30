# Durable Objects — backend parity and release gates (2026-10-29)

**Owner:** `oreslang-durable-objects.ores`. This is a progress and acceptance checklist, not a production readiness certificate.

## Review stack

| Backend | PR | Branch base | Implemented host slice | Local execution evidence | Missing |
| --- | --- | --- | --- | --- | --- |
| Java/GraalVM | [#2283](https://github.com/ores-truffle-oreslang/oreslang-source.java/pull/2283) | Java main | Private-issued queue tickets, FIFO, actor gate, JUnit cases | Java 21 standalone smoke passed, exact header blob | Actual tenant host integration, JUnit/JDK25, Graal Native Image, guest API |
| LLVM | [#1169](https://github.com/ores-truffle-oreslang/oreslang-source.llvm/pull/1169) | [IsolateVM #1158](https://github.com/ores-truffle-oreslang/oreslang-source.llvm/pull/1158) | Tenant-owned native FIFO, actor ticket validation and lifecycle guards | GCC/Clang standalone queue smoke passed earlier | Full isolate/compiler CTest, scheduler, guest API and persistence |
| .NET NativeAOT | [#253](https://github.com/ores-truffle-oreslang/oreslang-source.net/pull/253) | [Tenant host #243](https://github.com/ores-truffle-oreslang/oreslang-source.net/pull/243) | Per-tenant C# FIFO and actor/retirement/shutdown guards | **Not executed** (dotnet unavailable locally) | NativeAOT and managed suites, guest API |
| Forge | [#138](https://github.com/ores-truffle-oreslang/oreslang-source.forge/pull/138) | [Tenant isolate #131](https://github.com/ores-truffle-oreslang/oreslang-source.forge/pull/131) | C++20 tenant-owned FIFO, actor ticket gates and CTest | Exact queue header GCC & Clang ASan/UBSan smoke passed | Full CMake integration, source-generated guest execution |
| Wasm | [#181](https://github.com/ores-truffle-oreslang/oreslang-source.wasm/pull/181) | [Wasmtime Store #173](https://github.com/ores-truffle-oreslang/oreslang-source.wasm/pull/173) | Node queue and Rust queue owned by each Wasmtime Store; Rust unit tests | Node22 3/3 passed, exact ESM blobs | Cargo/Wasmtime stepful runs; authenticated actor/scheduler/guest bridge |
| JS | [#109](https://github.com/ores-truffle-oreslang/oreslang-source.js/pull/109) | JS main | ESM host queues, WeakMap-issued tickets, Node regressions | Node22 3/3 passed, exact ESM blobs | Worker tenant lifecycle, source compiler and guest API |
| Library | [#1](https://github.com/ores-truffle-oreslang/oreslang-durable-objects.ores/pull/1) | Library main | API specification + SQLite/WAL storage/dedup/alarm host prototype | GCC/Clang/ASan reference storage passed | Compiled .ores declarations, storage broker/lease/outbox/alarm dispatcher |

**Exact matching contract:** Every row's repository contains `contracts/v1/durable-objects/DURABLE_QUEUE_V1.md` (Git blob `a8d3b2a0d3b528ec12e965afd4ebf213fbde71bc`) and the matching JSON fixture. Matching documentation is *not* executable conformance.

## Executable parity gates

1. Run each repository's native target with the *exact reviewed head SHA*, nonzero Actions steps, and test report. Fail/no-step runner admission is **NOT_EXECUTED**, not test success or failure.
2. Execute the six `queue-v1.json` scenarios in each backend's native host; prove FIFO, actor non-reentrancy, tenant isolation, bounded admission, cancellation and shutdown with a trace/receipt.
3. Exercise independent tenant lifetime generation: destroy/recreate same tenant ID, reuse actor/lock IDs and reject every stale ticket or persisted native handle.
4. Bind to source-compiled Oreslang `.ores` declarations with sealed actor `state`, `handle_state` and `critical`; reject unsupported effects, detached work, actor reentrancy and unbounded allocation at compile time.
5. Run real persistence/backend contract on every runtime: commit, crash before/after commit, restart replay, idempotency, alarm restoration and outbox atomicity. SQLite reference is not itself the full cross-backend storage protocol.
6. Test physical retirement across cancellation, I/O and timeout. An actor ticket must not release its lock while its callback or continuation still executes.
7. Test distributed lease fencing across two hosts: only one authoritative owner at a time, stale epoch commit rejection, split-brain network partition, process kill, and WAL recovery.
8. For untrusted LLVM/Forge/.NET native tenants, require actual restricted OS worker processes, independently authenticated IPC, CPU/RSS/FD limits, guest pointer rejection and crash isolation. Wasmtime Stores and Node Workers alone are not OS security barriers.
9. Security governance: [private-source export incident #22](https://github.com/ores-truffle-oreslang/oreslang-platform-conformance/issues/22) is open. No private-source mirroring into a different organization until independently authorized (source, destination, exact snapshot) and visibility verified private.

## Observed CI runner admission at parity heads

One current-head Actions job was examined for each of the seven repositories. Each completed as failure with **0 executed job steps**, so there is no successful hosted backend release gate. Do not mark any draft ready or merge on the basis of these runs. No cross-organization private-source transfer was made for this parity work.

## Architecture direction

Keep canonical state machines/algorithms eventually in native Oreslang `.ores` libraries, compiled across all backends. The Java, C++, C#, Rust and JS host substrates here are interim minimal runtime adapters and admission/proof scaffolding. Native guest entrypoints must use versioned, typed, audited ABI capabilities; no raw pointers, no implicit `Future` flattening, no operator overloading, and no automatic actor reentrancy.
