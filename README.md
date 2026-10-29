# Oreslang Durable Objects — LLVM-first substrate

This repository implements the first **trusted-host native** durable queue/state
substrate for Oreslang. It is designed to bind to LLVM's `OresVM` and `IsolateVM`
(`oreslang-source.llvm` draft PRs #1158/#1159): **one VM isolate per tenant,
multiple independently scheduled FIFO queues per `lock_id` within that isolate**.

The repository was empty before this implementation. This is an intentionally
narrow, buildable starting point, not a claim that the Oreslang compiler already
supports a `durable actor` syntax or a fully sandboxed production Durable Object.

## Runtime model

- **Tenant boundary:** each tenant has one host-controlled isolate identity, a
  generation-fenced handle and an independent `0600` SQLite database. A
  cross-process `flock` owner token prohibits two active host processes from
  owning the same tenant's database at the same time.
- **Within each tenant:** any number of queues up to the configured quota,
  indexed by `lock_id`. Each queue is FIFO, with **one active handler per
  lock**; different lock IDs can process concurrently. A durable object is
  permanently bound to one lock ID unless an explicit future migration runs.
- **Durable admission:** queue enqueue, request-key deduplication and object
  lock binding commit in one SQLite transaction (`WAL` and `synchronous=FULL`).
  `Accepted` means committed admission, not handler completion.
- **State commit:** updating an object version and acknowledging its queue item
  are one transaction. A crash before ACK requeues work on tenant reopen;
  delivery is **at least once**, so external side effects require independent
  idempotency or an outbox. A completed request's idempotency key persists.
- **Safety:** leases are noncopyable, generation- and fence-checked; abandoning
  a live lease retries it automatically. No handler runs while the host storage
  mutex is held. Key, request, queue, tenant and data-size quotas are explicit.
- **Security:** this is *trusted in-process isolation*, **not** a process
  sandbox. Only trusted Oreslang actors may execute here until LLVM's supervised
  sandboxed-worker mode exists. No raw SQLite pointer, isolate handle or
  lease token is part of the proposed guest-facing API.

## Build and run

Requires a C++17 compiler, CMake >= 3.20, SQLite >= 3.35 and POSIX `flock`.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

`native/include/ores/durable/tenant_runtime.hpp` contains the tested host API,
while `llvm_oresvm_adapter.hpp` is an **optional integration adapter** requiring
an independently authorized checkout of `oreslang-source.llvm` to compile. The
adapter must not be mistaken for a current working compiler guest bridge.

See [API contract](docs/API_V0.md) and [integration/security checklist](docs/LLVM_INTEGRATION.md).