# Orleans-inspired actor synchronization: trusted host reference

Tracking: [issue #10](https://github.com/ores-truffle-oreslang/oreslang-durable-objects.ores/issues/10) and [shared conformance #230](https://github.com/ores-truffle-oreslang/oreslang-platform-conformance/issues/230).

## What this implementation actually proves

A **trusted, single-host C++17 + SQLite WAL** reference for grain-like durable snapshots, revisioned outbox events, idempotent commands, epoch-fenced writes, and restartable materialized projections. A regression test forks a **second operating-system process** and uses its own SQLite connections and separate replica database. The writer commits two changes after the reader establishes its snapshot cursor; the reader catches up, commits its projection, and its state survives reopen.

This is **NOT** two compiled Oreslang actors or an Oreslang ABI, an Orleans replacement, a distributed lease protocol, a cross-machine transport, or a production multi-tenant security boundary. The two host processes share access to one source SQLite file; change notification is test orchestration, not an async guest subscription.

The implementation is deliberately standalone and does **not** modify the existing tenant FIFO/lease runtime or the existing SqliteDurableStore API. Do not count its tests as proof that those other components are integrated.

## Source and semantics

- `native/include/ores/durable/actor_sync.hpp`: `SqliteActorSync` host adapter.
- `native/tests/actor_sync_test.cpp`: two-process and negative-logic regression suite.
- `SyncKey=(tenant, namespace, actor_type, object_id)`: fully qualified logical actor key; caller must authenticate/authorize its tenant outside this prototype.
- `activate(key, epoch)`: trusted controller advances monotonically increasing epoch. A higher epoch **fences earlier writes**, but *any holder of this adapter can call it*. This is **not** distributed owner election, an authenticated capability, lease renewal, or safe failover.
- `snapshot(key)`: returns current epoch/revision/schema fingerprint and full opaque snapshot.
- `commit(key, epoch, expected_revision, op_id, command, schema_fp, new_snapshot, reply)`: one `BEGIN IMMEDIATE` transaction checks epoch + expected revision; atomically writes state, stores immutable operation input/schema/reply for retry, and appends a revisioned outbox entry. A duplicate operation ID with identical command and schema replays the prior response; differing input/schema fails closed. No external effect is included in the transaction.
- `changes_after(key, cursor, limit)`: returns **up to 256** ordered committed full snapshots. It is a bounded **nonblocking polling API**, not an async iterator. Missing, pruned, or future revisions throw `ResyncRequired`. No implicit cursor reset.
- `bootstrap_projection(consumer, key, snapshot)`: materializes a snapshot in the reader's separate SQLite DB only once. Reinitialization must agree on epoch, version, schema and bytes.
- `apply_projection(consumer, key, change)`: atomic snapshot + durable cursor update; rejects gaps, stale epochs, schema/data/operation mismatches for repeated events, and out-of-order revisions. Applying an identical already-committed change is idempotent.
- `compact(key, epoch, through)`: prunes old outbox entries and durably records the minimum supported cursor; old readers must resnapshot.

**Important:** The change event currently carries the complete snapshot, not a field-level patch. There is one authoritative writer. There is no CRDT merge, event-sourced reducer, actor passivation, callback replay, timer/reminder delivery or cross-actor transaction.

### Two-process data flow

```text
Writer OS process                         Reader OS process
source SQLite (WAL)                      source SQLite (read connection)
   actor revision 0 ----snapshot-------> replica SQLite revision 0
   commit op-1 -> revision 1
   commit op-2 -> revision 2
                     changes_after(0) ---> apply revision 1, then 2
                                          duplicate rev 2 = no-op
                                          projection persists after reopen
```

## Build and verify

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

Standalone reference (POSIX, SQLite 3.35+, GCC or Clang):

```sh
c++ -std=c++17 -pthread -O1 -g -Wall -Wextra -Wpedantic -Werror -UNDEBUG \
  -Inative/include native/tests/actor_sync_test.cpp -lsqlite3 -o /tmp/actor-sync-test
/tmp/actor-sync-test
```

The repository's `native-ci.yml` additionally builds the test under GCC and Clang AddressSanitizer + UndefinedBehaviorSanitizer, subject to actual runnable Actions capacity. **Executed local host tests are not compiler-backed Oreslang guest evidence**. GitHub Actions runs with zero executed job steps must be marked `NOT_EXECUTED`.

## Required follow-up for a production guest API

1. Define versioned typed `.ores` `ActorRef<T>`, `SyncSnapshot<T>`, `Change<T>`, `watch` and `OutcomeUnknown` contracts; preserve explicit `spawn`, `state` and `critical handle_state` semantics, and no automatic nested Future flattening.
2. Replace manually trusted `activate` with authenticated admission and storage-backed distributed **lease/epoch authority** for multi-host operation. Test partitions, concurrent owners, crash recovery, stale continuations and migration. Do not use POSIX `flock` for cross-host consensus.
3. Implement a bounded **asynchronous** broker from source guest actor turns to the durable transactional outbox and snapshot API. No carrier blocking and no raw borrowed actor-state references through FFI. Add capability-checked schemas, quotas, cancellation, projection backpressure and resync protocols.
4. Source compile and execute the **same native `.ores` guest** on Java/GraalVM, LLVM, Wasm, Cranelift and .NET Native AOT. Forge separately. No host-only emulation may count as guest parity.
5. Add network partition/slow consumer/fault injection, independently review native teardown and tenant isolation, enforce exact-head source + compiler artifact identity, require stepful CI and protected main.
