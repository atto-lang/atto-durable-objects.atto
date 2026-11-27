# Typed workflow effects — host reference M0

Stacked on Orleans-style SQLite WAL actor synchronization (PR #11).
`workflow_effect.hpp` produces bounded, length-framed, serializable
`activity`, `timer`, `child` and `cancel` effect command identities,
with no executable closure or pointer transfer. The C++ test persists
commands and replies through an atomic SQLite commit, destroys the store,
reopens it, verifies immutable duplicate replay, records a timer and
cancellation terminal snapshot, rotates the writer epoch, and rejects a
stale continuation. It then reopens once more and verifies retained state.

This is **trusted C++ host reference**, not a compiled `.atto` workflow,
not an async scheduler, not actual activity execution, and not a verified
terminal-state/cancellation authority across processes. A holder of a
new epoch must still enforce the stored cancellation tombstone before
admitting any fresh command. The prototype does not solve distributed
leases, genuine crash injection or exactly-once remote effects.

Acceptance for backlog #53 remains blocked on source-level workflow AST,
runtime activities, canonical `.atto` compilation, real interruption/retry
and exact-head executed multi-backend positive/negative tests.
