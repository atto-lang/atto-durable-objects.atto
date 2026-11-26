#pragma once
#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace ores::durable::snapshot_admission_v1 {
// One statement sees the snapshot and retained history at one SQLite read point.
// Zero-revision actors may already have pending mail or alarms; those do not
// prove a previous checkpoint. Receipts and outbox rows do.
inline constexpr const char* query =
    "SELECT o.epoch,o.revision,o.schema_fp,o.snapshot,o.pruned_through,"
    "CASE WHEN o.revision=0 THEN ("
      "EXISTS(SELECT 1 FROM sync_operations r WHERE r.tenant=o.tenant AND r.space=o.space AND r.actor_type=o.actor_type AND r.id=o.id) OR "
      "EXISTS(SELECT 1 FROM sync_outbox b WHERE b.tenant=o.tenant AND b.space=o.space AND b.actor_type=o.actor_type AND b.id=o.id)"
    ") ELSE 0 END "
    "FROM sync_objects o WHERE o.tenant=? AND o.space=? AND o.actor_type=? AND o.id=?";
inline void validate(std::int64_t epoch,std::int64_t revision,std::string_view schema,
                     std::string_view bytes,std::int64_t pruned,bool prior_history) {
    if(epoch<=0||revision<0||pruned<0||pruned>revision||
       (revision==0&&(!schema.empty()||!bytes.empty()||prior_history))||
       (revision>0&&schema.empty()))
        throw std::logic_error("invalid persisted actor snapshot history");
}
} // namespace ores::durable::snapshot_admission_v1
