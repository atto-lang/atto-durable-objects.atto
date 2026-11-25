#pragma once

#include <sqlite3.h>
#include <cstddef>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace ores::durable {

// Trusted, single-host SQLite/WAL *reference*, not a guest ABI, authorization
// boundary, distributed lease, network transport, or source-level Oreslang actor.
struct SyncKey {
    std::string tenant, name_space, actor_type, object_id;
};
struct SyncSnapshot {
    std::int64_t epoch = 0;
    std::int64_t revision = 0;
    std::string schema;
    std::string bytes;
};
struct SyncChange {
    std::int64_t epoch = 0;
    std::int64_t revision = 0;
    std::string operation_id;
    std::string schema;
    std::string bytes; // complete typed snapshot for prototype (not a delta)
};
struct SyncCommit {
    std::int64_t revision;
    std::string reply;
    bool duplicate;
};
class ResyncRequired final : public std::logic_error {
public:
    ResyncRequired() : std::logic_error("sync cursor requires a fresh snapshot") {}
};

struct SyncLimits {
    std::size_t state_bytes = 1 << 20;
    std::size_t command_bytes = 1 << 20;
    std::size_t reply_bytes = 1 << 20;
    std::size_t feed_bytes = 4 << 20;
    std::size_t feed_entries = 64;
    std::size_t outbox_entries_per_actor = 4096;
    std::size_t dedupe_entries_per_actor = 8192;
    std::size_t actor_keys_per_database = 4096;
    std::size_t projection_consumers_per_actor = 1024;
};

class SqliteActorSync final {
public:
    explicit SqliteActorSync(const std::string& path, SyncLimits limits = {}) : limits_(limits) {
        if (limits_.state_bytes == 0 || limits_.command_bytes == 0 ||
            limits_.reply_bytes == 0 ||
            limits_.feed_bytes < limits_.state_bytes + 2 * max_label ||
            limits_.feed_entries == 0 || limits_.feed_entries > 256 ||
            limits_.outbox_entries_per_actor == 0 || limits_.dedupe_entries_per_actor == 0 ||
            limits_.actor_keys_per_database == 0 || limits_.projection_consumers_per_actor == 0 ||
            limits_.state_bytes > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
            limits_.command_bytes > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
            limits_.reply_bytes > static_cast<std::size_t>(std::numeric_limits<int>::max()))
            throw std::invalid_argument("invalid actor sync resource limits");
        if (path.empty()) throw std::invalid_argument("empty sync store path");
        const int rc = sqlite3_open_v2(path.c_str(), &db_,
            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);
        if (rc != SQLITE_OK) {
            const std::string why = db_ ? sqlite3_errmsg(db_) : "sqlite open failed";
            if (db_) sqlite3_close(db_);
            db_ = nullptr;
            throw std::runtime_error(why);
        }
        try {
            sqlite3_busy_timeout(db_, 5000);
            ensure_wal_mode();
            exec("PRAGMA synchronous=FULL");
            exec("PRAGMA foreign_keys=ON");
            // Serialize the first-open schema bootstrap/migration across
            // connections and processes. Without one BEGIN IMMEDIATE around
            // inspection and ALTER TABLE, two openers may both observe a
            // missing column and race into a duplicate-column error.
            // Roll back every DDL change if a later quota or schema check
            // rejects this database; never expose a partially migrated handle.
            Tx migration(*this);
            exec("CREATE TABLE IF NOT EXISTS sync_objects("
                 "tenant TEXT NOT NULL, space TEXT NOT NULL, actor_type TEXT NOT NULL, id TEXT NOT NULL,"
                 "epoch INTEGER NOT NULL CHECK(epoch>0), revision INTEGER NOT NULL DEFAULT 0 CHECK(revision>=0),"
                 "pruned_through INTEGER NOT NULL DEFAULT 0 CHECK(pruned_through>=0),"
                 "schema_fp TEXT NOT NULL DEFAULT '', snapshot BLOB NOT NULL DEFAULT X'',"
                 "PRIMARY KEY(tenant,space,actor_type,id)) WITHOUT ROWID");
            exec("CREATE TABLE IF NOT EXISTS sync_operations("
                 "tenant TEXT NOT NULL, space TEXT NOT NULL, actor_type TEXT NOT NULL, id TEXT NOT NULL,"
                 "op_id TEXT NOT NULL, command BLOB NOT NULL, schema_fp TEXT NOT NULL, revision INTEGER NOT NULL, reply BLOB NOT NULL,"
                 "PRIMARY KEY(tenant,space,actor_type,id,op_id),"
                 "FOREIGN KEY(tenant,space,actor_type,id) REFERENCES sync_objects(tenant,space,actor_type,id)) WITHOUT ROWID");
            exec("CREATE TABLE IF NOT EXISTS sync_outbox("
                 "tenant TEXT NOT NULL, space TEXT NOT NULL, actor_type TEXT NOT NULL, id TEXT NOT NULL,"
                 "revision INTEGER NOT NULL, epoch INTEGER NOT NULL, op_id TEXT NOT NULL,"
                 "schema_fp TEXT NOT NULL, snapshot BLOB NOT NULL,"
                 "PRIMARY KEY(tenant,space,actor_type,id,revision),"
                 "FOREIGN KEY(tenant,space,actor_type,id) REFERENCES sync_objects(tenant,space,actor_type,id)) WITHOUT ROWID");
            exec("CREATE TABLE IF NOT EXISTS sync_projection("
                 "consumer TEXT NOT NULL, tenant TEXT NOT NULL, space TEXT NOT NULL,"
                 "actor_type TEXT NOT NULL, id TEXT NOT NULL, epoch INTEGER NOT NULL,"
                 "revision INTEGER NOT NULL, op_id TEXT NOT NULL DEFAULT '',"
                 "schema_fp TEXT NOT NULL, snapshot BLOB NOT NULL,"
                 "resync_required INTEGER NOT NULL DEFAULT 0 CHECK(resync_required IN (0,1)),"
                 "resync_epoch INTEGER NOT NULL DEFAULT 0 CHECK(resync_epoch>=0),"
                 "PRIMARY KEY(consumer,tenant,space,actor_type,id)) WITHOUT ROWID");
            // Existing reference databases predate the durable resync fence.
            // Upgrade only the exact known additive column, never interpolate
            // an untrusted schema/table name.
            bool has_resync_column=false;
            bool has_required_epoch_column=false;
            {
                Statement fields(db_, "PRAGMA table_info(sync_projection)");
                while (fields.step()==SQLITE_ROW) {
                    const auto column=fields.str(1);
                    if (column=="resync_required") has_resync_column=true;
                    if (column=="resync_epoch") has_required_epoch_column=true;
                }
            }
            if (has_resync_column && !has_required_epoch_column) {
                // Older candidate binaries persisted a boolean poison but
                // not the observed writer epoch. Never auto-clear or guess
                // that unknown generation: explicit offline recovery is
                // required for these flagged rows.
                Statement unresolved(db_, "SELECT COUNT(*) FROM sync_projection "
                                          "WHERE resync_required<>0");
                if (unresolved.step()!=SQLITE_ROW || unresolved.number(0)!=0)
                    throw std::logic_error("legacy projection fence lacks observed epoch; "
                                           "manual authenticated recovery required");
            }
            if (!has_resync_column)
                exec("ALTER TABLE sync_projection ADD COLUMN resync_required "
                     "INTEGER NOT NULL DEFAULT 0 CHECK(resync_required IN (0,1))");
            if (!has_required_epoch_column)
                exec("ALTER TABLE sync_projection ADD COLUMN resync_epoch "
                     "INTEGER NOT NULL DEFAULT 0 CHECK(resync_epoch>=0)");
            validate_existing_database_quotas();
            migration.commit();
        } catch (...) { sqlite3_close(db_); db_ = nullptr; throw; }
    }
    ~SqliteActorSync() { if (db_) sqlite3_close(db_); }
    SqliteActorSync(const SqliteActorSync&) = delete;
    SqliteActorSync& operator=(const SqliteActorSync&) = delete;

    // Called only by a trusted controller that has already established writer
    // authority. Bumping an epoch is NOT itself distributed leader election.
    void activate(const SyncKey& key, std::int64_t epoch) {
        validate_key(key);
        if (epoch <= 0) throw std::invalid_argument("invalid epoch");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        Statement s(db_, "INSERT INTO sync_objects(tenant,space,actor_type,id,epoch) "
                         "VALUES(?,?,?,?,?) ON CONFLICT(tenant,space,actor_type,id) "
                         "DO UPDATE SET epoch=excluded.epoch WHERE epoch<excluded.epoch");
        bind_key(s, key);
        s.i64(5, epoch);
        s.done();
        if (sqlite3_changes(db_) != 1) throw std::logic_error("stale activation epoch");
        {
            Statement count(db_, "SELECT COUNT(*) FROM sync_objects");
            if (count.step()!=SQLITE_ROW) throw std::runtime_error("actor count failed");
            if (static_cast<std::size_t>(count.number(0)) > limits_.actor_keys_per_database)
                throw std::length_error("actor key storage quota exhausted");
        }
        tx.commit();
    }

    // Trusted single-owner controller only. Allocate the next durable epoch
    // atomically; this fences previous writers but does not elect a leader.
    std::int64_t activate_next(const SyncKey& key) {
        validate_key(key);
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        std::int64_t epoch = 1;
        {
            Statement read(db_, "SELECT epoch FROM sync_objects WHERE "
                                "tenant=? AND space=? AND actor_type=? AND id=?");
            bind_key(read, key);
            if (read.step() == SQLITE_ROW) {
                const auto old = read.number(0);
                if (old <= 0 || old == std::numeric_limits<std::int64_t>::max())
                    throw std::overflow_error("activation epoch exhausted or corrupt");
                epoch = old + 1;
            }
        }
        {
            Statement write(db_, "INSERT INTO sync_objects(tenant,space,actor_type,id,epoch) "
                                 "VALUES(?,?,?,?,?) ON CONFLICT(tenant,space,actor_type,id) "
                                 "DO UPDATE SET epoch=excluded.epoch WHERE epoch<excluded.epoch");
            bind_key(write, key); write.i64(5, epoch); write.done();
            if (sqlite3_changes(db_) != 1) throw std::logic_error("activation lost fence");
        }
        {
            Statement count(db_, "SELECT COUNT(*) FROM sync_objects");
            if (count.step()!=SQLITE_ROW) throw std::runtime_error("actor count failed");
            if (static_cast<std::size_t>(count.number(0)) > limits_.actor_keys_per_database)
                throw std::length_error("actor key storage quota exhausted");
        }
        tx.commit();
        return epoch;
    }

    SyncSnapshot snapshot(const SyncKey& key) {
        validate_key(key);
        std::lock_guard<std::mutex> lock(mu_);
        return snapshot_locked(key);
    }

    // Read-only receipt resolution before speculative guest execution. The
    // current owner epoch is checked even for duplicates. A missing receipt
    // is not a reservation: commit must still perform its own atomic check.
    std::optional<SyncCommit> lookup_receipt(const SyncKey& key, std::int64_t epoch,
                                            std::string_view operation_id,
                                            std::string_view command,
                                            std::string_view schema_fp) {
        validate_key(key); label(operation_id); label(schema_fp);
        bounded(command, limits_.command_bytes);
        if (epoch <= 0) throw std::invalid_argument("invalid epoch");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this, false);
        if (snapshot_locked(key).epoch != epoch) throw std::logic_error("stale writer epoch");
        std::optional<SyncCommit> replay;
        {
            Statement s(db_, "SELECT command,schema_fp,revision,reply FROM sync_operations WHERE "
                             "tenant=? AND space=? AND actor_type=? AND id=? AND op_id=?");
            bind_key(s, key); s.text(5, operation_id);
            if (s.step() == SQLITE_ROW) {
                if (s.blob(0) != command || s.str(1) != schema_fp)
                    throw std::logic_error("operation id reused with different command/schema");
                replay = SyncCommit{s.number(2), s.blob(3), true};
            }
        }
        tx.commit();
        return replay;
    }

    // Atomic state + exact-operation dedup + replay reply + outbox.
    // If the ACK is lost, retry with identical op_id/command; the caller must
    // not interpret a timeout as a definitely failed operation.
    SyncCommit commit(const SyncKey& key, std::int64_t epoch,
                      std::int64_t expected_revision, std::string_view operation_id,
                      std::string_view command, std::string_view schema_fp,
                      std::string_view next_state, std::string_view reply,
                      bool inject_precommit_failure = false) {
        validate_key(key); label(operation_id); label(schema_fp);
        bounded(command, limits_.command_bytes); bounded(next_state, limits_.state_bytes);
        bounded(reply, limits_.reply_bytes);
        if (expected_revision < 0 || epoch <= 0) throw std::invalid_argument("invalid expected revision / epoch");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        const auto old = snapshot_locked(key);
        if (old.epoch != epoch) throw std::logic_error("stale writer epoch");
        std::optional<SyncCommit> replay;
        {
            Statement s(db_, "SELECT command,schema_fp,revision,reply FROM sync_operations WHERE "
                             "tenant=? AND space=? AND actor_type=? AND id=? AND op_id=?");
            bind_key(s, key); s.text(5, operation_id);
            if (s.step() == SQLITE_ROW) {
                if (s.blob(0) != command || s.str(1) != schema_fp)
                    throw std::logic_error("operation id reused with different command/schema");
                replay = SyncCommit{s.number(2), s.blob(3), true};
            }
        }
        if (replay) { tx.commit(); return *replay; }
        if (old.revision != expected_revision) throw std::logic_error("revision conflict");
        if (count_for(key, "sync_operations") >= limits_.dedupe_entries_per_actor)
            throw std::length_error("durable operation retention quota exhausted");
        if (count_for(key, "sync_outbox") >= limits_.outbox_entries_per_actor)
            throw std::length_error("durable outbox retention quota exhausted");
        if (old.revision == std::numeric_limits<std::int64_t>::max())
            throw std::overflow_error("revision exhausted");
        const auto revision = old.revision + 1;
        {
            Statement s(db_, "UPDATE sync_objects SET revision=?,schema_fp=?,snapshot=? WHERE "
                             "tenant=? AND space=? AND actor_type=? AND id=? AND epoch=? AND revision=?");
            s.i64(1, revision); s.text(2, schema_fp); s.blob(3, next_state);
            bind_key(s, key, 4); s.i64(8, epoch); s.i64(9, expected_revision); s.done();
            if (sqlite3_changes(db_) != 1) throw std::logic_error("lost writer fencing/revision");
        }
        {
            Statement s(db_, "INSERT INTO sync_operations VALUES(?,?,?,?,?,?,?,?,?)");
            bind_key(s, key); s.text(5, operation_id); s.blob(6, command);
            s.text(7, schema_fp); s.i64(8, revision); s.blob(9, reply); s.done();
        }
        {
            Statement s(db_, "INSERT INTO sync_outbox VALUES(?,?,?,?,?,?,?,?,?)");
            bind_key(s, key); s.i64(5, revision); s.i64(6, epoch);
            s.text(7, operation_id); s.text(8, schema_fp); s.blob(9, next_state); s.done();
        }
        if (inject_precommit_failure) throw std::runtime_error("injected failure before commit");
        tx.commit();
        return {revision, std::string(reply), false};
    }

    // Each invocation observes a coherent revision and detects truncated/gap
    // history. No blocking wait is offered: a guest broker must provide an
    // asynchronous, bounded notification and cancellation mechanism.
    std::vector<SyncChange> changes_after(const SyncKey& key, std::int64_t after,
                                           std::size_t limit = 64) {
        validate_key(key);
        if (after < 0 || limit == 0 || limit > 256) throw std::invalid_argument("invalid feed cursor/limit");
        limit = std::min(limit, limits_.feed_entries);
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this, false); // WAL read snapshot: do not acquire SQLite writer reservation
        const auto info = snapshot_locked(key);
        std::int64_t pruned_through = -1;
        {
            Statement cutoff(db_, "SELECT pruned_through FROM sync_objects WHERE "
                                  "tenant=? AND space=? AND actor_type=? AND id=?");
            bind_key(cutoff, key);
            if (cutoff.step() != SQLITE_ROW) throw std::logic_error("missing sync object");
            pruned_through = cutoff.number(0);
        }
        if (after > info.revision || after < pruned_through) throw ResyncRequired();
        std::vector<SyncChange> changes;
        {
            // Finalize the potentially unfinished SELECT (including a byte-budget
            // page break) before committing the WAL read transaction. Otherwise
            // SQLite may retain a live statement/cursor across COMMIT.
            Statement s(db_, "SELECT epoch,revision,op_id,schema_fp,snapshot FROM sync_outbox "
                             "WHERE tenant=? AND space=? AND actor_type=? AND id=? AND revision>? "
                             "ORDER BY revision LIMIT ?");
            bind_key(s, key); s.i64(5, after); s.i64(6, static_cast<std::int64_t>(limit));
            std::int64_t previous = after;
            std::size_t used = 0;
            while (s.step() == SQLITE_ROW) {
                const auto revision = s.number(1);
                if (previous == std::numeric_limits<std::int64_t>::max() || revision != previous + 1)
                    throw ResyncRequired();
                SyncChange change{s.number(0), revision, s.str(2), s.str(3), s.blob(4)};
                const auto amount = change.bytes.size() + change.schema.size() + change.operation_id.size();
                if (amount > limits_.feed_bytes) throw std::length_error("one sync event exceeds total feed budget");
                if (used > limits_.feed_bytes - amount) break;
                used += amount;
                changes.push_back(std::move(change));
                previous = revision;
            }
        }
        if (changes.empty() && after != info.revision) throw ResyncRequired();
        tx.commit();
        return changes;
    }

    void compact(const SyncKey& key, std::int64_t epoch, std::int64_t through) {
        validate_key(key);
        if (through < 0) throw std::invalid_argument("negative compact revision");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        const auto s = snapshot_locked(key);
        if (s.epoch != epoch || through > s.revision) throw std::logic_error("stale epoch / invalid compaction");
        {
            Statement read(db_, "SELECT pruned_through FROM sync_objects WHERE tenant=? AND space=? AND actor_type=? AND id=?");
            bind_key(read, key);
            if (read.step() != SQLITE_ROW || through < read.number(0))
                throw std::logic_error("compaction watermark regression");
        }
        Statement del(db_, "DELETE FROM sync_outbox WHERE tenant=? AND space=? AND actor_type=? AND id=? AND revision<=?");
        bind_key(del, key); del.i64(5, through); del.done();
        Statement set(db_, "UPDATE sync_objects SET pruned_through=? WHERE tenant=? AND space=? AND actor_type=? AND id=?");
        set.i64(1, through); bind_key(set, key, 2); set.done();
        tx.commit();
    }

    // Projection operations are *trusted host adapters*, not guest-authorized
    // feeds. The projection DB may be physically separate from the source DB.
    void bootstrap_projection(std::string_view consumer, const SyncKey& key, const SyncSnapshot& ss) {
        label(consumer); validate_key(key); label_or_empty(ss.schema); bounded(ss.bytes, limits_.state_bytes);
        if (ss.epoch <= 0 || ss.revision < 0)
            throw std::invalid_argument("invalid projection snapshot epoch/revision");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        Statement s(db_, "INSERT INTO sync_projection(consumer,tenant,space,actor_type,id,"
                         "epoch,revision,op_id,schema_fp,snapshot) "
                         "VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING");
        s.text(1,consumer); bind_key(s,key,2); s.i64(6,ss.epoch);
        s.i64(7,ss.revision); s.text(8, ""); s.text(9,ss.schema); s.blob(10,ss.bytes); s.done();
        const int inserted=sqlite3_changes(db_);
        if (inserted == 1 &&
            count_for(key, "sync_projection") > limits_.projection_consumers_per_actor)
            throw std::length_error("projection consumer quota exhausted");
        if (inserted != 1) {
            auto prior = projection_locked(consumer,key);
            if (!prior || prior->epoch != ss.epoch || prior->revision != ss.revision ||
                prior->schema != ss.schema || prior->bytes != ss.bytes)
                throw std::logic_error("projection already initialized with different state");
        }
        tx.commit();
    }
    // Trusted resync after the source explicitly reports a compacted/gapped
    // cursor. The trusted broker must supply an authenticated, consistent
    // snapshot from the source; a network client cannot invoke this directly.
    // Never downgrade a stored cursor or silently rewrite an equal revision.
    bool resync_projection(std::string_view consumer, const SyncKey& key,
                           const SyncSnapshot& ss) {
        label(consumer); validate_key(key); label_or_empty(ss.schema);
        bounded(ss.bytes, limits_.state_bytes);
        if (ss.epoch <= 0 || ss.revision < 0)
            throw std::invalid_argument("invalid resync epoch/revision");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        const auto prior = projection_locked(consumer,key);
        if (!prior) throw std::logic_error("projection must be bootstrapped before resync");
        if (ss.epoch < prior->epoch || ss.revision < prior->revision ||
            ss.epoch < projection_required_epoch_locked(consumer,key))
            throw std::logic_error("projection resync cannot move backward or predate observed epoch");
        if (ss.revision == prior->revision) {
            if (ss.bytes != prior->bytes || ss.schema != prior->schema)
                throw std::logic_error("same-revision resync conflicts with projection");
            if (ss.epoch == prior->epoch && !projection_needs_resync_locked(consumer,key)) {
                tx.commit(); return false;
            }
        }
        Statement update(db_, "UPDATE sync_projection SET epoch=?,revision=?,op_id='',schema_fp=?,"
                              "snapshot=?,resync_required=0,resync_epoch=0 "
                              "WHERE consumer=? AND tenant=? AND space=? AND actor_type=? AND id=? AND revision=?");
        update.i64(1,ss.epoch); update.i64(2,ss.revision);
        update.text(3,ss.schema); update.blob(4,ss.bytes);
        update.text(5,consumer); bind_key(update,key,6); update.i64(10,prior->revision);
        update.done();
        if (sqlite3_changes(db_) != 1) throw std::logic_error("projection resync CAS lost");
        tx.commit(); return true;
    }

    bool apply_projection(std::string_view consumer, const SyncKey& key, const SyncChange& change) {
        label(consumer); validate_key(key); label(change.schema); label(change.operation_id);
        bounded(change.bytes, limits_.state_bytes);
        if (change.revision <= 0 || change.epoch <= 0) throw std::invalid_argument("invalid change");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        const auto prior = projection_locked(consumer,key);
        if (!prior) throw ResyncRequired();
        if (change.epoch > prior->epoch) {
            // Persist the observed new incarnation before asking for a
            // trusted snapshot. After this point, even an old-epoch delta
            // with the expected revision must not advance the projection.
            {
                Statement mark(db_, "UPDATE sync_projection SET resync_required=1,"
                                    "resync_epoch=MAX(resync_epoch,?) WHERE "
                                    "consumer=? AND tenant=? AND space=? AND actor_type=? AND id=?");
                mark.i64(1,change.epoch);
                mark.text(2,consumer); bind_key(mark,key,3); mark.done();
            }
            tx.commit();
            throw ResyncRequired();
        }
        if (projection_needs_resync_locked(consumer,key))
            throw ResyncRequired();
        if (change.revision == prior->revision) {
            std::string previous_op;
            {
                Statement op(db_, "SELECT op_id FROM sync_projection WHERE consumer=? AND tenant=? "
                                  "AND space=? AND actor_type=? AND id=?");
                op.text(1,consumer); bind_key(op,key,2);
                if (op.step()!=SQLITE_ROW) throw std::logic_error("projection lost identity");
                previous_op = op.str(0);
            }
            // A snapshot checkpoint has no verifiable event ID. It may never
            // impersonate an authenticated previously-applied event.
            if (prior->epoch != change.epoch || prior->schema != change.schema ||
                prior->bytes != change.bytes || previous_op.empty() ||
                previous_op != change.operation_id)
                throw std::logic_error("conflicting or unproven duplicate event");
            tx.commit(); return false;
        }
        // An event from a different owner epoch is never sufficient evidence
        // for activation takeover, even when its revision is contiguous.
        // An authenticated *full* source snapshot must establish the new
        // generation via resync_projection before any delta can be applied.
        if (change.epoch != prior->epoch || change.revision <= prior->revision ||
            prior->revision == std::numeric_limits<std::int64_t>::max() ||
            change.revision != prior->revision + 1) throw ResyncRequired();
        Statement s(db_, "UPDATE sync_projection SET epoch=?,revision=?,op_id=?,schema_fp=?,snapshot=? WHERE "
                         "consumer=? AND tenant=? AND space=? AND actor_type=? AND id=? AND revision=?");
        s.i64(1,change.epoch); s.i64(2,change.revision); s.text(3,change.operation_id);
        s.text(4,change.schema); s.blob(5,change.bytes);
        s.text(6,consumer); bind_key(s,key,7); s.i64(11,prior->revision); s.done();
        if (sqlite3_changes(db_) != 1) throw std::logic_error("projection CAS failed");
        tx.commit(); return true;
    }
    SyncSnapshot projection(std::string_view consumer, const SyncKey& key) {
        label(consumer); validate_key(key);
        std::lock_guard<std::mutex> lock(mu_);
        const auto ss = projection_locked(consumer,key);
        if (!ss) throw std::logic_error("projection missing");
        // Do not expose a checkpoint as an authoritative, usable replica
        // after we have observed a newer writer incarnation. The trusted
        // broker must first authenticate and install a full resync snapshot.
        if (projection_needs_resync_locked(consumer,key)) throw ResyncRequired();
        return *ss;
    }

private:
    static constexpr std::size_t max_label = 256;
    const SyncLimits limits_;
    sqlite3* db_ = nullptr;
    std::mutex mu_;
    static void label(std::string_view s) {
        if (s.empty() || s.size() > max_label || s.find('\0') != std::string_view::npos)
            throw std::invalid_argument("invalid sync key");
    }
    static void label_or_empty(std::string_view s) { if (!s.empty()) label(s); }
    static void bounded(std::string_view s, std::size_t limit) {
        if (s.size() > limit) throw std::length_error("sync payload too large");
    }
    // A process may reopen a database written with different quotas or a
    // previous binary. Fail before exposing handles when persisted rows would
    // violate the newly configured admission limits. These aggregate checks
    // use fixed SQL, not caller-controlled table names.
    void validate_existing_database_quotas() {
        auto validate = [&](const char* sql, std::size_t maximum) {
            Statement s(db_,sql);
            if (s.step()!=SQLITE_ROW)
                throw std::runtime_error("persisted quota inspection failed");
            const auto value=s.number(0);
            if (value<0 || static_cast<std::uint64_t>(value)>maximum)
                throw std::length_error("persisted actor sync database exceeds configured limits");
        };
        validate("SELECT COUNT(*) FROM sync_objects",limits_.actor_keys_per_database);
        validate("SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(*) AS n FROM sync_outbox "
                 "GROUP BY tenant,space,actor_type,id)",limits_.outbox_entries_per_actor);
        validate("SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(*) AS n FROM sync_operations "
                 "GROUP BY tenant,space,actor_type,id)",limits_.dedupe_entries_per_actor);
        validate("SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(*) AS n FROM sync_projection "
                 "GROUP BY tenant,space,actor_type,id)",limits_.projection_consumers_per_actor);
        validate("SELECT COALESCE(MAX(LENGTH(snapshot)),0) FROM sync_objects",limits_.state_bytes);
        validate("SELECT COALESCE(MAX(LENGTH(snapshot)),0) FROM sync_outbox",limits_.state_bytes);
        validate("SELECT COALESCE(MAX(LENGTH(snapshot)),0) FROM sync_projection",limits_.state_bytes);
        validate("SELECT COALESCE(MAX(LENGTH(command)),0) FROM sync_operations",limits_.command_bytes);
        validate("SELECT COALESCE(MAX(LENGTH(reply)),0) FROM sync_operations",limits_.reply_bytes);
    }
    std::size_t count_for(const SyncKey& key, const char* table) {
        // Fixed compile-time table allowlist only: no caller-controlled SQL identifiers.
        const char* sql = nullptr;
        if (std::string_view(table) == "sync_operations")
            sql = "SELECT COUNT(*) FROM sync_operations WHERE tenant=? AND space=? AND actor_type=? AND id=?";
        else if (std::string_view(table) == "sync_outbox")
            sql = "SELECT COUNT(*) FROM sync_outbox WHERE tenant=? AND space=? AND actor_type=? AND id=?";
        else if (std::string_view(table) == "sync_projection")
            sql = "SELECT COUNT(*) FROM sync_projection WHERE tenant=? AND space=? AND actor_type=? AND id=?";
        else throw std::invalid_argument("invalid quota table");
        Statement s(db_, sql);
        bind_key(s, key);
        if (s.step() != SQLITE_ROW) throw std::runtime_error("quota query failed");
        return static_cast<std::size_t>(s.number(0));
    }
    static void validate_key(const SyncKey& k) { label(k.tenant); label(k.name_space); label(k.actor_type); label(k.object_id); }
    void ensure_wal_mode() {
        // Journal mode changes must occur outside a transaction. Two host
        // processes can race while upgrading an old DELETE-mode database to
        // WAL, before our BEGIN IMMEDIATE migration gate is acquired.
        // Retry ONLY SQLite contention errors within a fixed time budget;
        // do not silently continue in DELETE mode or block a guest carrier.
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        for (;;) {
            bool confirmed_wal = false;
            char* error = nullptr;
            const int result = sqlite3_exec(
                db_, "PRAGMA journal_mode=WAL",
                [](void* output, int count, char** values, char**) -> int {
                    if (count > 0 && values && values[0]
                        && std::string_view(values[0]) == "wal")
                        *static_cast<bool*>(output) = true;
                    return 0;
                }, &confirmed_wal, &error);
            const std::string why = error ? error : sqlite3_errmsg(db_);
            sqlite3_free(error);
            if (result == SQLITE_OK) {
                if (!confirmed_wal)
                    throw std::runtime_error("actor sync requires SQLite WAL journal mode");
                return;
            }
            if ((result == SQLITE_BUSY || result == SQLITE_LOCKED)
                && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            throw std::runtime_error("actor sync WAL admission failed: " + why);
        }
    }
    void exec(const char* sql) {
        char* err = nullptr;
        if (sqlite3_exec(db_,sql,nullptr,nullptr,&err) != SQLITE_OK) {
            const std::string why = err ? err : sqlite3_errmsg(db_);
            sqlite3_free(err); throw std::runtime_error(why);
        }
    }
    class Statement final {
    public:
        sqlite3_stmt* p = nullptr;
        explicit Statement(sqlite3* db, const char* sql) {
            if (sqlite3_prepare_v2(db,sql,-1,&p,nullptr) != SQLITE_OK)
                throw std::runtime_error(sqlite3_errmsg(db));
        }
        ~Statement() { if (p) sqlite3_finalize(p); }
        void text(int i, std::string_view v) {
            if (sqlite3_bind_text(p,i,v.data(),static_cast<int>(v.size()),SQLITE_TRANSIENT)!=SQLITE_OK)
                throw std::runtime_error("sync text bind failed");
        }
        void blob(int i, std::string_view v) {
            const char empty=0;
            if (sqlite3_bind_blob(p,i,v.empty()?static_cast<const void*>(&empty):v.data(),
                    static_cast<int>(v.size()),SQLITE_TRANSIENT)!=SQLITE_OK)
                throw std::runtime_error("sync blob bind failed");
        }
        void i64(int i, std::int64_t v) {
            if (sqlite3_bind_int64(p,i,v)!=SQLITE_OK) throw std::runtime_error("sync int bind failed");
        }
        int step() {
            const int rc=sqlite3_step(p);
            if (rc!=SQLITE_ROW && rc!=SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(sqlite3_db_handle(p)));
            return rc;
        }
        void done() { if (step()!=SQLITE_DONE) throw std::logic_error("unexpected SQL result"); }
        std::int64_t number(int i) const { return sqlite3_column_int64(p,i); }
        std::string str(int i) const { const auto* s=sqlite3_column_text(p,i); return s?reinterpret_cast<const char*>(s):""; }
        std::string blob(int i) const {
            const auto* b=static_cast<const char*>(sqlite3_column_blob(p,i));
            const int n=sqlite3_column_bytes(p,i);
            return std::string(b?b:"",static_cast<std::size_t>(n));
        }
    };
    class Tx final {
        SqliteActorSync& store_;
        bool committed_=false;
    public:
        explicit Tx(SqliteActorSync& store, bool write=true):store_(store) {
            store_.exec(write ? "BEGIN IMMEDIATE" : "BEGIN DEFERRED");
        }
        ~Tx() { if (!committed_) sqlite3_exec(store_.db_,"ROLLBACK",nullptr,nullptr,nullptr); }
        void commit() { store_.exec("COMMIT"); committed_=true; }
    };
    static void bind_key(Statement& s, const SyncKey& key, int i=1) {
        s.text(i,key.tenant); s.text(i+1,key.name_space); s.text(i+2,key.actor_type); s.text(i+3,key.object_id);
    }
    SyncSnapshot snapshot_locked(const SyncKey& key) {
        Statement s(db_, "SELECT epoch,revision,schema_fp,snapshot FROM sync_objects WHERE "
                         "tenant=? AND space=? AND actor_type=? AND id=?");
        bind_key(s,key);
        if (s.step()!=SQLITE_ROW) throw std::logic_error("sync actor missing");
        return {s.number(0),s.number(1),s.str(2),s.blob(3)};
    }
    std::int64_t projection_required_epoch_locked(std::string_view consumer,
                                                   const SyncKey& key) {
        Statement s(db_, "SELECT resync_epoch FROM sync_projection WHERE "
                         "consumer=? AND tenant=? AND space=? AND actor_type=? AND id=?");
        s.text(1,consumer); bind_key(s,key,2);
        if (s.step()!=SQLITE_ROW) throw std::logic_error("projection missing");
        const auto epoch=s.number(0);
        if (epoch<0) throw std::logic_error("invalid persisted resync epoch");
        return epoch;
    }
    bool projection_needs_resync_locked(std::string_view consumer, const SyncKey& key) {
        Statement s(db_, "SELECT resync_required FROM sync_projection WHERE "
                         "consumer=? AND tenant=? AND space=? AND actor_type=? AND id=?");
        s.text(1,consumer); bind_key(s,key,2);
        if (s.step()!=SQLITE_ROW) throw std::logic_error("projection missing");
        return s.number(0)!=0;
    }
    std::optional<SyncSnapshot> projection_locked(std::string_view consumer, const SyncKey& key) {
        Statement s(db_, "SELECT epoch,revision,schema_fp,snapshot FROM sync_projection WHERE "
                         "consumer=? AND tenant=? AND space=? AND actor_type=? AND id=?");
        s.text(1,consumer); bind_key(s,key,2);
        if (s.step()!=SQLITE_ROW) return std::nullopt;
        return SyncSnapshot{s.number(0),s.number(1),s.str(2),s.blob(3)};
    }
};

} // namespace ores::durable
