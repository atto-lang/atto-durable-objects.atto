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
// One durable alarm per logical actor. Generation survives cancel/consume.
struct SyncAlarm {
    SyncKey key;
    std::int64_t generation;
    std::int64_t due_ms;
};
struct SyncMail {
    SyncKey key;
    std::int64_t sequence;
    std::string operation_id, command, schema;
    std::int64_t failures=0, retry_at_ms=0, attempt_epoch=0;
    std::string principal={};
};
struct SyncAdmission {
    std::int64_t sequence; // 0 means already completed
    std::optional<SyncCommit> receipt;
    bool dead_letter=false;
    bool authorization_denied=false;
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
    std::size_t inbox_entries_per_actor = 128;
    std::size_t inbox_entries_per_database = 4096;
    std::size_t inbox_command_bytes_per_database = 64u << 20;
    std::size_t inbox_entries_per_tenant = 1024;
    std::size_t inbox_command_bytes_per_tenant = 16u << 20;
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
            limits_.inbox_entries_per_actor == 0 || limits_.inbox_entries_per_database == 0 ||
            limits_.inbox_entries_per_database > 65536 ||
            limits_.inbox_entries_per_tenant == 0 || limits_.inbox_entries_per_tenant > 65536 ||
            limits_.inbox_command_bytes_per_tenant == 0 || limits_.inbox_command_bytes_per_tenant > (1u << 30) ||
            limits_.inbox_command_bytes_per_database == 0 ||
            limits_.inbox_command_bytes_per_database > (1u << 30) ||
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
            // Older stores allowed arbitrary operation IDs. Fail closed
            // before introducing the reserved alarm receipt namespace.
            {
                Statement table(db_,"SELECT 1 FROM sqlite_master WHERE type='table' AND name='sync_alarms'");
                if(table.step()!=SQLITE_ROW) {
                    Statement legacy(db_,"SELECT 1 FROM sync_operations WHERE substr(op_id,1,7)='@alarm:' LIMIT 1");
                    if(legacy.step()==SQLITE_ROW) throw std::logic_error("legacy reserved alarm receipt requires migration");
                }
            }
            exec("CREATE TABLE IF NOT EXISTS sync_alarms("
                 "tenant TEXT NOT NULL, space TEXT NOT NULL, actor_type TEXT NOT NULL, id TEXT NOT NULL,"
                 "generation INTEGER NOT NULL CHECK(generation>0), due_ms INTEGER CHECK(due_ms>=0),"
                 "PRIMARY KEY(tenant,space,actor_type,id),"
                 "FOREIGN KEY(tenant,space,actor_type,id) REFERENCES sync_objects(tenant,space,actor_type,id)) WITHOUT ROWID");
            exec("CREATE INDEX IF NOT EXISTS sync_alarm_due ON sync_alarms(due_ms,tenant,space,actor_type,id) WHERE due_ms IS NOT NULL");
            exec("CREATE TABLE IF NOT EXISTS sync_inbox("
                 "sequence INTEGER PRIMARY KEY AUTOINCREMENT, tenant TEXT NOT NULL, space TEXT NOT NULL,"
                 "actor_type TEXT NOT NULL, id TEXT NOT NULL, op_id TEXT NOT NULL, command BLOB NOT NULL,"
                 "schema_fp TEXT NOT NULL, UNIQUE(tenant,space,actor_type,id,op_id),"
                 "FOREIGN KEY(tenant,space,actor_type,id) REFERENCES sync_objects(tenant,space,actor_type,id))");
            exec("CREATE INDEX IF NOT EXISTS sync_inbox_actor_fifo ON sync_inbox(tenant,space,actor_type,id,sequence)");
            exec("CREATE TABLE IF NOT EXISTS sync_mail_principal("
                 "sequence INTEGER PRIMARY KEY, principal TEXT NOT NULL,"
                 "denied INTEGER NOT NULL DEFAULT 0 CHECK(denied IN (0,1)),"
                 "FOREIGN KEY(sequence) REFERENCES sync_inbox(sequence) ON DELETE CASCADE)");
            // Additive retry metadata; older pending rows start at attempt zero.
            // Dead letters retain their original inbox row and count against
            // its quotas until an explicit future administrative retention API.
            exec("CREATE TABLE IF NOT EXISTS sync_mail_retry("
                 "sequence INTEGER PRIMARY KEY, failures INTEGER NOT NULL CHECK(failures BETWEEN 1 AND 3),"
                 "retry_at_ms INTEGER NOT NULL CHECK(retry_at_ms>=0), dead INTEGER NOT NULL CHECK(dead IN (0,1)),"
                 "CHECK((dead=1 AND failures=3) OR (dead=0 AND failures<3)),"
                 "FOREIGN KEY(sequence) REFERENCES sync_inbox(sequence) ON DELETE CASCADE)");
            exec("CREATE TABLE IF NOT EXISTS sync_mail_attempt("
                 "sequence INTEGER PRIMARY KEY, epoch INTEGER NOT NULL CHECK(epoch>0),"
                 "FOREIGN KEY(sequence) REFERENCES sync_inbox(sequence) ON DELETE CASCADE)");
            // A managed controller binds its entire canonical deployment
            // manifest before starting any workers. No implicit adoption.
            exec("CREATE TABLE IF NOT EXISTS sync_deployment("
                 "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
                 "manifest BLOB NOT NULL CHECK(length(manifest)>0 AND length(manifest)<=4194304))");
            exec("CREATE TABLE IF NOT EXISTS sync_principal_namespace("
                 "singleton INTEGER PRIMARY KEY CHECK(singleton=1))");
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

    // Trusted exclusive controller startup only. Empty manifest denotes the
    // legacy host-fixture mode, which must not reopen a managed deployment.
    // Exact bytes (including artifact hashes/profile) are immutable in v1.
    // Any migration, adoption or rotation requires a separate offline protocol.
    void bind_deployment(std::string_view manifest) {
        if(manifest.size()>(4u<<20)) throw std::length_error("deployment manifest too large");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        std::optional<std::string> prior;
        {
            Statement read(db_,"SELECT manifest FROM sync_deployment WHERE singleton=1");
            if(read.step()==SQLITE_ROW) prior=read.blob(0);
        }
        if(prior) {
            if(manifest.empty() || *prior!=manifest)
                throw std::logic_error("deployment identity mismatch; migration required");
        } else if(!manifest.empty()) {
            {
                Statement existing(db_,"SELECT 1 FROM sync_objects LIMIT 1");
                if(existing.step()==SQLITE_ROW)
                    throw std::logic_error("legacy actors require explicit deployment adoption");
            }
            {
                Statement existing(db_,"SELECT 1 FROM sync_projection LIMIT 1");
                if(existing.step()==SQLITE_ROW)
                    throw std::logic_error("legacy projections require explicit deployment adoption");
            }
            Statement insert(db_,"INSERT INTO sync_deployment VALUES(1,?)");
            insert.blob(1,manifest);insert.done();
        }
        tx.commit();
    }

    // Only the trusted principal gateway can enable this mode at startup.
    // Reject legacy receipts/inbox using the reserved prefix before claiming
    // the namespace. A raw controller cannot reopen a gateway-mode store.
    void bind_principal_namespace(bool enabled) {
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        bool prior=false;
        {
            Statement read(db_,"SELECT 1 FROM sync_principal_namespace WHERE singleton=1");
            prior=read.step()==SQLITE_ROW;
        }
        if(prior && !enabled) throw std::logic_error("principal gateway required");
        if(enabled) {
            Statement read(db_,"SELECT 1 FROM sync_inbox m LEFT JOIN sync_mail_principal p ON p.sequence=m.sequence "
                               "WHERE p.sequence IS NULL LIMIT 1");
            if(read.step()==SQLITE_ROW) throw std::logic_error("unbound legacy mail requires explicit migration");
        }
        if(enabled && !prior) {
            {
                Statement read(db_,"SELECT 1 FROM sync_operations WHERE substr(op_id,1,11)='@principal:' LIMIT 1");
                if(read.step()==SQLITE_ROW) throw std::logic_error("legacy principal receipt requires migration");
            }
            {
                Statement read(db_,"SELECT 1 FROM sync_inbox WHERE substr(op_id,1,11)='@principal:' LIMIT 1");
                if(read.step()==SQLITE_ROW) throw std::logic_error("legacy principal inbox requires migration");
            }
            exec("INSERT INTO sync_principal_namespace VALUES(1)");
        }
        tx.commit();
    }

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

    // Trusted actor-turn controller only. Replace/cancel with an exact
    // generation CAS; NULL deadline is a retained tombstone, never reuse IDs.
    // This control transaction is separate from state mutation in v0.
    std::int64_t set_alarm(const SyncKey& key, std::int64_t epoch,
                           std::int64_t expected_generation,
                           std::optional<std::int64_t> due_ms) {
        validate_key(key);
        if(epoch<=0 || expected_generation<0 || (due_ms && *due_ms<0))
            throw std::invalid_argument("invalid alarm control");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        if(snapshot_locked(key).epoch!=epoch) throw std::logic_error("stale alarm owner");
        std::int64_t old=0;
        {
            Statement read(db_,"SELECT generation FROM sync_alarms WHERE tenant=? AND space=? AND actor_type=? AND id=?");
            bind_key(read,key);
            if(read.step()==SQLITE_ROW) old=read.number(0);
        }
        if(old!=expected_generation) throw std::logic_error("alarm generation conflict");
        if(old==std::numeric_limits<std::int64_t>::max()) throw std::overflow_error("alarm generation exhausted");
        {
            Statement write(db_,"INSERT INTO sync_alarms VALUES(?,?,?,?,?,?) "
                "ON CONFLICT(tenant,space,actor_type,id) DO UPDATE SET generation=excluded.generation,due_ms=excluded.due_ms");
            bind_key(write,key);write.i64(5,old+1);
            if(due_ms) write.i64(6,*due_ms);
            else if(sqlite3_bind_null(write.p,6)!=SQLITE_OK) throw std::runtime_error("alarm null bind failed");
            write.done();
        }
        tx.commit();return old+1;
    }

    std::int64_t alarm_generation(const SyncKey& key,std::int64_t epoch) {
        validate_key(key);
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this,false);
        if(snapshot_locked(key).epoch!=epoch) throw std::logic_error("stale alarm owner");
        std::int64_t generation=0;
        {
            Statement read(db_,"SELECT generation FROM sync_alarms WHERE tenant=? AND space=? AND actor_type=? AND id=?");
            bind_key(read,key);
            if(read.step()==SQLITE_ROW) generation=read.number(0);
        }
        tx.commit();return generation;
    }

    // Trusted scheduler: bounded indexed scan; not a claim or reservation.
    std::vector<SyncAlarm> due_alarms(std::int64_t now_ms, std::size_t limit=64) {
        if(now_ms<0 || limit==0 || limit>256) throw std::invalid_argument("invalid alarm scan");
        std::lock_guard<std::mutex> lock(mu_);
        std::vector<SyncAlarm> due;
        Statement read(db_,"SELECT tenant,space,actor_type,id,generation,due_ms FROM sync_alarms "
                           "WHERE due_ms IS NOT NULL AND due_ms<=? ORDER BY due_ms,tenant,space,actor_type,id LIMIT ?");
        read.i64(1,now_ms);read.i64(2,static_cast<std::int64_t>(limit));
        while(read.step()==SQLITE_ROW) {
            SyncAlarm a{{read.str(0),read.str(1),read.str(2),read.str(3)},read.number(4),read.number(5)};
            validate_key(a.key);due.push_back(std::move(a));
        }
        return due;
    }
    bool alarm_pending(const SyncAlarm& alarm, std::int64_t epoch, std::int64_t now_ms) {
        validate_key(alarm.key);
        if(epoch<=0 || alarm.generation<=0 || alarm.due_ms<0 || now_ms<alarm.due_ms)
            return false;
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this,false);
        if(snapshot_locked(alarm.key).epoch!=epoch) throw std::logic_error("stale alarm owner");
        const bool pending=alarm_matches_locked(alarm,now_ms);
        tx.commit();return pending;
    }

    static std::string alarm_operation_id(std::int64_t generation) {
        if(generation<=0) throw std::invalid_argument("invalid alarm generation");
        return "@alarm:"+std::to_string(generation);
    }

    // Durable admission is independent of execution. An accepted sequence
    // survives crash; exact retries resolve to the pending row or receipt.
    // Caller must hold authenticated exclusive controller/turn authority.
    SyncAdmission admit_mail(const SyncKey& key,std::int64_t epoch,
                             std::string_view operation_id,std::string_view command,
                             std::string_view schema_fp,std::string_view principal={}) {
        validate_key(key);label(operation_id);label(schema_fp);label_or_empty(principal);
        bounded(command,limits_.command_bytes);
        if(epoch<=0 || operation_id.substr(0,7)=="@alarm:")
            throw std::invalid_argument("invalid mailbox admission");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        if(snapshot_locked(key).epoch!=epoch) throw std::logic_error("stale mailbox owner");
        {
            Statement mode(db_,"SELECT 1 FROM sync_principal_namespace WHERE singleton=1");
            const bool bound=mode.step()==SQLITE_ROW;
            if(bound!=!principal.empty() || (!principal.empty() && operation_id.substr(0,11)!="@principal:"))
                throw std::logic_error("mail principal namespace mismatch");
        }
        std::optional<SyncCommit> completed;
        {
            Statement read(db_,"SELECT command,schema_fp,revision,reply FROM sync_operations WHERE tenant=? AND space=? AND actor_type=? AND id=? AND op_id=?");
            bind_key(read,key);read.text(5,operation_id);
            if(read.step()==SQLITE_ROW) {
                if(read.blob(0)!=command || read.str(1)!=schema_fp)
                    throw std::logic_error("mail identity reused with different command/schema");
                completed=SyncCommit{read.number(2),read.blob(3),true};
            }
        }
        if(completed){tx.commit();return {0,completed};}
        std::int64_t existing=0;
        bool dead=false;
        {
            Statement read(db_,"SELECT m.sequence,m.command,m.schema_fp,COALESCE(r.dead,0) FROM sync_inbox m "
                "LEFT JOIN sync_mail_retry r ON r.sequence=m.sequence WHERE m.tenant=? AND m.space=? AND m.actor_type=? AND m.id=? AND m.op_id=?");
            bind_key(read,key);read.text(5,operation_id);
            if(read.step()==SQLITE_ROW) {
                if(read.blob(1)!=command || read.str(2)!=schema_fp)
                    throw std::logic_error("pending mail identity reused with different command/schema");
                existing=read.number(0);dead=read.number(3)!=0;
            }
        }
        if(existing){
            auto binding=mail_principal_locked(existing);
            if(binding.first!=principal) throw std::logic_error("pending mail principal mismatch");
            tx.commit();return {existing,std::nullopt,dead,binding.second};
        }
        // Reserve one receipt and outbox slot for each live admitted turn.
        const auto live=live_mail_count(key);
        if(live>=limits_.dedupe_entries_per_actor ||
           count_for(key,"sync_operations")>=limits_.dedupe_entries_per_actor-live ||
           live>=limits_.outbox_entries_per_actor ||
           count_for(key,"sync_outbox")>=limits_.outbox_entries_per_actor-live)
            throw std::length_error("mail completion reservation exhausted");
        if(count_for(key,"sync_inbox")>=limits_.inbox_entries_per_actor)
            throw std::length_error("mailbox actor quota exhausted");
        {
            Statement count(db_,"SELECT COUNT(*),COALESCE(SUM(LENGTH(command)),0) FROM sync_inbox");
            if(count.step()!=SQLITE_ROW) throw std::runtime_error("mailbox quota query failed");
            const auto bytes=count.number(1);
            if(static_cast<std::uint64_t>(count.number(0))>=limits_.inbox_entries_per_database ||
               bytes<0 || static_cast<std::uint64_t>(bytes)>limits_.inbox_command_bytes_per_database ||
               command.size()>limits_.inbox_command_bytes_per_database-static_cast<std::size_t>(bytes))
                throw std::length_error("mailbox database quota exhausted");
        }
        {
            // Count retained dead letters as well as live mail. Check exact
            // replay above first, so a full tenant can still recover receipts.
            Statement count(db_,"SELECT COUNT(*),COALESCE(SUM(LENGTH(command)),0) FROM sync_inbox WHERE tenant=?");
            count.text(1,key.tenant);
            if(count.step()!=SQLITE_ROW) throw std::runtime_error("tenant mailbox quota query failed");
            const auto bytes=count.number(1);
            if(static_cast<std::uint64_t>(count.number(0))>=limits_.inbox_entries_per_tenant ||
               bytes<0 || static_cast<std::uint64_t>(bytes)>limits_.inbox_command_bytes_per_tenant ||
               command.size()>limits_.inbox_command_bytes_per_tenant-static_cast<std::size_t>(bytes))
                throw std::length_error("mailbox tenant quota exhausted");
        }
        {
            Statement write(db_,"INSERT INTO sync_inbox(tenant,space,actor_type,id,op_id,command,schema_fp) VALUES(?,?,?,?,?,?,?)");
            bind_key(write,key);write.text(5,operation_id);write.blob(6,command);write.text(7,schema_fp);write.done();
        }
        const auto seq=sqlite3_last_insert_rowid(db_);
        if(seq<=0) throw std::overflow_error("mail sequence exhausted");
        if(!principal.empty()) {
            Statement bind(db_,"INSERT INTO sync_mail_principal(sequence,principal) VALUES(?,?)");
            bind.i64(1,seq);bind.text(2,principal);bind.done();
        }
        tx.commit();return {seq,std::nullopt};
    }

    // Oldest pending record per object, globally ordered by admission.
    // The scan is a hint; consume checks identity, epoch and head again.
    std::vector<SyncMail> pending_mail(std::size_t limit=64,
                                      std::int64_t now_ms=std::numeric_limits<std::int64_t>::max()) {
        if(limit==0 || limit>256 || now_ms<0) throw std::invalid_argument("invalid mailbox scan");
        std::lock_guard<std::mutex> lock(mu_);
        Statement read(db_,"SELECT m.tenant,m.space,m.actor_type,m.id,m.sequence,m.op_id,m.command,m.schema_fp,"
            "COALESCE(r.failures,0),COALESCE(r.retry_at_ms,0),COALESCE(a.epoch,0) FROM sync_inbox AS m "
            "LEFT JOIN sync_mail_retry r ON r.sequence=m.sequence "
            "LEFT JOIN sync_mail_attempt a ON a.sequence=m.sequence "
            "WHERE COALESCE(r.dead,0)=0 AND COALESCE(r.retry_at_ms,0)<=? "
            "AND NOT EXISTS(SELECT 1 FROM sync_inbox AS p LEFT JOIN sync_mail_retry pr ON pr.sequence=p.sequence "
            "WHERE p.tenant=m.tenant AND p.space=m.space AND p.actor_type=m.actor_type AND p.id=m.id "
            "AND p.sequence<m.sequence AND COALESCE(pr.dead,0)=0) ORDER BY m.sequence LIMIT ?");
        read.i64(1,now_ms);read.i64(2,static_cast<std::int64_t>(limit));
        std::vector<SyncMail> result;
        while(read.step()==SQLITE_ROW) {
            SyncMail m{{read.str(0),read.str(1),read.str(2),read.str(3)},read.number(4),read.str(5),read.blob(6),read.str(7),read.number(8),read.number(9),read.number(10)};
            validate_key(m.key);label(m.operation_id);label(m.schema);
            m.principal=mail_principal_locked(m.sequence).first;label_or_empty(m.principal);
            bounded(m.command,limits_.command_bytes);result.push_back(std::move(m));
        }
        return result;
    }

    // Round-robin candidate discovery across tenants, oldest eligible head
    // first within each tenant. Rank before LIMIT so an early hot tenant
    // cannot hide later tenants. Cursor is trusted controller scheduling
    // state, not authorization, and need not persist across restart.
    std::vector<SyncMail> fair_pending_mail(std::size_t limit,std::int64_t now_ms,
                                            std::string_view after_tenant={}) {
        if(limit==0 || limit>256 || now_ms<0) throw std::invalid_argument("invalid fair mailbox scan");
        label_or_empty(after_tenant);
        std::lock_guard<std::mutex> lock(mu_);
        Statement read(db_,"WITH heads AS (SELECT m.tenant,m.space,m.actor_type,m.id,m.sequence,m.op_id,m.command,m.schema_fp,"
            "COALESCE(r.failures,0) AS failures,COALESCE(r.retry_at_ms,0) AS retry_at,COALESCE(a.epoch,0) AS attempt_epoch,"
            "ROW_NUMBER() OVER (PARTITION BY m.tenant ORDER BY m.sequence) AS tenant_rank "
            "FROM sync_inbox m LEFT JOIN sync_mail_retry r ON r.sequence=m.sequence "
            "LEFT JOIN sync_mail_attempt a ON a.sequence=m.sequence "
            "WHERE COALESCE(r.dead,0)=0 AND COALESCE(r.retry_at_ms,0)<=? "
            "AND NOT EXISTS(SELECT 1 FROM sync_inbox p LEFT JOIN sync_mail_retry pr ON pr.sequence=p.sequence "
            "WHERE p.tenant=m.tenant AND p.space=m.space AND p.actor_type=m.actor_type AND p.id=m.id "
            "AND p.sequence<m.sequence AND COALESCE(pr.dead,0)=0)) "
            "SELECT tenant,space,actor_type,id,sequence,op_id,command,schema_fp,failures,retry_at,attempt_epoch FROM heads "
            "ORDER BY tenant_rank,CASE WHEN tenant>? THEN 0 ELSE 1 END,tenant LIMIT ?");
        read.i64(1,now_ms);read.text(2,after_tenant.empty()?std::string_view(""):after_tenant);read.i64(3,static_cast<std::int64_t>(limit));
        std::vector<SyncMail> result;
        while(read.step()==SQLITE_ROW) {
            SyncMail m{{read.str(0),read.str(1),read.str(2),read.str(3)},read.number(4),read.str(5),read.blob(6),read.str(7),read.number(8),read.number(9),read.number(10)};
            validate_key(m.key);label(m.operation_id);label(m.schema);
            m.principal=mail_principal_locked(m.sequence).first;label_or_empty(m.principal);
            bounded(m.command,limits_.command_bytes);result.push_back(std::move(m));
        }
        return result;
    }

    bool mail_pending(const SyncMail& mail,std::int64_t epoch) {
        validate_key(mail.key);
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this,false);
        if(snapshot_locked(mail.key).epoch!=epoch) throw std::logic_error("stale mailbox owner");
        const bool pending=mail_matches_locked(mail);
        tx.commit();return pending;
    }

    // Persist a dispatch marker BEFORE running a guest. The trusted controller
    // must hold the exclusive turn and establish physical retirement before
    // recovery of an older epoch. Marker and failure/commit are atomic with
    // their respective transactions; a crash before launch can spend budget.
    SyncMail begin_mail(const SyncMail& mail,std::int64_t epoch,std::int64_t now_ms) {
        validate_key(mail.key);
        if(epoch<=0 || mail.attempt_epoch!=0 || now_ms<0 || now_ms<mail.retry_at_ms)
            throw std::invalid_argument("invalid mailbox attempt");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        if(snapshot_locked(mail.key).epoch!=epoch || !mail_matches_locked(mail))
            throw std::logic_error("stale mailbox attempt");
        Statement write(db_,"INSERT INTO sync_mail_attempt VALUES(?,?)");
        write.i64(1,mail.sequence);write.i64(2,epoch);write.done();
        tx.commit();
        auto started=mail;started.attempt_epoch=epoch;return started;
    }

    // Record a classified stopped-guest failure, or resolve a surviving
    // marker after the trusted controller has retired the previous owner.
    // Unmarked legacy host calls remain supported; they cannot bypass a marker.
    // Exact head/attempt CAS prevents stale retry tokens from consuming budget.
    // Fixed policy v1: two retries, 100ms then 200ms; third failure is terminal.
    // No state, success receipt or outbox mutation in this transaction.
    bool fail_mail(const SyncMail& mail,std::int64_t epoch,std::int64_t now_ms) {
        validate_key(mail.key);
        if(epoch<=0 || now_ms<0 || mail.failures<0 || mail.failures>=3 ||
           now_ms<mail.retry_at_ms || mail.attempt_epoch<0 || mail.attempt_epoch>epoch ||
           now_ms>std::numeric_limits<std::int64_t>::max()-200)
            throw std::invalid_argument("invalid mail failure envelope");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        if(snapshot_locked(mail.key).epoch!=epoch) throw std::logic_error("stale mailbox owner");
        if(!mail_matches_locked(mail)) throw std::logic_error("stale mailbox failure token");
        const auto failures=mail.failures+1;
        const bool dead=failures==3;
        const auto retry_at=dead?0:now_ms+100*failures;
        {
            Statement write(db_,"INSERT INTO sync_mail_retry VALUES(?,?,?,?) "
                "ON CONFLICT(sequence) DO UPDATE SET failures=excluded.failures,retry_at_ms=excluded.retry_at_ms,dead=excluded.dead");
            write.i64(1,mail.sequence);write.i64(2,failures);write.i64(3,retry_at);write.i64(4,dead?1:0);write.done();
        }
        if(mail.attempt_epoch!=0) {
            Statement clear(db_,"DELETE FROM sync_mail_attempt WHERE sequence=? AND epoch=?");
            clear.i64(1,mail.sequence);clear.i64(2,mail.attempt_epoch);clear.done();
            if(sqlite3_changes(db_)!=1) throw std::logic_error("lost mailbox attempt marker");
        }
        tx.commit();return dead;
    }

    // Policy denial is terminal and distinct from retry exhaustion in
    // admission responses. The retry table's dead sentinel releases the
    // completion reservation; no state, receipt or outbox is manufactured.
    void deny_mail(const SyncMail& mail,std::int64_t epoch) {
        validate_key(mail.key);label(mail.principal);
        if(epoch<=0 || mail.attempt_epoch>epoch) throw std::invalid_argument("invalid denied mail envelope");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        if(snapshot_locked(mail.key).epoch!=epoch || !mail_matches_locked(mail))
            throw std::logic_error("stale denied mail token");
        {
            Statement write(db_,"UPDATE sync_mail_principal SET denied=1 WHERE sequence=? AND principal=? AND denied=0");
            write.i64(1,mail.sequence);write.text(2,mail.principal);write.done();
            if(sqlite3_changes(db_)!=1) throw std::logic_error("missing principal binding");
        }
        {
            Statement write(db_,"INSERT INTO sync_mail_retry VALUES(?,3,0,1) "
                "ON CONFLICT(sequence) DO UPDATE SET failures=3,retry_at_ms=0,dead=1");
            write.i64(1,mail.sequence);write.done();
        }
        if(mail.attempt_epoch!=0) {
            Statement clear(db_,"DELETE FROM sync_mail_attempt WHERE sequence=? AND epoch=?");
            clear.i64(1,mail.sequence);clear.i64(2,mail.attempt_epoch);clear.done();
            if(sqlite3_changes(db_)!=1) throw std::logic_error("lost denied mail attempt");
        }
        tx.commit();
    }

    // Atomic state + exact-operation dedup + replay reply + outbox.
    // If the ACK is lost, retry with identical op_id/command; the caller must
    // not interpret a timeout as a definitely failed operation.
    SyncCommit commit(const SyncKey& key, std::int64_t epoch,
                      std::int64_t expected_revision, std::string_view operation_id,
                      std::string_view command, std::string_view schema_fp,
                      std::string_view next_state, std::string_view reply,
                      bool inject_precommit_failure = false,
                      const SyncAlarm* alarm = nullptr, std::int64_t now_ms = 0,
                      const SyncMail* mail = nullptr) {
        validate_key(key); label(operation_id); label(schema_fp);
        if(mail && (alarm || mail->sequence<=0 ||
            mail->key.tenant!=key.tenant || mail->key.name_space!=key.name_space ||
            mail->key.actor_type!=key.actor_type || mail->key.object_id!=key.object_id ||
            mail->operation_id!=operation_id || mail->command!=command || mail->schema!=schema_fp))
            throw std::invalid_argument("invalid mailbox commit envelope");
        if(alarm) {
            if(alarm->key.tenant!=key.tenant || alarm->key.name_space!=key.name_space ||
               alarm->key.actor_type!=key.actor_type || alarm->key.object_id!=key.object_id ||
               operation_id!=alarm_operation_id(alarm->generation) ||
               command!=("alarm:"+std::to_string(alarm->due_ms)) ||
               alarm->due_ms<0 || now_ms<alarm->due_ms)
                throw std::invalid_argument("invalid alarm commit envelope");
        } else if(operation_id.substr(0,7)=="@alarm:") {
            throw std::invalid_argument("reserved alarm operation identity");
        }
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
        if(mail && ((mail->attempt_epoch!=0 && mail->attempt_epoch!=epoch) || !mail_matches_locked(*mail)))
            throw std::logic_error("mail is not the current FIFO head");
        if(!mail) {
            Statement pending(db_,"SELECT 1 FROM sync_inbox WHERE tenant=? AND space=? AND actor_type=? AND id=? AND op_id=?");
            bind_key(pending,key);pending.text(5,operation_id);
            if(pending.step()==SQLITE_ROW) throw std::logic_error("mailbox completion required");
        }
        if(alarm && !alarm_matches_locked(*alarm,now_ms))
            throw std::logic_error("alarm cancelled, replaced or not due");
        if (old.revision != expected_revision) throw std::logic_error("revision conflict");
        // Non-mail commits cannot take capacity reserved by accepted mail.
        if(!mail) {
            const auto live=live_mail_count(key);
            if(live>=limits_.dedupe_entries_per_actor ||
               count_for(key,"sync_operations")>=limits_.dedupe_entries_per_actor-live ||
               live>=limits_.outbox_entries_per_actor ||
               count_for(key,"sync_outbox")>=limits_.outbox_entries_per_actor-live)
                throw std::length_error("completion capacity reserved for mailbox");
        }
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
        if(alarm) {
            Statement consume(db_,"UPDATE sync_alarms SET due_ms=NULL WHERE tenant=? AND space=? AND actor_type=? AND id=? AND generation=? AND due_ms=?");
            bind_key(consume,key);consume.i64(5,alarm->generation);consume.i64(6,alarm->due_ms);
            consume.done();
            if(sqlite3_changes(db_)!=1) throw std::logic_error("lost alarm generation");
        }
        if(mail) {
            Statement consume(db_,"DELETE FROM sync_inbox WHERE sequence=? AND tenant=? AND space=? AND actor_type=? AND id=?");
            consume.i64(1,mail->sequence);bind_key(consume,key,2);consume.done();
            if(sqlite3_changes(db_)!=1) throw std::logic_error("lost mailbox head");
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
    std::size_t live_mail_count(const SyncKey& key) {
        Statement read(db_,"SELECT COUNT(*) FROM sync_inbox m LEFT JOIN sync_mail_retry r ON r.sequence=m.sequence "
            "WHERE m.tenant=? AND m.space=? AND m.actor_type=? AND m.id=? AND COALESCE(r.dead,0)=0");
        bind_key(read,key);
        if(read.step()!=SQLITE_ROW) throw std::runtime_error("mail reservation count failed");
        return static_cast<std::size_t>(read.number(0));
    }
    std::pair<std::string,bool> mail_principal_locked(std::int64_t seq) {
        Statement read(db_,"SELECT principal,denied FROM sync_mail_principal WHERE sequence=?");
        read.i64(1,seq);
        if(read.step()!=SQLITE_ROW) return {"",false};
        return {read.str(0),read.number(1)!=0};
    }
    bool mail_matches_locked(const SyncMail& mail) {
        Statement read(db_,"SELECT m.sequence,m.op_id,m.command,m.schema_fp,COALESCE(r.failures,0),COALESCE(r.retry_at_ms,0),COALESCE(a.epoch,0) "
            "FROM sync_inbox m LEFT JOIN sync_mail_retry r ON r.sequence=m.sequence "
            "LEFT JOIN sync_mail_attempt a ON a.sequence=m.sequence "
            "WHERE m.tenant=? AND m.space=? AND m.actor_type=? AND m.id=? AND COALESCE(r.dead,0)=0 ORDER BY m.sequence LIMIT 1");
        bind_key(read,mail.key);
        return read.step()==SQLITE_ROW && read.number(0)==mail.sequence &&
            read.str(1)==mail.operation_id && read.blob(2)==mail.command && read.str(3)==mail.schema &&
            read.number(4)==mail.failures && read.number(5)==mail.retry_at_ms && read.number(6)==mail.attempt_epoch && mail_principal_locked(mail.sequence).first==mail.principal;
    }
    bool alarm_matches_locked(const SyncAlarm& alarm,std::int64_t now_ms) {
        Statement read(db_,"SELECT 1 FROM sync_alarms WHERE tenant=? AND space=? AND actor_type=? AND id=? "
                           "AND generation=? AND due_ms=? AND due_ms<=?");
        bind_key(read,alarm.key);read.i64(5,alarm.generation);read.i64(6,alarm.due_ms);read.i64(7,now_ms);
        return read.step()==SQLITE_ROW;
    }
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
        // Reject an older database with more live promises than completion
        // capacity. Reconfiguration must not silently abandon accepted mail.
        validate("SELECT COALESCE(MAX(n),0) FROM (SELECT "
            "(SELECT COUNT(*) FROM sync_operations o WHERE o.tenant=k.tenant AND o.space=k.space AND o.actor_type=k.actor_type AND o.id=k.id)+"
            "(SELECT COUNT(*) FROM sync_inbox m LEFT JOIN sync_mail_retry r ON r.sequence=m.sequence WHERE m.tenant=k.tenant AND m.space=k.space AND m.actor_type=k.actor_type AND m.id=k.id AND COALESCE(r.dead,0)=0) AS n FROM sync_objects k)",
            limits_.dedupe_entries_per_actor);
        validate("SELECT COALESCE(MAX(n),0) FROM (SELECT "
            "(SELECT COUNT(*) FROM sync_outbox o WHERE o.tenant=k.tenant AND o.space=k.space AND o.actor_type=k.actor_type AND o.id=k.id)+"
            "(SELECT COUNT(*) FROM sync_inbox m LEFT JOIN sync_mail_retry r ON r.sequence=m.sequence WHERE m.tenant=k.tenant AND m.space=k.space AND m.actor_type=k.actor_type AND m.id=k.id AND COALESCE(r.dead,0)=0) AS n FROM sync_objects k)",
            limits_.outbox_entries_per_actor);

        validate("SELECT COUNT(*) FROM sync_inbox",limits_.inbox_entries_per_database);
        validate("SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(*) AS n FROM sync_inbox GROUP BY tenant)",limits_.inbox_entries_per_tenant);
        validate("SELECT COALESCE(MAX(n),0) FROM (SELECT SUM(LENGTH(command)) AS n FROM sync_inbox GROUP BY tenant)",limits_.inbox_command_bytes_per_tenant);
        validate("SELECT COALESCE(SUM(LENGTH(command)),0) FROM sync_inbox",limits_.inbox_command_bytes_per_database);
        validate("SELECT COALESCE(MAX(n),0) FROM (SELECT COUNT(*) AS n FROM sync_inbox GROUP BY tenant,space,actor_type,id)",limits_.inbox_entries_per_actor);
        validate("SELECT COALESCE(MAX(LENGTH(command)),0) FROM sync_inbox",limits_.command_bytes);
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
        else if (std::string_view(table) == "sync_inbox")
            sql = "SELECT COUNT(*) FROM sync_inbox WHERE tenant=? AND space=? AND actor_type=? AND id=?";
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
