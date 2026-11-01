#pragma once

#include <sqlite3.h>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
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

class SqliteActorSync final {
public:
    explicit SqliteActorSync(const std::string& path) {
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
            exec("PRAGMA journal_mode=WAL");
            exec("PRAGMA synchronous=FULL");
            exec("PRAGMA foreign_keys=ON");
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
                 "PRIMARY KEY(consumer,tenant,space,actor_type,id)) WITHOUT ROWID");
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
        tx.commit();
    }

    SyncSnapshot snapshot(const SyncKey& key) {
        validate_key(key);
        std::lock_guard<std::mutex> lock(mu_);
        return snapshot_locked(key);
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
        bounded(command); bounded(next_state); bounded(reply);
        if (expected_revision < 0 || epoch <= 0) throw std::invalid_argument("invalid expected revision / epoch");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        const auto old = snapshot_locked(key);
        if (old.epoch != epoch) throw std::logic_error("stale writer epoch");
        {
            Statement s(db_, "SELECT command,schema_fp,revision,reply FROM sync_operations WHERE "
                             "tenant=? AND space=? AND actor_type=? AND id=? AND op_id=?");
            bind_key(s, key); s.text(5, operation_id);
            if (s.step() == SQLITE_ROW) {
                if (s.blob(0) != command || s.str(1) != schema_fp)
                    throw std::logic_error("operation id reused with different command/schema");
                const auto result = SyncCommit{s.number(2), s.blob(3), true};
                tx.commit();
                return result;
            }
        }
        if (old.revision != expected_revision) throw std::logic_error("revision conflict");
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
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        const auto info = snapshot_locked(key);
        Statement cutoff(db_, "SELECT pruned_through FROM sync_objects WHERE "
                              "tenant=? AND space=? AND actor_type=? AND id=?");
        bind_key(cutoff, key);
        if (cutoff.step() != SQLITE_ROW) throw std::logic_error("missing sync object");
        if (after > info.revision || after < cutoff.number(0)) throw ResyncRequired();
        std::vector<SyncChange> changes;
        Statement s(db_, "SELECT epoch,revision,op_id,schema_fp,snapshot FROM sync_outbox "
                         "WHERE tenant=? AND space=? AND actor_type=? AND id=? AND revision>? "
                         "ORDER BY revision LIMIT ?");
        bind_key(s, key); s.i64(5, after); s.i64(6, static_cast<std::int64_t>(limit));
        std::int64_t previous = after;
        while (s.step() == SQLITE_ROW) {
            const auto revision = s.number(1);
            if (previous == std::numeric_limits<std::int64_t>::max() || revision != previous + 1)
                throw ResyncRequired();
            changes.push_back({s.number(0), revision, s.str(2), s.str(3), s.blob(4)});
            previous = revision;
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
        Statement read(db_, "SELECT pruned_through FROM sync_objects WHERE tenant=? AND space=? AND actor_type=? AND id=?");
        bind_key(read, key);
        if (read.step() != SQLITE_ROW || through < read.number(0))
            throw std::logic_error("compaction watermark regression");
        Statement del(db_, "DELETE FROM sync_outbox WHERE tenant=? AND space=? AND actor_type=? AND id=? AND revision<=?");
        bind_key(del, key); del.i64(5, through); del.done();
        Statement set(db_, "UPDATE sync_objects SET pruned_through=? WHERE tenant=? AND space=? AND actor_type=? AND id=?");
        set.i64(1, through); bind_key(set, key, 2); set.done();
        tx.commit();
    }

    // Projection operations are *trusted host adapters*, not guest-authorized
    // feeds. The projection DB may be physically separate from the source DB.
    void bootstrap_projection(std::string_view consumer, const SyncKey& key, const SyncSnapshot& ss) {
        label(consumer); validate_key(key); label_or_empty(ss.schema); bounded(ss.bytes);
        if (ss.revision < 0) throw std::invalid_argument("negative projection revision");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        Statement s(db_, "INSERT INTO sync_projection VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING");
        s.text(1,consumer); bind_key(s,key,2); s.i64(6,ss.epoch);
        s.i64(7,ss.revision); s.text(8, ""); s.text(9,ss.schema); s.blob(10,ss.bytes); s.done();
        if (sqlite3_changes(db_) != 1) {
            auto prior = projection_locked(consumer,key);
            if (!prior || prior->epoch != ss.epoch || prior->revision != ss.revision ||
                prior->schema != ss.schema || prior->bytes != ss.bytes)
                throw std::logic_error("projection already initialized with different state");
        }
        tx.commit();
    }
    bool apply_projection(std::string_view consumer, const SyncKey& key, const SyncChange& change) {
        label(consumer); validate_key(key); label(change.schema); label(change.operation_id); bounded(change.bytes);
        if (change.revision <= 0 || change.epoch <= 0) throw std::invalid_argument("invalid change");
        std::lock_guard<std::mutex> lock(mu_);
        Tx tx(*this);
        const auto prior = projection_locked(consumer,key);
        if (!prior) throw ResyncRequired();
        if (change.revision == prior->revision) {
            Statement op(db_, "SELECT op_id FROM sync_projection WHERE consumer=? AND tenant=? "
                              "AND space=? AND actor_type=? AND id=?");
            op.text(1,consumer); bind_key(op,key,2);
            if (op.step()!=SQLITE_ROW || prior->epoch != change.epoch ||
                prior->schema != change.schema || prior->bytes != change.bytes ||
                (!op.str(0).empty() && op.str(0) != change.operation_id))
                throw std::logic_error("conflicting duplicate event");
            tx.commit(); return false;
        }
        if (change.epoch < prior->epoch || change.revision <= prior->revision ||
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
        return *ss;
    }

private:
    static constexpr std::size_t max_label = 256;
    static constexpr std::size_t max_bytes = 1 << 20;
    sqlite3* db_ = nullptr;
    std::mutex mu_;
    static void label(std::string_view s) {
        if (s.empty() || s.size() > max_label || s.find('\0') != std::string_view::npos)
            throw std::invalid_argument("invalid sync key");
    }
    static void label_or_empty(std::string_view s) { if (!s.empty()) label(s); }
    static void bounded(std::string_view s) { if (s.size() > max_bytes) throw std::length_error("sync payload too large"); }
    static void validate_key(const SyncKey& k) { label(k.tenant); label(k.name_space); label(k.actor_type); label(k.object_id); }
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
        explicit Tx(SqliteActorSync& store):store_(store) { store_.exec("BEGIN IMMEDIATE"); }
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
    std::optional<SyncSnapshot> projection_locked(std::string_view consumer, const SyncKey& key) {
        Statement s(db_, "SELECT epoch,revision,schema_fp,snapshot FROM sync_projection WHERE "
                         "consumer=? AND tenant=? AND space=? AND actor_type=? AND id=?");
        s.text(1,consumer); bind_key(s,key,2);
        if (s.step()!=SQLITE_ROW) return std::nullopt;
        return SyncSnapshot{s.number(0),s.number(1),s.str(2),s.blob(3)};
    }
};

} // namespace ores::durable
