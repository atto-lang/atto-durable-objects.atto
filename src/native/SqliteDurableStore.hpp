#pragma once

#include <sqlite3.h>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace oreslang::durable {

// Trusted host-only SQLite/WAL reference adapter. Not a guest ABI, storage
// capability authentication layer, or distributed consensus implementation.
struct ObjectKey final {
    std::string tenant;
    std::string space;
    std::string object;
};

struct Alarm final {
    std::string name;
    std::int64_t due_unix_ms;
};

struct CommitResult final {
    bool deduplicated = false;
    std::string reply;
};

class SqliteDurableStore final {
public:
    explicit SqliteDurableStore(const std::string& file) {
        if (file.empty()) throw std::invalid_argument("SQLite path must not be empty");
        if (sqlite3_open_v2(file.c_str(), &db_,
                            SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                            nullptr) != SQLITE_OK) {
            const std::string reason = db_ ? sqlite3_errmsg(db_) : "sqlite3_open_v2 failed";
            if (db_) sqlite3_close(db_);
            db_ = nullptr;
            throw std::runtime_error(reason);
        }
        try {
            sqlite3_busy_timeout(db_, 3000);
            // Enforce durable commits rather than relying on process-local heap.
            exec("PRAGMA journal_mode=WAL");
            exec("PRAGMA synchronous=FULL");
            exec("PRAGMA foreign_keys=ON");
            exec("CREATE TABLE IF NOT EXISTS objects("
                 "tenant TEXT NOT NULL, space TEXT NOT NULL, object_id TEXT NOT NULL,"
                 "epoch INTEGER NOT NULL CHECK(epoch>0),"
                 "PRIMARY KEY(tenant,space,object_id)) WITHOUT ROWID");
            exec("CREATE TABLE IF NOT EXISTS data("
                 "tenant TEXT NOT NULL, space TEXT NOT NULL, object_id TEXT NOT NULL,"
                 "key TEXT NOT NULL, value BLOB NOT NULL,"
                 "PRIMARY KEY(tenant,space,object_id,key),"
                 "FOREIGN KEY(tenant,space,object_id) REFERENCES objects"
                 "(tenant,space,object_id)) WITHOUT ROWID");
            exec("CREATE TABLE IF NOT EXISTS dedup("
                 "tenant TEXT NOT NULL, space TEXT NOT NULL, object_id TEXT NOT NULL,"
                 "op_key TEXT NOT NULL, reply BLOB NOT NULL,"
                 "PRIMARY KEY(tenant,space,object_id,op_key),"
                 "FOREIGN KEY(tenant,space,object_id) REFERENCES objects"
                 "(tenant,space,object_id)) WITHOUT ROWID");
            exec("CREATE TABLE IF NOT EXISTS alarms("
                 "tenant TEXT NOT NULL, space TEXT NOT NULL, object_id TEXT NOT NULL,"
                 "name TEXT NOT NULL, due_ms INTEGER NOT NULL CHECK(due_ms>=0),"
                 "PRIMARY KEY(tenant,space,object_id,name),"
                 "FOREIGN KEY(tenant,space,object_id) REFERENCES objects"
                 "(tenant,space,object_id)) WITHOUT ROWID");
        } catch (...) {
            sqlite3_close(db_);
            db_ = nullptr;
            throw;
        }
    }
    ~SqliteDurableStore() { if (db_) sqlite3_close(db_); }
    SqliteDurableStore(const SqliteDurableStore&) = delete;
    SqliteDurableStore& operator=(const SqliteDurableStore&) = delete;

    // The caller must own a valid external tenant/lease capability. Epoch is
    // a fencing token, NOT a lease: this host-only adapter cannot elect owners.
    void activate(const ObjectKey& key, std::int64_t new_epoch) {
        std::lock_guard<std::mutex> guard(mu_);
        check(key);
        if (new_epoch <= 0) throw std::invalid_argument("epoch must be positive");
        Tx tx(*this);
        Statement read(db_, "SELECT epoch FROM objects WHERE tenant=? AND space=? AND object_id=?");
        bind_key(read, key);
        const int rc = sqlite3_step(read.p);
        if (rc == SQLITE_ROW) {
            const auto old = sqlite3_column_int64(read.p, 0);
            if (new_epoch <= old) throw std::logic_error("epoch must increase monotonically");
            Statement update(db_, "UPDATE objects SET epoch=? WHERE tenant=? AND space=? AND object_id=?");
            bind_i64(update, 1, new_epoch);
            bind_key(update, key, 2);
            step_done(update);
        } else if (rc == SQLITE_DONE) {
            Statement insert(db_, "INSERT INTO objects(tenant,space,object_id,epoch) VALUES(?,?,?,?)");
            bind_key(insert, key);
            bind_i64(insert, 4, new_epoch);
            step_done(insert);
        } else sql_error("lookup epoch");
        tx.commit();
    }

    // Atomically mutate one entry, record a replay response, and optionally
    // schedule an alarm. Never run user callbacks inside this transaction.
    // fail_before_commit is a fault-injection hook for deterministic tests only.
    CommitResult apply(const ObjectKey& key, std::int64_t epoch,
                       std::string_view op_key, std::string_view data_key,
                       std::string_view value, std::string_view reply,
                       std::optional<Alarm> alarm = std::nullopt,
                       bool fail_before_commit = false) {
        std::lock_guard<std::mutex> guard(mu_);
        check(key); check_label(op_key); check_label(data_key);
        if (value.size() > MAX_VALUE || reply.size() > MAX_VALUE)
            throw std::length_error("value/reply too large");
        if (alarm) {
            check_label(alarm->name);
            if (alarm->due_unix_ms < 0) throw std::invalid_argument("negative alarm deadline");
        }
        Tx tx(*this);
        verify_epoch(key, epoch);
        {
            Statement read(db_, "SELECT reply FROM dedup WHERE tenant=? AND space=? AND object_id=? AND op_key=?");
            bind_key(read, key); bind_text(read, 4, op_key);
            const int rc = sqlite3_step(read.p);
            if (rc == SQLITE_ROW) {
                const auto* ptr = static_cast<const char*>(sqlite3_column_blob(read.p, 0));
                const int size = sqlite3_column_bytes(read.p, 0);
                std::string stored(ptr ? ptr : "", static_cast<std::size_t>(size));
                tx.commit();
                return {true, std::move(stored)};
            }
            if (rc != SQLITE_DONE) sql_error("lookup dedup");
        }
        {
            Statement put(db_, "INSERT INTO data(tenant,space,object_id,key,value) VALUES(?,?,?,?,?) "
                "ON CONFLICT(tenant,space,object_id,key) DO UPDATE SET value=excluded.value");
            bind_key(put, key); bind_text(put, 4, data_key); bind_blob(put, 5, value);
            step_done(put);
        }
        {
            Statement record(db_, "INSERT INTO dedup(tenant,space,object_id,op_key,reply) VALUES(?,?,?,?,?)");
            bind_key(record, key); bind_text(record, 4, op_key); bind_blob(record, 5, reply);
            step_done(record);
        }
        if (alarm) {
            Statement schedule(db_, "INSERT INTO alarms(tenant,space,object_id,name,due_ms) VALUES(?,?,?,?,?) "
                "ON CONFLICT(tenant,space,object_id,name) DO UPDATE SET due_ms=excluded.due_ms");
            bind_key(schedule, key); bind_text(schedule, 4, alarm->name);
            bind_i64(schedule, 5, alarm->due_unix_ms); step_done(schedule);
        }
        if (fail_before_commit) throw std::runtime_error("injected failure before COMMIT");
        tx.commit();
        return {false, std::string(reply)};
    }

    std::optional<std::string> get(const ObjectKey& key, std::string_view data_key) {
        std::lock_guard<std::mutex> guard(mu_);
        check(key); check_label(data_key);
        Statement st(db_, "SELECT value FROM data WHERE tenant=? AND space=? AND object_id=? AND key=?");
        bind_key(st, key); bind_text(st, 4, data_key);
        const int rc = sqlite3_step(st.p);
        if (rc == SQLITE_DONE) return std::nullopt;
        if (rc != SQLITE_ROW) sql_error("get value");
        const auto* ptr = static_cast<const char*>(sqlite3_column_blob(st.p, 0));
        const int size = sqlite3_column_bytes(st.p, 0);
        return std::string(ptr ? ptr : "", static_cast<std::size_t>(size));
    }

    std::optional<std::int64_t> alarm_time(const ObjectKey& key, std::string_view name) {
        std::lock_guard<std::mutex> guard(mu_);
        check(key); check_label(name);
        Statement st(db_, "SELECT due_ms FROM alarms WHERE tenant=? AND space=? AND object_id=? AND name=?");
        bind_key(st, key); bind_text(st, 4, name);
        const int rc = sqlite3_step(st.p);
        if (rc == SQLITE_DONE) return std::nullopt;
        if (rc != SQLITE_ROW) sql_error("get alarm");
        return sqlite3_column_int64(st.p, 0);
    }

private:
    static constexpr std::size_t MAX_NAME = 256;
    static constexpr std::size_t MAX_VALUE = 16 * 1024 * 1024;
    sqlite3* db_ = nullptr;
    std::mutex mu_;
    class Statement final {
    public:
        sqlite3_stmt* p = nullptr;
        Statement(sqlite3* db, const char* sql) {
            if (sqlite3_prepare_v2(db, sql, -1, &p, nullptr) != SQLITE_OK)
                throw std::runtime_error(sqlite3_errmsg(db));
        }
        ~Statement() { if (p) sqlite3_finalize(p); }
        Statement(const Statement&) = delete;
        Statement& operator=(const Statement&) = delete;
    };
    class Tx final {
    public:
        explicit Tx(SqliteDurableStore& store) : store_(store) { store_.exec("BEGIN IMMEDIATE"); }
        ~Tx() { if (!committed_) sqlite3_exec(store_.db_, "ROLLBACK", nullptr, nullptr, nullptr); }
        void commit() { store_.exec("COMMIT"); committed_ = true; }
    private:
        SqliteDurableStore& store_;
        bool committed_ = false;
    };
    static void check_label(std::string_view v) {
        if (v.empty() || v.size() > MAX_NAME)
            throw std::invalid_argument("empty or overlong key");
    }
    static void check(const ObjectKey& key) {
        check_label(key.tenant); check_label(key.space); check_label(key.object);
    }
    void exec(const char* sql) {
        char* error = nullptr;
        const int rc = sqlite3_exec(db_, sql, nullptr, nullptr, &error);
        if (rc != SQLITE_OK) {
            std::string msg = error ? error : sqlite3_errmsg(db_);
            sqlite3_free(error);
            throw std::runtime_error(msg);
        }
    }
    [[noreturn]] void sql_error(const char* context) const {
        throw std::runtime_error(std::string(context) + ": " + sqlite3_errmsg(db_));
    }
    static void bind_text(Statement& st, int index, std::string_view value) {
        if (sqlite3_bind_text(st.p, index, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT) != SQLITE_OK)
            throw std::runtime_error("sqlite bind_text failed");
    }
    static void bind_blob(Statement& st, int index, std::string_view value) {
        // SQLITE_TRANSIENT allows original temporary values to be destroyed.
        const char empty = 0;
        const void* ptr = value.empty() ? static_cast<const void*>(&empty) : value.data();
        if (sqlite3_bind_blob(st.p, index, ptr, static_cast<int>(value.size()), SQLITE_TRANSIENT) != SQLITE_OK)
            throw std::runtime_error("sqlite bind_blob failed");
    }
    static void bind_i64(Statement& st, int index, std::int64_t value) {
        if (sqlite3_bind_int64(st.p, index, value) != SQLITE_OK)
            throw std::runtime_error("sqlite bind_int64 failed");
    }
    static void bind_key(Statement& st, const ObjectKey& key, int first = 1) {
        bind_text(st, first, key.tenant);
        bind_text(st, first+1, key.space);
        bind_text(st, first+2, key.object);
    }
    void step_done(Statement& st) {
        if (sqlite3_step(st.p) != SQLITE_DONE) sql_error("write");
    }
    void verify_epoch(const ObjectKey& key, std::int64_t epoch) {
        Statement st(db_, "SELECT epoch FROM objects WHERE tenant=? AND space=? AND object_id=?");
        bind_key(st, key);
        const int rc = sqlite3_step(st.p);
        if (rc != SQLITE_ROW || sqlite3_column_int64(st.p, 0) != epoch)
            throw std::logic_error("stale/missing durable object epoch");
    }
};
} // namespace oreslang::durable
