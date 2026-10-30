#include "ores/durable/tenant_runtime.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <cerrno>
#include <system_error>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ores::durable {
namespace {
[[noreturn]] void error(sqlite3* db, const char* operation) {
    throw std::runtime_error(std::string(operation) + ": " + sqlite3_errmsg(db));
}
void sql(sqlite3* db, const char* statement) {
    char* msg = nullptr;
    if (sqlite3_exec(db, statement, nullptr, nullptr, &msg) == SQLITE_OK) return;
    const std::string info = msg ? msg : "SQLite error";
    sqlite3_free(msg);
    throw std::runtime_error(info);
}
struct Statement final {
    sqlite3_stmt* s = nullptr;
    sqlite3* db;
    Statement(sqlite3* db_, const char* query) : db(db_) {
        if (sqlite3_prepare_v2(db, query, -1, &s, nullptr) != SQLITE_OK)
            error(db, "prepare");
    }
    ~Statement() { sqlite3_finalize(s); }
    Statement(const Statement&) = delete;
    void text(int i, const std::string& value) {
        if (sqlite3_bind_text(s, i, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT)
            != SQLITE_OK) error(db, "bind text");
    }
    void blob(int i, const std::string& value) {
        if (sqlite3_bind_blob(s, i, value.data(), static_cast<int>(value.size()), SQLITE_TRANSIENT)
            != SQLITE_OK) error(db, "bind blob");
    }
    void integer(int i, std::int64_t value) {
        if (sqlite3_bind_int64(s, i, value) != SQLITE_OK) error(db, "bind int");
    }
    int step() {
        const int result = sqlite3_step(s);
        if (result != SQLITE_ROW && result != SQLITE_DONE) error(db, "step");
        return result;
    }
    std::int64_t number(int column) const { return sqlite3_column_int64(s, column); }
    std::string bytes(int column) const {
        const auto size = sqlite3_column_bytes(s, column);
        const void* value = sqlite3_column_blob(s, column);
        return value && size ? std::string(static_cast<const char*>(value),size) : std::string();
    }
    std::string str(int column) const {
        const auto* v = sqlite3_column_text(s,column);
        return v ? std::string(reinterpret_cast<const char*>(v),sqlite3_column_bytes(s,column)) : "";
    }
};
struct Tx final {
    sqlite3* db;
    bool committed = false;
    explicit Tx(sqlite3* d) : db(d) { sql(db,"BEGIN IMMEDIATE"); }
    ~Tx() { if (!committed) sqlite3_exec(db,"ROLLBACK",nullptr,nullptr,nullptr); }
    void commit() { sql(db,"COMMIT"); committed = true; }
};
void require_key(const std::string& k) {
    if (k.empty() || k.size()>128 ||
        std::any_of(k.begin(), k.end(), [](unsigned char x) {return x<0x20 || x==0x7f;}))
        throw std::invalid_argument("invalid or oversized durable object key");
}
std::string filename(const std::string& id) {
    constexpr char digits[] = "0123456789abcdef";
    std::string out = "tenant-";
    for (unsigned char c : id) {out.push_back(digits[c>>4]); out.push_back(digits[c&15]);}
    return out + ".sqlite";
}
std::int64_t scalar(sqlite3* db, const char* statement, const std::string* bind = nullptr) {
    Statement q(db, statement);
    if (bind) q.text(1, *bind);
    if (q.step() != SQLITE_ROW) error(db,"missing scalar");
    return q.number(0);
}
void schema(sqlite3* db) {
    sql(db,"PRAGMA journal_mode=WAL");
    sql(db,"PRAGMA synchronous=FULL");
    sql(db,"PRAGMA foreign_keys=ON");
    // Validate, requeue and promote the schema in one atomic WAL transaction.
    // A failed admission must never leave a partially recovered queue.
    Tx startup_tx(db);
    if (scalar(db,"PRAGMA user_version") > 1) throw std::runtime_error("unsupported durable schema");
    sql(db,"CREATE TABLE IF NOT EXISTS objects("
           "object_id TEXT PRIMARY KEY,lock_id TEXT NOT NULL,version INTEGER NOT NULL DEFAULT 0,state BLOB NOT NULL DEFAULT X'')");
    sql(db,"CREATE TABLE IF NOT EXISTS jobs("
           "seq INTEGER PRIMARY KEY AUTOINCREMENT,lock_id TEXT NOT NULL,object_id TEXT NOT NULL,"
           "request_id TEXT NOT NULL UNIQUE,payload BLOB NOT NULL,status INTEGER NOT NULL DEFAULT 0,"
           "attempt INTEGER NOT NULL DEFAULT 0,fence INTEGER NOT NULL DEFAULT 0)");
    sql(db,"CREATE INDEX IF NOT EXISTS jobs_lock_order ON jobs(lock_id,status,seq)");
    sql(db,"CREATE TABLE IF NOT EXISTS dedupe("
           "request_id TEXT PRIMARY KEY, seq INTEGER NOT NULL,object_id TEXT NOT NULL,"
           "lock_id TEXT NOT NULL,payload BLOB NOT NULL)");
    // Fail closed on malformed persistent metadata instead of silently
    // hiding unreachable queue entries or reviving a job under another lock.
    // Read-only validation precedes recovery so an invalid file is not
    // partially repaired and accidentally promoted to an active tenant.
    // SQLite INTEGER affinity does not guarantee integer storage: very
    // large REAL values are allowed and must never reach sqlite3_column_int64.
    if (scalar(db,"SELECT COUNT(*) FROM jobs WHERE "
                  "status NOT IN (0,1) OR fence<0 OR attempt<0 OR "
                  "typeof(status)!='integer' OR typeof(fence)!='integer' OR "
                  "typeof(attempt)!='integer'") != 0)
        throw std::runtime_error("invalid persisted durable job metadata");
    if (scalar(db,"SELECT COUNT(*) FROM objects WHERE "
                  "version<0 OR typeof(version)!='integer'") != 0)
        throw std::runtime_error("invalid persisted durable object version");
    if (scalar(db,"SELECT COUNT(*) FROM dedupe WHERE "
                  "seq<=0 OR typeof(seq)!='integer'") != 0)
        throw std::runtime_error("invalid persisted durable dedupe sequence");
    if (scalar(db,"SELECT COUNT(*) FROM jobs AS j "
                  "LEFT JOIN objects AS o ON o.object_id=j.object_id "
                  "WHERE o.object_id IS NULL OR o.lock_id<>j.lock_id") != 0)
        throw std::runtime_error("persisted durable job has invalid object lock");
    // Recovery must not overflow the persistent fencing counter. SQLite can
    // promote an overflowing INTEGER expression to REAL; such a fence would
    // make stale leases impossible to distinguish reliably.
    if (scalar(db,"SELECT COUNT(*) FROM jobs WHERE "
                  "(status=1 AND fence>=9223372036854775805) OR "
                  "(status=0 AND fence>=9223372036854775806)") != 0)
        throw std::overflow_error("durable recovery fencing exhausted");
    // Recover abandoned in-flight turns; state and ACK share a WAL commit.
    sql(db,"UPDATE jobs SET status=0,fence=fence+1 WHERE status=1");
    sql(db,"PRAGMA user_version=1");
    startup_tx.commit();
}
} // namespace

TenantRuntime::Tenant::~Tenant() noexcept {
    if (db && sqlite3_close(db) != SQLITE_OK) std::terminate();
    if (lock_fd >= 0) {
        if (flock(lock_fd, LOCK_UN) != 0) std::terminate();
        if (::close(lock_fd) != 0) std::terminate();
    }
}
TenantRuntime::TenantHandle::TenantHandle(const std::shared_ptr<Tenant>& t)
    : tenant_(t), generation_(t->generation) {}
TenantRuntime::Lease::Lease(std::shared_ptr<Core> c, std::shared_ptr<Tenant> t,
            std::int64_t seq, std::int64_t fence, std::string lock,
            std::string object, std::string request, std::string payload)
    : core_(std::move(c)), tenant_(t), generation_(t->generation),
      sequence_(seq), fence_(fence), lock_id_(std::move(lock)),
      object_id_(std::move(object)), request_id_(std::move(request)),
      payload_(std::move(payload)),active_(true) {}
TenantRuntime::Lease::Lease(Lease&& x) noexcept
    : core_(std::move(x.core_)),tenant_(std::move(x.tenant_)),
      generation_(x.generation_),sequence_(x.sequence_),fence_(x.fence_),
      lock_id_(std::move(x.lock_id_)),object_id_(std::move(x.object_id_)),
      request_id_(std::move(x.request_id_)),payload_(std::move(x.payload_)),
      active_(std::exchange(x.active_,false)) {}
TenantRuntime::Lease::~Lease() noexcept {
    if (!active_) return;
    try {
        std::lock_guard<std::mutex> lock(core_->mu);
        TenantRuntime::retry_locked(*core_, *this);
    } catch (...) { std::terminate(); }
}
TenantRuntime::TenantRuntime(std::filesystem::path root, Limits limits)
    : core_(std::make_shared<Core>(std::move(root), limits)) {
    if (limits.max_tenants==0 || limits.max_lock_queues==0 ||
        limits.max_jobs_per_tenant==0 || limits.max_jobs_per_lock==0 ||
        limits.max_request_bytes==0 || limits.max_state_bytes==0 || limits.max_dedupe_keys==0)
        throw std::invalid_argument("all durable quotas must be positive");
    // The database and WAL must never sit inside a world-readable directory.
    // Do not silently chmod an existing user-controlled path.
    const bool created = std::filesystem::create_directories(core_->root);
    if (created && ::chmod(core_->root.c_str(), 0700) != 0)
        throw std::system_error(errno, std::generic_category(), "private root chmod");
    struct stat st = {};
    if (::lstat(core_->root.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)
        || (st.st_mode & 0077) || st.st_uid != ::geteuid())
        throw std::invalid_argument("durable store root must be owner-only (0700)");
}
std::shared_ptr<TenantRuntime::Tenant> TenantRuntime::check_tenant(
        const Core& core, const TenantHandle& h) {
    auto t = h.tenant_.lock();
    if (!t || h.generation_ != t->generation || !t->accepting)
        throw std::logic_error("stale tenant isolate handle");
    const auto it = core.tenants.find(t->id);
    if (it==core.tenants.end() || it->second!=t)
        throw std::logic_error("foreign tenant isolate handle");
    return t;
}
std::shared_ptr<TenantRuntime::Tenant> TenantRuntime::check_lease(
        const Core& core, const Lease& lease) {
    if (!lease.active_ || lease.core_.get()!=&core)
        throw std::logic_error("settled or foreign durable lease");
    auto t=lease.tenant_.lock();
    if (!t || lease.generation_ != t->generation)
        throw std::logic_error("retired tenant durable lease");
    const auto it=core.tenants.find(t->id);
    if (it==core.tenants.end() || it->second!=t ||
        t->active.at(lease.lock_id_)!=lease.sequence_)
        throw std::logic_error("stale or mismatched durable lease");
    return t;
}
TenantRuntime::TenantHandle TenantRuntime::open_tenant(const std::string& id) {
    require_key(id);
    std::lock_guard<std::mutex> lock(core_->mu);
    if (core_->tenants.count(id)) throw std::logic_error("exactly one isolate per tenant");
    if (core_->tenants.size() >= core_->limits.max_tenants)
        throw std::logic_error("tenant isolate budget exhausted");
    if (core_->next_generation == std::numeric_limits<std::uint64_t>::max())
        throw std::overflow_error("tenant isolate generation exhausted");
    auto t=std::make_shared<Tenant>(id,core_->next_generation);
    const auto path=core_->root / filename(id);
    const auto lock_path=path.string()+".lock";
    t->lock_fd=::open(lock_path.c_str(), O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW,0600);
    if(t->lock_fd<0)throw std::system_error(errno,std::generic_category(),"tenant lock open");
    if(::flock(t->lock_fd, LOCK_EX|LOCK_NB)!=0)
        throw std::logic_error("tenant already has an active process owner");
    struct stat lock_stat={};
    if (::fstat(t->lock_fd,&lock_stat)!=0 || !S_ISREG(lock_stat.st_mode)
        || lock_stat.st_uid!=::geteuid() || (lock_stat.st_mode & 0077))
        throw std::logic_error("tenant lock file must be private regular file");
    // Create 0600 storage before SQLite opens it, and refuse symlink aliases.
    const int dbfd=::open(path.c_str(),O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW,0600);
    if(dbfd<0)throw std::system_error(errno,std::generic_category(),"tenant database open");
    struct stat dbstat={};
    const bool safe=::fstat(dbfd,&dbstat)==0 && S_ISREG(dbstat.st_mode)
         && dbstat.st_uid==::geteuid() && !(dbstat.st_mode & 0077);
    if (::close(dbfd)!=0)throw std::logic_error("database preflight close failed");
    if(!safe)throw std::logic_error("tenant database must be owner-only regular file");
    if (sqlite3_open_v2(path.string().c_str(),&t->db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX,nullptr)
        != SQLITE_OK) error(t->db,"open tenant store");
    sqlite3_busy_timeout(t->db,5000);
    schema(t->db);
    { Statement q(t->db,"SELECT DISTINCT lock_id FROM objects");
      while(q.step()==SQLITE_ROW) t->active.emplace(q.str(0),0); }
    if (t->active.size()>core_->limits.max_lock_queues)
        throw std::logic_error("persisted lock queues exceed tenant quota");
    core_->tenants.emplace(id,t);
    ++core_->next_generation;
    return TenantHandle(t);
}
void TenantRuntime::close_tenant(const TenantHandle& h) {
    std::lock_guard<std::mutex> lock(core_->mu);
    const auto t=check_tenant(*core_,h);
    for(const auto& [_,seq]:t->active)
        if (seq) throw std::logic_error("tenant has active lock leases");
    t->accepting=false;
    core_->tenants.erase(t->id);
}
Receipt TenantRuntime::enqueue(const TenantHandle& h, const std::string& object,
                   const std::string& lock_id, const std::string& request,
                   const std::string& payload) {
    require_key(object);require_key(lock_id);require_key(request);
    if (payload.size()>core_->limits.max_request_bytes)
        throw std::invalid_argument("request payload exceeds limit");
    std::lock_guard<std::mutex> lock(core_->mu);
    const auto t=check_tenant(*core_,h);
    Tx tx(t->db);
    {
        Statement q(t->db,"SELECT seq,object_id,lock_id,payload FROM dedupe WHERE request_id=?");
        q.text(1,request);
        if(q.step()==SQLITE_ROW) {
            if(q.str(1)!=object || q.str(2)!=lock_id || q.bytes(3)!=payload)
                throw std::logic_error("idempotency key reused with different request");
            const auto seq=q.number(0);
            tx.commit();return {EnqueueStatus::Duplicate,seq};
        }
    }
    if (static_cast<std::size_t>(scalar(t->db,"SELECT COUNT(*) FROM dedupe"))
        >= core_->limits.max_dedupe_keys)
        throw std::logic_error("dedupe record quota exhausted");
    if(static_cast<std::size_t>(scalar(t->db,"SELECT COUNT(*) FROM jobs"))
        >=core_->limits.max_jobs_per_tenant) return {EnqueueStatus::TenantFull,0};
    if(static_cast<std::size_t>(scalar(t->db,
        "SELECT COUNT(*) FROM jobs WHERE lock_id=?", &lock_id))
        >=core_->limits.max_jobs_per_lock) return {EnqueueStatus::LockFull,0};
    bool fresh_lock=!t->active.count(lock_id);
    if(fresh_lock && t->active.size()>=core_->limits.max_lock_queues)
        throw std::logic_error("tenant lock queue quota exhausted");
    {
        Statement q(t->db,"SELECT lock_id FROM objects WHERE object_id=?");
        q.text(1,object);
        if(q.step()==SQLITE_ROW && q.str(0)!=lock_id)
            throw std::logic_error("object lock_id cannot change without migration");
    }
    {
        Statement q(t->db,"INSERT OR IGNORE INTO objects(object_id,lock_id) VALUES (?,?)");
        q.text(1,object);q.text(2,lock_id);q.step();
    }
    {
        Statement q(t->db,"INSERT INTO jobs(lock_id,object_id,request_id,payload) VALUES(?,?,?,?)");
        q.text(1,lock_id);q.text(2,object);q.text(3,request);q.blob(4,payload);q.step();
    }
    const auto seq=sqlite3_last_insert_rowid(t->db);
    {
        Statement q(t->db,"INSERT INTO dedupe(request_id,seq,object_id,lock_id,payload) VALUES(?,?,?,?,?)");
        q.text(1,request);q.integer(2,seq);q.text(3,object);q.text(4,lock_id);
        q.blob(5,payload);q.step();
    }
    // Allocate the index BEFORE persisting transaction, to preserve exception safety.
    if(fresh_lock) t->active.emplace(lock_id,0);
    try { tx.commit(); }
    catch (...) { if(fresh_lock) t->active.erase(lock_id); throw; }
    return {EnqueueStatus::Accepted,seq};
}
std::optional<TenantRuntime::Lease> TenantRuntime::try_claim(
        const TenantHandle& h, const std::string& lock_id) {
    require_key(lock_id);
    std::lock_guard<std::mutex> lock(core_->mu);
    const auto t=check_tenant(*core_,h);
    const auto cursor=t->active.find(lock_id);
    if (cursor==t->active.end() || cursor->second) return std::nullopt;
    Tx tx(t->db);
    std::int64_t seq,fence,attempt;
    std::string object,request,payload;
    {
        Statement q(t->db,"SELECT seq,fence,attempt,object_id,request_id,payload FROM jobs "
                          "WHERE lock_id=? AND status=0 ORDER BY seq LIMIT 1");
        q.text(1,lock_id);
        if(q.step()!=SQLITE_ROW)return std::nullopt;
        seq=q.number(0);fence=q.number(1);attempt=q.number(2);
        object=q.str(3);request=q.str(4);payload=q.bytes(5);
    }
    // Reserve one increment for retry/recovery after admission. A lease
    // cannot be admitted at MAX, even if it might complete immediately.
    if (fence<0 || fence>=std::numeric_limits<std::int64_t>::max()-1 ||
        attempt<0 || attempt==std::numeric_limits<std::int64_t>::max())
        throw std::overflow_error("durable claim fencing or attempt exhausted");
    {
        Statement q(t->db,"UPDATE jobs SET status=1, fence=fence+1, attempt=attempt+1 "
                          "WHERE seq=? AND status=0 AND fence=?");
        q.integer(1,seq);q.integer(2,fence);q.step();
        if(sqlite3_changes(t->db)!=1)throw std::logic_error("lost queue claim");
    }
    Lease lease(core_,t,seq,fence+1,lock_id,object,request,payload);
    lease.active_=false;
    // No allocations after durable commit; map key was installed on enqueue/open.
    tx.commit();
    cursor->second=seq;
    lease.active_=true;
    return std::optional<Lease>(std::move(lease));
}
StateSnapshot TenantRuntime::read_state(const Lease& lease) const {
    std::lock_guard<std::mutex> lock(core_->mu);
    const auto t=check_lease(*core_,lease);
    Statement q(t->db,"SELECT version,state FROM objects WHERE object_id=? AND lock_id=?");
    q.text(1,lease.object_id_);q.text(2,lease.lock_id_);
    if(q.step()!=SQLITE_ROW)throw std::logic_error("durable object lost binding");
    return {q.bytes(1),q.number(0)};
}
void TenantRuntime::complete(Lease& lease, const std::string& next_state) {
    if(next_state.size()>core_->limits.max_state_bytes)
        throw std::invalid_argument("durable state exceeds limit");
    std::lock_guard<std::mutex> lock(core_->mu);
    auto t=check_lease(*core_,lease);
    Tx tx(t->db);
    {
        Statement q(t->db,"SELECT fence FROM jobs WHERE seq=? AND status=1");
        q.integer(1,lease.sequence_);
        if(q.step()!=SQLITE_ROW || q.number(0)!=lease.fence_)
            throw std::logic_error("stale durable lease fencing token");
    }
    {
        Statement q(t->db,"UPDATE objects SET state=?,version=version+1 "
                          "WHERE object_id=? AND lock_id=? AND version<9223372036854775807");
        q.blob(1,next_state);q.text(2,lease.object_id_);q.text(3,lease.lock_id_);
        q.step();
        if(sqlite3_changes(t->db)!=1)throw std::overflow_error("object version exhausted");
    }
    {
        Statement q(t->db,"DELETE FROM jobs WHERE seq=? AND status=1 AND fence=?");
        q.integer(1,lease.sequence_);q.integer(2,lease.fence_);q.step();
        if(sqlite3_changes(t->db)!=1)throw std::logic_error("durable ACK failed");
    }
    tx.commit();
    t->active.at(lease.lock_id_)=0;
    lease.active_=false;
}
void TenantRuntime::retry_locked(Core& core, Lease& lease) {
    auto t=check_lease(core,lease);
    if (lease.fence_==std::numeric_limits<std::int64_t>::max())
        throw std::overflow_error("durable retry fencing exhausted");
    Tx tx(t->db);
    {
        Statement q(t->db,"UPDATE jobs SET status=0,fence=fence+1 "
                          "WHERE seq=? AND status=1 AND fence=?");
        q.integer(1,lease.sequence_);q.integer(2,lease.fence_);q.step();
        if(sqlite3_changes(t->db)!=1)throw std::logic_error("durable retry fencing failed");
    }
    tx.commit();
    t->active.at(lease.lock_id_)=0;
    lease.active_=false;
}
void TenantRuntime::retry(Lease& lease) {
    std::lock_guard<std::mutex> lock(core_->mu);
    retry_locked(*core_,lease);
}
std::size_t TenantRuntime::pending(const TenantHandle& h, const std::string& lock_id) const {
    require_key(lock_id);
    std::lock_guard<std::mutex> lock(core_->mu);
    const auto t=check_tenant(*core_,h);
    return static_cast<std::size_t>(scalar(t->db,
        "SELECT COUNT(*) FROM jobs WHERE lock_id=?",&lock_id));
}
} // namespace ores::durable
