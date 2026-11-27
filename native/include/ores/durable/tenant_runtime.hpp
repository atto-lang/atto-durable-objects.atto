#pragma once

#include <sqlite3.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ores::durable {

// Trusted host-only reference. This is NOT an LLVM guest ABI or OS sandbox.
// Each tenant owns exactly one registry entry and its own SQLite WAL store.
struct Limits {
    std::size_t max_tenants = 128;
    std::size_t max_lock_queues = 256;
    std::size_t max_jobs_per_tenant = 4096;
    std::size_t max_jobs_per_lock = 256;
    std::size_t max_request_bytes = 1 << 20;
    std::size_t max_state_bytes = 1 << 20;
    std::size_t max_dedupe_keys = 32768;
};

enum class EnqueueStatus { Accepted, Duplicate, LockFull, TenantFull };

// These are host-only diagnostic bit flags, not a guest API. A nonzero value
// quarantines this tenant for the lifetime of the runtime. No auto-unquarantine.
enum TenantIncident : unsigned {
    NoIncident = 0,
    RetryStorageFailure = 1u << 0,
    AckStorageFailure = 1u << 1,
    RollbackStorageFailure = 1u << 2,
    SqliteCloseFailure = 1u << 3,
    LockUnlockFailure = 1u << 4,
    FileCloseFailure = 1u << 5,
    AdmissionStorageFailure = 1u << 6,
    ClaimStorageFailure = 1u << 7,
    ReadStorageFailure = 1u << 8
};

// Test-only fault seam at the trusted host's SQLite/POSIX call boundaries.
// Synthetic failures exercise containment; they are not SQLite VFS I/O receipts.
enum class StorageFaultPoint {
    None, EnqueueBeforeCommit, ClaimBeforeCommit, RetryBeforeWrite, RetryBeforeCommit,
    AckBeforeWrite, AckBeforeCommit,
    Rollback, SqliteClose, LockUnlock, FileClose, ReadState, PendingQuery,
    IncidentUnlink, IncidentDirSync, IncidentGuardRelease
};
struct Receipt {
    EnqueueStatus status;
    std::int64_t sequence;
};
struct StateSnapshot {
    std::string bytes;
    std::int64_t version;
};

class TenantRuntime final {
    struct Tenant;
    struct Core;
public:
    class TenantHandle final {
        friend class TenantRuntime;
        std::weak_ptr<Tenant> tenant_;
        std::uint64_t generation_ = 0;
        explicit TenantHandle(const std::shared_ptr<Tenant>& t);
    public:
        TenantHandle() = default;
        unsigned incident_flags() const noexcept; // nonzero means quarantined
    };

    class Lease final {
        friend class TenantRuntime;
        std::shared_ptr<Core> core_;
        std::weak_ptr<Tenant> tenant_;
        std::uint64_t generation_ = 0;
        std::int64_t sequence_ = 0;
        std::int64_t fence_ = 0;
        std::string lock_id_, object_id_, request_id_, payload_;
        bool active_ = false;
        Lease(std::shared_ptr<Core>, std::shared_ptr<Tenant>, std::int64_t,
              std::int64_t, std::string, std::string, std::string, std::string);
    public:
        Lease() = delete;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&&) noexcept;
        Lease& operator=(Lease&&) = delete;
        ~Lease() noexcept; // automatically retries on exception/unwind
        const std::string& payload() const noexcept { return payload_; }
        const std::string& object_id() const noexcept { return object_id_; }
        const std::string& lock_id() const noexcept { return lock_id_; }
        std::int64_t sequence() const noexcept { return sequence_; }
        std::int64_t fence() const noexcept { return fence_; }
    };

    explicit TenantRuntime(std::filesystem::path root, Limits limits = {});
    TenantRuntime(const TenantRuntime&) = delete;
    TenantRuntime& operator=(const TenantRuntime&) = delete;
    TenantHandle open_tenant(const std::string& tenant_id);
    // Trusted supervisor-only recovery; never call automatically on restart.
    TenantHandle recover_tenant(const std::string& tenant_id);
    void close_tenant(const TenantHandle&); // leaves persisted queues for restart
    // Trusted test harness only: never expose to Oreslang guest code.
    void inject_fault_for_test(const TenantHandle&, StorageFaultPoint, int sqlite_code = SQLITE_IOERR);
    // Host supervisor polls this outside noexcept teardown. Never a guest callback.
    std::vector<std::pair<std::string, unsigned>> quarantined_tenants() const;

    Receipt enqueue(const TenantHandle&, const std::string& object_id,
                    const std::string& lock_id, const std::string& request_id,
                    const std::string& payload);
    std::optional<Lease> try_claim(const TenantHandle&, const std::string& lock_id);
    StateSnapshot read_state(const Lease&) const;
    void complete(Lease&, const std::string& next_state);
    void retry(Lease&);
    std::size_t pending(const TenantHandle&, const std::string& lock_id) const;
    // Host utility only; tenant identity never appears in guest callbacks.
private:
    struct Tenant final {
        std::string id;
        std::uint64_t generation;
        sqlite3* db = nullptr;
        int lock_fd = -1; // POSIX per-tenant OS-process singleton fence
        bool accepting = true;
        std::atomic<unsigned> incidents{NoIncident};
        StorageFaultPoint fault_point = StorageFaultPoint::None; // guarded by Core::mu
        int fault_code = SQLITE_IOERR;
        void quarantine(unsigned bit) noexcept { incidents.fetch_or(bit, std::memory_order_relaxed); }
        std::unordered_map<std::string, std::int64_t> active; // one per lock_id
        Tenant(std::string key, std::uint64_t gen) : id(std::move(key)), generation(gen) {}
        ~Tenant() noexcept;
    };
    struct Core final {
        std::mutex mu;
        const std::filesystem::path root;
        const Limits limits;
        std::uint64_t next_generation = 1;
        std::unordered_map<std::string, std::shared_ptr<Tenant>> tenants;
        Core(std::filesystem::path p, Limits l) : root(std::move(p)), limits(l) {}
    };
    std::shared_ptr<Core> core_;
    TenantHandle open_impl(const std::string& tenant_id, bool supervised_recovery);
    static std::shared_ptr<Tenant> check_tenant(const Core&, const TenantHandle&);
    static std::shared_ptr<Tenant> check_lease(const Core&, const Lease&);
    static void retry_locked(Core&, Lease&);
    static void inject_sqlite_fault(Tenant&, StorageFaultPoint);
};

} // namespace ores::durable
