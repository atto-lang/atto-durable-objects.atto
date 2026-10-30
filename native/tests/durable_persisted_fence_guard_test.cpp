#include "ores/durable/tenant_runtime.hpp"
#include <cassert>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <sqlite3.h>
#include <sys/stat.h>

using namespace ores::durable;
int main() {
    const auto root = std::filesystem::temp_directory_path() /
        ("ores-fence-guard-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directory(root);
    std::filesystem::permissions(root, std::filesystem::perms::owner_all,
                                 std::filesystem::perm_options::replace);
    try {
        {
            TenantRuntime initial(root);
            auto tenant = initial.open_tenant("t");
            assert(initial.enqueue(tenant,"obj","lock","request","data").status
                   == EnqueueStatus::Accepted);
            initial.close_tenant(tenant);
        }
        std::filesystem::path filename;
        for (const auto& item : std::filesystem::directory_iterator(root))
            if (item.path().extension() == ".sqlite") filename = item.path();
        assert(!filename.empty());
        sqlite3* db = nullptr;
        assert(sqlite3_open_v2(filename.c_str(), &db, SQLITE_OPEN_READWRITE, nullptr)
               == SQLITE_OK);
        assert(sqlite3_exec(db,
            "UPDATE jobs SET status=1,fence=9223372036854775807",
            nullptr,nullptr,nullptr) == SQLITE_OK);
        assert(sqlite3_close(db) == SQLITE_OK);
        TenantRuntime restarted(root);
        bool refused = false;
        try { (void)restarted.open_tenant("t"); }
        catch (const std::overflow_error&) { refused = true; }
        assert(refused);

        // Each independently corrupted tenant must be rejected on recovery,
        // rather than leaving invisible jobs or allowing lock reassignment.
        const auto reject_corrupt = [&](const std::string& name, const char* mutation) {
            const auto isolated = root / name;
            {
                TenantRuntime seeded(isolated);
                auto tenant = seeded.open_tenant("t");
                assert(seeded.enqueue(tenant,"obj","lock","req","data").status
                       == EnqueueStatus::Accepted);
                seeded.close_tenant(tenant);
            }
            const auto db_file = isolated / "tenant-74.sqlite"; // hex("t")
            sqlite3* damaged = nullptr;
            assert(sqlite3_open_v2(db_file.c_str(), &damaged, SQLITE_OPEN_READWRITE,
                                   nullptr) == SQLITE_OK);
            assert(sqlite3_exec(damaged, mutation, nullptr, nullptr, nullptr) == SQLITE_OK);
            assert(sqlite3_close(damaged) == SQLITE_OK);
            TenantRuntime restarted_case(isolated);
            bool rejected = false;
            try { (void)restarted_case.open_tenant("t"); }
            catch (const std::runtime_error&) { rejected = true; }
            assert(rejected);
        };
        reject_corrupt("invalid-status", "UPDATE jobs SET status=7");
        reject_corrupt("negative-fence", "UPDATE jobs SET fence=-1");
        reject_corrupt("negative-attempt", "UPDATE jobs SET attempt=-1");
        reject_corrupt("negative-version", "UPDATE objects SET version=-1");
        reject_corrupt("foreign-lock", "UPDATE jobs SET lock_id='wrong'");
        reject_corrupt("missing-object", "UPDATE jobs SET object_id='missing'");
        // SQLite's INTEGER affinity accepts huge REAL values without
        // raising a constraint error; all persisted counters must remain
        // actual INTEGER storage before the native runtime reads int64.
        reject_corrupt("real-status", "UPDATE jobs SET status=1e100");
        reject_corrupt("real-fence", "UPDATE jobs SET fence=1e100");
        reject_corrupt("real-attempt", "UPDATE jobs SET attempt=1e100");
        reject_corrupt("real-version", "UPDATE objects SET version=1e100");
        reject_corrupt("real-dedupe-sequence", "UPDATE dedupe SET seq=1e100");
        // A recovered active claim needs a further increment for the
        // next claim; refuse values that would create a permanently
        // unclaimable persisted queue after startup.
        reject_corrupt("near-overflow-active",
                       "UPDATE jobs SET status=1,fence=9223372036854775805");
        reject_corrupt("near-overflow-pending",
                       "UPDATE jobs SET status=0,fence=9223372036854775806");
    } catch (...) {
        std::filesystem::remove_all(root);
        throw;
    }
    std::filesystem::remove_all(root);
}
