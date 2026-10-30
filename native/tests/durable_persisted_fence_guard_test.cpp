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
    } catch (...) {
        std::filesystem::remove_all(root);
        throw;
    }
    std::filesystem::remove_all(root);
}
