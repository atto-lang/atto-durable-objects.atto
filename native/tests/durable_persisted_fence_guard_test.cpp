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
        // SQLite accepts TEXT in BLOB-affinity columns. Refuse alternate
        // storage classes even when the byte string happens to compare equal:
        // length(TEXT) counts Unicode characters, not raw payload bytes.
        reject_corrupt("text-object-state", "UPDATE objects SET state='text'");
        reject_corrupt("text-job-payload", "UPDATE jobs SET payload='text'");
        reject_corrupt("text-receipt-payload",
                       "UPDATE dedupe SET payload='text'");

        reject_corrupt("real-dedupe-sequence", "UPDATE dedupe SET seq=1e100");
        // A recovered active claim needs a further increment for the
        // next claim; refuse values that would create a permanently
        // unclaimable persisted queue after startup.
        reject_corrupt("near-overflow-active",
                       "UPDATE jobs SET status=1,fence=9223372036854775805");
        reject_corrupt("near-overflow-pending",
                       "UPDATE jobs SET status=0,fence=9223372036854775806");

        // Pending jobs and idempotency receipts must refer to exactly the
        // same request, sequence, object, lock and payload. Completed jobs
        // legitimately retain dedupe records without an active queue row.
        reject_corrupt("missing-receipt", "DELETE FROM dedupe");
        reject_corrupt("incorrect-receipt-seq", "UPDATE dedupe SET seq=2");
        reject_corrupt("incorrect-receipt-payload",
                       "UPDATE dedupe SET payload=X'11'");
        reject_corrupt("incorrect-receipt-object",
                       "UPDATE dedupe SET object_id='other'");
        reject_corrupt("incorrect-receipt-lock",
                       "UPDATE dedupe SET lock_id='wrong'");
        reject_corrupt("incorrect-receipt-request",
                       "UPDATE dedupe SET request_id='other'");
        reject_corrupt("negative-job-sequence", "UPDATE jobs SET seq=-1");
        reject_corrupt("receipt-aliases-another-job",
                       "INSERT INTO dedupe(request_id,seq,object_id,lock_id,payload) "
                       "VALUES('forged',1,'obj','lock',X'00')");

        // Completed requests retain their dedupe receipt after their job
        // row is deleted. A lost table or reset AUTOINCREMENT high-water must
        // fail on reopen, otherwise an ACKed request can be run twice.
        const auto reject_completed_history = [&](const std::string& name,
                                                  const char* mutation) {
            const auto isolated = root / ("completed-" + name);
            {
                TenantRuntime seeded(isolated);
                auto tenant = seeded.open_tenant("t");
                assert(seeded.enqueue(tenant,"obj","lock","req","data").status
                       == EnqueueStatus::Accepted);
                auto lease = seeded.try_claim(tenant,"lock");
                assert(lease.has_value());
                seeded.complete(*lease,"done");
                lease.reset();
                seeded.close_tenant(tenant);
            }
            const auto db_file = isolated / "tenant-74.sqlite";
            sqlite3* db = nullptr;
            assert(sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE,
                                   nullptr) == SQLITE_OK);
            assert(sqlite3_exec(db, mutation, nullptr, nullptr, nullptr) == SQLITE_OK);
            assert(sqlite3_close(db) == SQLITE_OK);
            TenantRuntime recovered(isolated);
            bool rejected = false;
            try { (void)recovered.open_tenant("t"); }
            catch (const std::runtime_error&) { rejected = true; }
            assert(rejected);
        };
        reject_completed_history("dropped-dedupe", "DROP TABLE dedupe");
        reject_completed_history("reset-schema-version", "PRAGMA user_version=0");
        reject_completed_history("reset-sequence",
                                 "UPDATE sqlite_sequence SET seq=0 WHERE name='jobs'");
        reject_completed_history("deleted-sequence",
                                 "DELETE FROM sqlite_sequence WHERE name='jobs'");
        reject_completed_history("real-sequence",
                                 "UPDATE sqlite_sequence SET seq=1e100 WHERE name='jobs'");
        reject_completed_history("negative-sequence",
                                 "UPDATE sqlite_sequence SET seq=-1 WHERE name='jobs'");
        reject_completed_history("duplicate-sqlite-sequence-record",
                                 "INSERT INTO sqlite_sequence(name,seq) VALUES('jobs',1)");
        reject_completed_history("destroyed-schema-and-version",
            "DROP TABLE dedupe; DROP TABLE jobs; DROP TABLE objects;"
            "PRAGMA user_version=0");
        reject_completed_history("historical-receipt-alias",
            "INSERT INTO dedupe(request_id,seq,object_id,lock_id,payload) "
            "SELECT 'alias',seq,object_id,lock_id,payload FROM dedupe LIMIT 1");

        // A completed job deliberately has no live jobs row, but its
        // historical receipt and object bindings must still be valid keys.
        // SQLite's TEXT affinity permits embedded NUL/control bytes and
        // even separately stored BLOB values, none of which are guest keys.
        reject_completed_history("control-receipt-request",
            "UPDATE dedupe SET request_id=char(1)||request_id");
        reject_completed_history("nul-receipt-object",
            "UPDATE dedupe SET object_id=char(0)||object_id");
        reject_completed_history("blob-receipt-request",
            "UPDATE dedupe SET request_id=X'726571'");
        reject_completed_history("oversized-receipt-lock",
            "UPDATE dedupe SET lock_id=printf('%0129d',0)");
        reject_completed_history("control-object-lock",
            "UPDATE objects SET lock_id=char(127)||lock_id");

        // On restart, current Limits also apply to previously persisted
        // records. Raising limits during a previous session must not allow
        // a subsequent stricter tenant to admit too much state/backlog.
        const auto reject_tightened_quota = [&](const std::string& name,
                const Limits& limits, bool two_jobs, bool completed,
                const std::string& expected_error) {
            const auto isolated = root / ("quota-" + name);
            {
                TenantRuntime seeded(isolated);
                const auto tenant = seeded.open_tenant("t");
                assert(seeded.enqueue(tenant,"obj","lock","one","data").status
                       == EnqueueStatus::Accepted);
                if (two_jobs)
                    assert(seeded.enqueue(tenant,"obj","lock","two","more").status
                           == EnqueueStatus::Accepted);
                if (completed) {
                    auto lease = seeded.try_claim(tenant,"lock");
                    assert(lease.has_value());
                    seeded.complete(*lease,"done");
                    lease.reset();
                }
                seeded.close_tenant(tenant);
            }
            TenantRuntime restarted_case(isolated,limits);
            bool rejected = false;
            try { (void)restarted_case.open_tenant("t"); }
            catch (const std::logic_error& ex) {
                rejected = std::string(ex.what()).find(expected_error)
                           != std::string::npos;
            }
            assert(rejected);
        };
        {
            Limits limits;
            limits.max_jobs_per_tenant = 1;
            reject_tightened_quota("tenant-backlog",limits,true,false,
                                   "job count exceeds tenant quota");
        }
        {
            Limits limits;
            limits.max_jobs_per_lock = 1;
            reject_tightened_quota("lock-backlog",limits,true,false,
                                   "lock backlog exceeds quota");
        }
        {
            Limits limits;
            limits.max_dedupe_keys = 1;
            reject_tightened_quota("dedupe-history",limits,true,false,
                                   "dedupe keys exceed quota");
        }
        {
            Limits limits;
            limits.max_state_bytes = 1;
            reject_tightened_quota("object-state",limits,false,true,
                                   "state exceeds byte quota");
        }
        {
            Limits limits;
            limits.max_request_bytes = 1;
            reject_tightened_quota("queued-request",limits,false,false,
                                   "request exceeds byte quota");
            reject_tightened_quota("completed-request",limits,false,true,
                                   "request exceeds byte quota");
        }
        // A too-small quota must refuse an abandoned claim before modifying
        // its fence or status; a failed open cannot partially requeue history.
        {
            const auto isolated = root / "quota-atomic-recovery";
            {
                TenantRuntime seeded(isolated);
                const auto tenant = seeded.open_tenant("t");
                assert(seeded.enqueue(tenant,"obj","lock","one","data").status
                       == EnqueueStatus::Accepted);
                assert(seeded.enqueue(tenant,"obj","lock","two","data").status
                       == EnqueueStatus::Accepted);
                seeded.close_tenant(tenant);
            }
            const auto db_file = isolated / "tenant-74.sqlite";
            sqlite3* db = nullptr;
            assert(sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE,
                                   nullptr) == SQLITE_OK);
            assert(sqlite3_exec(db,
                "UPDATE jobs SET status=1,fence=7 WHERE request_id='one'",
                nullptr,nullptr,nullptr) == SQLITE_OK);
            assert(sqlite3_close(db) == SQLITE_OK);
            Limits limits;
            limits.max_jobs_per_tenant = 1;
            TenantRuntime restarted_case(isolated,limits);
            bool rejected = false;
            try { (void)restarted_case.open_tenant("t"); }
            catch (const std::logic_error& ex) {
                rejected = std::string(ex.what()).find(
                    "job count exceeds tenant quota") != std::string::npos;
            }
            assert(rejected);
            assert(sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE,
                                   nullptr) == SQLITE_OK);
            sqlite3_stmt* query = nullptr;
            assert(sqlite3_prepare_v2(db,
                "SELECT status,fence FROM jobs WHERE request_id='one'",
                -1,&query,nullptr) == SQLITE_OK);
            assert(sqlite3_step(query) == SQLITE_ROW);
            assert(sqlite3_column_int(query,0) == 1);
            assert(sqlite3_column_int64(query,1) == 7);
            assert(sqlite3_finalize(query) == SQLITE_OK);
            assert(sqlite3_close(db) == SQLITE_OK);
        }

        // A damaged second receipt must prevent recovery from advancing a
        // different in-flight row. Validate everything before any requeue.
        {
            const auto isolated = root / "atomic-recovery-refusal";
            {
                TenantRuntime seeded(isolated);
                const auto tenant = seeded.open_tenant("t");
                assert(seeded.enqueue(tenant,"obj","lock","good","first").status
                       == EnqueueStatus::Accepted);
                assert(seeded.enqueue(tenant,"obj","lock","bad","second").status
                       == EnqueueStatus::Accepted);
                seeded.close_tenant(tenant);
            }
            const auto db_file = isolated / "tenant-74.sqlite";
            sqlite3* db = nullptr;
            assert(sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE,
                                   nullptr) == SQLITE_OK);
            assert(sqlite3_exec(db,
                "UPDATE jobs SET status=1,fence=7 WHERE request_id='good';"
                "DELETE FROM dedupe WHERE request_id='bad'",
                nullptr,nullptr,nullptr) == SQLITE_OK);
            assert(sqlite3_close(db) == SQLITE_OK);
            TenantRuntime restarted_case(isolated);
            bool refused = false;
            try { (void)restarted_case.open_tenant("t"); }
            catch (const std::runtime_error&) { refused = true; }
            assert(refused);
            assert(sqlite3_open_v2(db_file.c_str(), &db, SQLITE_OPEN_READWRITE,
                                   nullptr) == SQLITE_OK);
            sqlite3_stmt* query = nullptr;
            assert(sqlite3_prepare_v2(db,
                "SELECT status,fence FROM jobs WHERE request_id='good'",
                -1,&query,nullptr) == SQLITE_OK);
            assert(sqlite3_step(query) == SQLITE_ROW);
            assert(sqlite3_column_int(query,0) == 1);
            assert(sqlite3_column_int64(query,1) == 7);
            assert(sqlite3_finalize(query) == SQLITE_OK);
            assert(sqlite3_close(db) == SQLITE_OK);
        }
    } catch (...) {
        std::filesystem::remove_all(root);
        throw;
    }
    std::filesystem::remove_all(root);
}
