#include "ores/durable/tenant_runtime.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <utility>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdexcept>
#include <string>
#include <thread>

using namespace ores::durable;

template <class F> bool rejects(F&& work) {
    try { work(); } catch (const std::exception&) { return true; }
    return false;
}
static std::filesystem::path root_for(const char* label) {
    const auto root = std::filesystem::temp_directory_path() /
        (std::string("ores-durable-fault-") + label + "-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directory(root);
    std::filesystem::permissions(root, std::filesystem::perms::owner_all,
                                 std::filesystem::perm_options::replace);
    return root;
}
static void retry_fault(const char* label, StorageFaultPoint point, int sqlite_code,
                        unsigned expected) {
    const auto root = root_for(label);
    std::int64_t first_fence = 0;
    {
        TenantRuntime runtime(root);
        const auto a = runtime.open_tenant("tenant-A");
        const auto b = runtime.open_tenant("tenant-B");
        assert(runtime.enqueue(a,"object-A","lock-A","request-A","a").status == EnqueueStatus::Accepted);
        assert(runtime.enqueue(b,"object-B","lock-B","request-B","b").status == EnqueueStatus::Accepted);
        auto claim = runtime.try_claim(a,"lock-A");
        assert(claim);
        first_fence = claim->fence();
        runtime.inject_fault_for_test(a,point,sqlite_code);
        std::atomic<bool> good{false};
        // B progresses in a different thread while A is being disposed.
        std::thread healthy([&] {
            try {
                auto lease = runtime.try_claim(b,"lock-B");
                if (lease && lease->payload()=="b") {
                    runtime.complete(*lease,"healthy");
                    good.store(true);
                }
            } catch (...) { /* assert in parent thread */ }
        });
        claim.reset(); // must NEVER terminate the host on retry failure
        healthy.join();
        assert(good.load());
        assert((a.incident_flags() & expected) == expected);
        const auto incidents=runtime.quarantined_tenants();
        assert(incidents.size()==1 && incidents[0].first=="tenant-A");
        assert((incidents[0].second & expected)==expected);
        assert(rejects([&] { (void)runtime.try_claim(a,"lock-A"); }));
        assert(rejects([&] { (void)runtime.enqueue(a,"object-A","lock-A","new","x"); }));
        assert(rejects([&] { runtime.close_tenant(a); }));
        assert(runtime.pending(b,"lock-B")==0);
        runtime.close_tenant(b);
    }
    // No assumed success after a simulated SQLite failure. The old turn
    // is recovered as pending with a STRICTLY HIGHER persistent fence.
    {
        TenantRuntime restarted(root);
        assert(rejects([&]{ (void)restarted.open_tenant("tenant-A"); }));
        const auto a = restarted.recover_tenant("tenant-A");
        auto recovered = restarted.try_claim(a,"lock-A");
        assert(recovered && recovered->fence()>first_fence);
        assert(restarted.read_state(*recovered).version==0);
        assert(restarted.read_state(*recovered).bytes.empty());
        restarted.complete(*recovered,"recovered");
        recovered.reset();
        restarted.close_tenant(a);
        auto b = restarted.open_tenant("tenant-B");
        assert(restarted.enqueue(b,"object-B","lock-B","request-B","b").status == EnqueueStatus::Duplicate);
        restarted.close_tenant(b);
    }
    std::filesystem::remove_all(root);
}
static void ack_fault(const char* label, StorageFaultPoint point, int sqlite_code) {
    const auto root=root_for(label);
    std::int64_t fence=0;
    {
        TenantRuntime runtime(root);
        auto a=runtime.open_tenant("A");
        auto b=runtime.open_tenant("B");
        assert(runtime.enqueue(a,"obj","lock","req","payload").status==EnqueueStatus::Accepted);
        assert(runtime.enqueue(b,"obj","lock","req","payload").status==EnqueueStatus::Accepted);
        auto claim=runtime.try_claim(a,"lock");
        auto healthy=runtime.try_claim(b,"lock");
        assert(claim && healthy);
        fence=claim->fence();
        runtime.inject_fault_for_test(a,point,sqlite_code);
        assert(rejects([&] { runtime.complete(*claim,"must-not-be-silent"); }));
        assert((a.incident_flags() & AckStorageFailure)!=0);
        const auto incidents=runtime.quarantined_tenants();
        assert(incidents.size()==1 && incidents[0].first=="A");
        assert(rejects([&] { (void)runtime.pending(a,"lock"); }));
        assert(rejects([&] { runtime.retry(*claim); }));
        runtime.complete(*healthy,"healthy");
        healthy.reset();
        claim.reset(); // quarantine must make unwind nonfatal
        runtime.close_tenant(b);
    }
    {
        TenantRuntime restarted(root);
        auto a=restarted.recover_tenant("A");
        auto recovered=restarted.try_claim(a,"lock");
        assert(recovered && recovered->fence()>fence);
        assert(restarted.read_state(*recovered).bytes.empty());
        assert(restarted.read_state(*recovered).version==0);
        restarted.complete(*recovered,"safe");
        recovered.reset();
        restarted.close_tenant(a);
    }
    std::filesystem::remove_all(root);
}
// The contender must exec a fresh process image. A fork-only child inherits
// the parent's locked open-file-description; that could make this test pass
// for the wrong reason. The lock fd is O_CLOEXEC, so exec drops that copy.
static void assert_fresh_process_owner_result(const std::filesystem::path& root,
                                              bool expect_refusal) {
    const pid_t child=::fork();
    if (child<0) throw std::runtime_error("fork competing-owner probe failed");
    if (child==0) {
        const auto path=root.string();
        ::execl("/proc/self/exe","durable_tenant_fault_quarantine_test",
                expect_refusal ? "--probe-singleton-lock" : "--probe-clean-reopen",
                path.c_str(),static_cast<char*>(nullptr));
        _exit(84); // exec failed: never count this as a lock result
    }
    int status=0;
    if (::waitpid(child,&status,0)!=child)
        throw std::runtime_error("waitpid competing-owner probe failed");
    assert(WIFEXITED(status) && WEXITSTATUS(status)==0);
}
static void dispose_fault(const char* label, StorageFaultPoint point, unsigned bit) {
    const auto root=root_for(label);
    {
        TenantRuntime runtime(root);
        auto a=runtime.open_tenant("A");
        auto b=runtime.open_tenant("B");
        runtime.inject_fault_for_test(a,point);
        assert(rejects([&] { runtime.close_tenant(a); }));
        assert((a.incident_flags() & bit)!=0);
        // A retired marker must never disappear on a rejected disposal.
        assert(std::filesystem::is_regular_file(
            root/"tenant-41.sqlite.incident"));
        const auto incidents=runtime.quarantined_tenants();
        assert(incidents.size()==1 && incidents[0].first=="A");
        assert((incidents[0].second & bit)!=0);
        assert(rejects([&] { runtime.enqueue(a,"obj","lock","req","x"); }));
        TenantRuntime contender(root);
        assert(rejects([&] { (void)contender.open_tenant("A"); }));
        assert_fresh_process_owner_result(root,true);
        assert(runtime.enqueue(b,"obj","lock","req","ok").status==EnqueueStatus::Accepted);
        auto claim=runtime.try_claim(b,"lock");
        assert(claim);
        runtime.complete(*claim,"healthy");
        claim.reset();
        runtime.close_tenant(b);
    }
    // The tenant data remains recoverable when the host is safely restarted.
    {
        TenantRuntime restarted(root);
        assert(rejects([&]{ (void)restarted.open_tenant("A"); }));
        auto a=restarted.recover_tenant("A");
        assert(restarted.enqueue(a,"obj","lock","req","new").status==EnqueueStatus::Accepted);
        restarted.close_tenant(a);
    }
    // After explicit clean retirement, a genuinely independent process
    // must be able to open and close the same tenant (no leaked guard).
    assert_fresh_process_owner_result(root,false);
    std::filesystem::remove_all(root);
}
static void shutdown_with_outstanding_lease() {
    const auto root=root_for("shutdown-lease");
    std::optional<TenantRuntime::Lease> orphan;
    std::int64_t old_fence=0;
    {
        auto runtime=std::make_unique<TenantRuntime>(root);
        const auto a=runtime->open_tenant("A");
        assert(runtime->enqueue(a,"obj","lock","req","work").status==EnqueueStatus::Accepted);
        auto admitted=runtime->try_claim(a,"lock");
        assert(admitted);
        old_fence=admitted->fence();
        orphan.emplace(std::move(*admitted));
        admitted.reset();
        runtime.reset(); // a Lease still pins Core and tenant-owned POSIX/SQLite resources
        assert(orphan.has_value());
        orphan.reset(); // must retry, dispose and release owner lock WITHOUT terminating
    }
    {
        TenantRuntime restarted(root);
        auto a=restarted.recover_tenant("A");
        auto recovered=restarted.try_claim(a,"lock");
        assert(recovered && recovered->fence()>old_fence);
        restarted.complete(*recovered,"committed");
        recovered.reset();
        restarted.close_tenant(a);
    }
    std::filesystem::remove_all(root);
}
static void process_kill_before_ack() {
    const auto root=root_for("crash-before-ack");
    const pid_t child=::fork();
    if (child<0) throw std::runtime_error("fork failed");
    if (child==0) {
        try {
            TenantRuntime runtime(root);
            const auto a=runtime.open_tenant("A");
            if (runtime.enqueue(a,"obj","lock","req","payload").status
                != EnqueueStatus::Accepted) _exit(91);
            auto claimed=runtime.try_claim(a,"lock");
            if (!claimed || claimed->fence()!=1) _exit(92);
            // Abrupt death: neither ACK, retry nor destructors can run.
            _exit(42);
        } catch (...) { _exit(93); }
    }
    int status=0;
    if (::waitpid(child,&status,0)!=child)
        throw std::runtime_error("waitpid failed");
    assert(WIFEXITED(status) && WEXITSTATUS(status)==42);
    {
        TenantRuntime restarted(root);
        auto a=restarted.recover_tenant("A");
        assert(restarted.enqueue(a,"obj","lock","req","payload").status
               == EnqueueStatus::Duplicate);
        auto recovered=restarted.try_claim(a,"lock");
        assert(recovered && recovered->fence()>1);
        assert(restarted.read_state(*recovered).bytes.empty());
        assert(restarted.read_state(*recovered).version==0);
        restarted.complete(*recovered,"committed-after-kill");
        recovered.reset();
        restarted.close_tenant(a);
    }
    std::filesystem::remove_all(root);
}
static void admission_or_claim_commit_failure(
    const char* label, StorageFaultPoint point, unsigned expected_flag) {
    const auto root=root_for(label);
    {
        TenantRuntime runtime(root);
        auto a=runtime.open_tenant("tenant-A");
        auto b=runtime.open_tenant("tenant-B");
        assert(runtime.enqueue(b,"obj","lock","req-B","b").status==EnqueueStatus::Accepted);
        if (point==StorageFaultPoint::ClaimBeforeCommit)
            assert(runtime.enqueue(a,"obj","lock","req-A","a").status==EnqueueStatus::Accepted);
        runtime.inject_fault_for_test(a,point,SQLITE_IOERR);
        if (point==StorageFaultPoint::EnqueueBeforeCommit)
            assert(rejects([&] { (void)runtime.enqueue(a,"obj","lock","req-A","a"); }));
        else
            assert(rejects([&] { (void)runtime.try_claim(a,"lock"); }));
        assert((a.incident_flags() & expected_flag)!=0);
        assert(rejects([&] { (void)runtime.try_claim(a,"lock"); }));
        assert(rejects([&] { (void)runtime.enqueue(a,"obj","lock","another","x"); }));
        auto healthy=runtime.try_claim(b,"lock");
        assert(healthy && healthy->payload()=="b");
        runtime.complete(*healthy,"healthy");
        healthy.reset();
        runtime.close_tenant(b);
    }
    {
        TenantRuntime runtime(root);
        auto a=runtime.recover_tenant("tenant-A");
        // The injected error happened before COMMIT, so SQLite rolled back.
        // A real xSync failure has an ambiguous outcome; see VFS integration.
        assert(runtime.enqueue(a,"obj","lock","req-A","a").status
               == (point==StorageFaultPoint::EnqueueBeforeCommit
                       ? EnqueueStatus::Accepted : EnqueueStatus::Duplicate));
        auto claim=runtime.try_claim(a,"lock");
        assert(claim && claim->payload()=="a");
        runtime.complete(*claim,"recovered");
        claim.reset();
        runtime.close_tenant(a);
        auto b=runtime.open_tenant("tenant-B");
        assert(runtime.enqueue(b,"obj","lock","req-B","b").status==EnqueueStatus::Duplicate);
        runtime.close_tenant(b);
    }
    std::filesystem::remove_all(root);
}

static void read_failure_quarantines_tenant(const char* label, StorageFaultPoint point) {
    const auto root=root_for(label);
    std::int64_t first_fence=0;
    {
        TenantRuntime runtime(root);
        auto a=runtime.open_tenant("tenant-A");
        auto b=runtime.open_tenant("tenant-B");
        assert(runtime.enqueue(a,"obj","lock","req-A","a").status==EnqueueStatus::Accepted);
        assert(runtime.enqueue(b,"obj","lock","req-B","b").status==EnqueueStatus::Accepted);
        auto lease_a=runtime.try_claim(a,"lock");
        assert(lease_a);
        first_fence=lease_a->fence();
        runtime.inject_fault_for_test(a,point,SQLITE_IOERR);
        if (point==StorageFaultPoint::ReadState)
            assert(rejects([&] { (void)runtime.read_state(*lease_a); }));
        else
            assert(rejects([&] { (void)runtime.pending(a,"lock"); }));
        assert((a.incident_flags() & ReadStorageFailure)!=0);
        assert(rejects([&] { (void)runtime.enqueue(a,"obj","lock","new","x"); }));
        assert(rejects([&] { (void)runtime.try_claim(a,"lock"); }));
        assert(rejects([&] { runtime.complete(*lease_a,"should-not-commit"); }));
        assert(rejects([&] { runtime.close_tenant(a); }));
        auto healthy=runtime.try_claim(b,"lock");
        assert(healthy && runtime.read_state(*healthy).version==0);
        runtime.complete(*healthy,"healthy");
        healthy.reset();
        runtime.close_tenant(b);
        lease_a.reset(); // destructor must be noexcept, even when quarantined
    }
    {
        TenantRuntime restarted(root);
        auto a=restarted.recover_tenant("tenant-A");
        auto recovered=restarted.try_claim(a,"lock");
        assert(recovered && recovered->fence()>first_fence);
        auto state=restarted.read_state(*recovered);
        assert(state.version==0 && state.bytes.empty());
        restarted.complete(*recovered,"recovered");
        recovered.reset();
        restarted.close_tenant(a);
        auto b=restarted.open_tenant("tenant-B");
        assert(restarted.enqueue(b,"obj","lock","req-B","b").status==EnqueueStatus::Duplicate);
        restarted.close_tenant(b);
    }
    std::filesystem::remove_all(root);
}

// Persistent marker contract: clean close clears it, an unclean exit
// cannot silently restart, and missing/forged storage remains quarantined.
static void incident_marker_lifecycle_regression() {
    const auto root=root_for("incident-lifecycle");
    const auto db=root/"tenant-41.sqlite";
    const auto marker=std::filesystem::path(db.string()+".incident");
    {
        TenantRuntime clean(root);
        assert(rejects([&]{ (void)clean.recover_tenant("A"); }));
        auto a=clean.open_tenant("A");
        assert(std::filesystem::is_regular_file(marker));
        // WAL is a persisted database property: verify it through a separate
        // SQLite connection, not just the runtime's successful PRAGMA return.
        sqlite3* verifier=nullptr;
        assert(sqlite3_open_v2(db.c_str(),&verifier,
               SQLITE_OPEN_READWRITE|SQLITE_OPEN_FULLMUTEX,nullptr)==SQLITE_OK);
        sqlite3_stmt* journal=nullptr;
        assert(sqlite3_prepare_v2(verifier,"PRAGMA journal_mode",-1,
                                  &journal,nullptr)==SQLITE_OK);
        assert(sqlite3_step(journal)==SQLITE_ROW);
        const unsigned char* mode=sqlite3_column_text(journal,0);
        assert(mode && std::string(reinterpret_cast<const char*>(mode))=="wal");
        assert(sqlite3_step(journal)==SQLITE_DONE);
        assert(sqlite3_finalize(journal)==SQLITE_OK);
        assert(sqlite3_close(verifier)==SQLITE_OK);
        clean.close_tenant(a);
        assert(!std::filesystem::exists(marker));
    }
    {
        TenantRuntime abandoned(root);
        auto a=abandoned.open_tenant("A");
        assert(abandoned.enqueue(a,"obj","lock","req","body").status ==
               EnqueueStatus::Accepted);
        // No explicit close: the marker must survive scope exit.
    }
    assert(std::filesystem::is_regular_file(marker));
    {
        TenantRuntime supervisor(root);
        assert(rejects([&]{ (void)supervisor.open_tenant("A"); }));
        auto a=supervisor.recover_tenant("A");
        assert(supervisor.enqueue(a,"obj","lock","req","body").status ==
               EnqueueStatus::Duplicate);
        supervisor.close_tenant(a);
    }
    assert(!std::filesystem::exists(marker));
    {
        TenantRuntime abandoned(root);
        auto a=abandoned.open_tenant("A");
        (void)a;
    }
    assert(std::filesystem::is_regular_file(marker));
    assert(std::filesystem::remove(db));
    {
        TenantRuntime supervisor(root);
        assert(rejects([&]{ (void)supervisor.open_tenant("A"); }));
        assert(rejects([&]{ (void)supervisor.recover_tenant("A"); }));
        assert(std::filesystem::is_regular_file(marker));
    }
    std::filesystem::remove_all(root);

    const auto forged=root_for("incident-forged-marker");
    const auto forged_db=forged/"tenant-41.sqlite";
    const auto forged_marker=std::filesystem::path(forged_db.string()+".incident");
    {
        TenantRuntime clean(forged);
        auto a=clean.open_tenant("A");
        clean.close_tenant(a);
    }
    std::filesystem::create_symlink(forged_db,forged_marker);
    {
        TenantRuntime supervisor(forged);
        assert(rejects([&]{ (void)supervisor.open_tenant("A"); }));
        assert(rejects([&]{ (void)supervisor.recover_tenant("A"); }));
    }
    std::filesystem::remove(forged_marker);
    // Regression: an O_RDONLY open of a FIFO would block forever before
    // fstat could reject it. Marker validation must use nonblocking I/O.
    assert(::mkfifo(forged_marker.c_str(),0600)==0);
    {
        TenantRuntime supervisor(forged);
        assert(rejects([&]{ (void)supervisor.open_tenant("A"); }));
        assert(rejects([&]{ (void)supervisor.recover_tenant("A"); }));
    }
    std::filesystem::remove_all(forged);
}
int main(int argc, char** argv) {
    if (argc==3 && (std::string(argv[1])=="--probe-singleton-lock" ||
                    std::string(argv[1])=="--probe-clean-reopen")) {
        // A new process after exec: no inherited O_CLOEXEC tenant lock.
        const bool expect_refusal=std::string(argv[1])=="--probe-singleton-lock";
        try {
            TenantRuntime contender(argv[2]);
            auto handle=contender.open_tenant("A");
            if (expect_refusal) return 81; // second owner incorrectly admitted
            contender.close_tenant(handle);
            return 0; // clean handoff to a second process is permitted
        } catch (const std::logic_error& e) {
            if (!expect_refusal) return 82;
            return std::string(e.what())=="tenant already has an active process owner"
                      ? 0 : 82; // unrelated validation error is not a pass
        } catch (...) {
            return 83;
        }
    }
    if (argc!=1) return 85;
    retry_fault("retry-ioerr",StorageFaultPoint::RetryBeforeWrite,SQLITE_IOERR,RetryStorageFailure);
    retry_fault("retry-full",StorageFaultPoint::RetryBeforeCommit,SQLITE_FULL,RetryStorageFailure);
    retry_fault("rollback",StorageFaultPoint::Rollback,SQLITE_IOERR,
                RetryStorageFailure | RollbackStorageFailure);
    ack_fault("ack-before-write-ioerr",StorageFaultPoint::AckBeforeWrite,SQLITE_IOERR);
    ack_fault("ack-before-commit-full",StorageFaultPoint::AckBeforeCommit,SQLITE_FULL);
    dispose_fault("sqlite-close",StorageFaultPoint::SqliteClose,SqliteCloseFailure);
    dispose_fault("flock-unlock",StorageFaultPoint::LockUnlock,LockUnlockFailure);
    dispose_fault("fd-close",StorageFaultPoint::FileClose,FileCloseFailure);
    // Simulated interruption after primary fd close but before marker unlink.
    // A duplicate must continue to own the exact same flock description.
    dispose_fault("incident-retire",StorageFaultPoint::IncidentUnlink,FileCloseFailure);
    // Failure AFTER unlink: the runtime must re-arm the marker while the
    // duplicate owner flock still blocks all competing processes.
    dispose_fault("incident-dir-sync",StorageFaultPoint::IncidentDirSync,FileCloseFailure);
    // Failure at the final guard-release boundary: marker must be durably
    // re-armed WHILE the same open-file-description still owns the flock.
    // A separate exec'd process must observe lock refusal; tenant B works.
    dispose_fault("incident-guard-release",StorageFaultPoint::IncidentGuardRelease,FileCloseFailure);
    shutdown_with_outstanding_lease();
    process_kill_before_ack();
    admission_or_claim_commit_failure("enqueue-commit",StorageFaultPoint::EnqueueBeforeCommit,
                                      AdmissionStorageFailure);
    admission_or_claim_commit_failure("claim-commit",StorageFaultPoint::ClaimBeforeCommit,
                                      ClaimStorageFailure);
    read_failure_quarantines_tenant("read-state",StorageFaultPoint::ReadState);
    read_failure_quarantines_tenant("pending-count",StorageFaultPoint::PendingQuery);
    incident_marker_lifecycle_regression();
}
