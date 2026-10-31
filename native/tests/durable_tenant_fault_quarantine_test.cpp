#include "ores/durable/tenant_runtime.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <utility>
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
        const auto a = restarted.open_tenant("tenant-A");
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
        auto a=restarted.open_tenant("A");
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
static void dispose_fault(const char* label, StorageFaultPoint point, unsigned bit) {
    const auto root=root_for(label);
    {
        TenantRuntime runtime(root);
        auto a=runtime.open_tenant("A");
        auto b=runtime.open_tenant("B");
        runtime.inject_fault_for_test(a,point);
        assert(rejects([&] { runtime.close_tenant(a); }));
        assert((a.incident_flags() & bit)!=0);
        const auto incidents=runtime.quarantined_tenants();
        assert(incidents.size()==1 && incidents[0].first=="A");
        assert((incidents[0].second & bit)!=0);
        assert(rejects([&] { runtime.enqueue(a,"obj","lock","req","x"); }));
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
        auto a=restarted.open_tenant("A");
        assert(restarted.enqueue(a,"obj","lock","req","new").status==EnqueueStatus::Accepted);
        restarted.close_tenant(a);
    }
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
        auto a=restarted.open_tenant("A");
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
        auto a=restarted.open_tenant("A");
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
        auto a=runtime.open_tenant("tenant-A");
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
int main() {
    retry_fault("retry-ioerr",StorageFaultPoint::RetryBeforeWrite,SQLITE_IOERR,RetryStorageFailure);
    retry_fault("retry-full",StorageFaultPoint::RetryBeforeCommit,SQLITE_FULL,RetryStorageFailure);
    retry_fault("rollback",StorageFaultPoint::Rollback,SQLITE_IOERR,
                RetryStorageFailure | RollbackStorageFailure);
    ack_fault("ack-before-write-ioerr",StorageFaultPoint::AckBeforeWrite,SQLITE_IOERR);
    ack_fault("ack-before-commit-full",StorageFaultPoint::AckBeforeCommit,SQLITE_FULL);
    dispose_fault("sqlite-close",StorageFaultPoint::SqliteClose,SqliteCloseFailure);
    dispose_fault("flock-unlock",StorageFaultPoint::LockUnlock,LockUnlockFailure);
    dispose_fault("fd-close",StorageFaultPoint::FileClose,FileCloseFailure);
    shutdown_with_outstanding_lease();
    process_kill_before_ack();
    admission_or_claim_commit_failure("enqueue-commit",StorageFaultPoint::EnqueueBeforeCommit,
                                      AdmissionStorageFailure);
    admission_or_claim_commit_failure("claim-commit",StorageFaultPoint::ClaimBeforeCommit,
                                      ClaimStorageFailure);
}
