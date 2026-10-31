#include "ores/durable/tenant_runtime.hpp"
#include "durable_fault_vfs.hpp"

#include <filesystem>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <unistd.h>

using ores::durable::TenantRuntime;
using ores::durable::EnqueueStatus;
using ores::durable::AckStorageFailure;
using ores::durable::test::FaultVfs;

static void require(bool condition, const char* reason) {
    if (!condition) throw std::runtime_error(reason);
}
template<class F> static bool rejects(F&& f) {
    try { f(); } catch (const std::exception&) { return true; }
    return false;
}
static void exercise(FaultVfs::Point fault) {
    auto root=(std::filesystem::temp_directory_path()/"ores-durable-vfs-XXXXXX").string();
    if (!::mkdtemp(root.data())) throw std::runtime_error("mkdtemp failed");
    try {
        std::int64_t old_fence=0;
        {
            // SQLite's default VFS is replaced ONLY in this isolated test
            // executable, and the interception is restricted to A's WAL.
            FaultVfs vfs("tenant-41.sqlite-wal");
            TenantRuntime runtime(root);
            const auto a=runtime.open_tenant("A");
            const auto b=runtime.open_tenant("B");
            require(runtime.enqueue(a,"object","lock","req-A","payload").status==
                    EnqueueStatus::Accepted,"A admission");
            require(runtime.enqueue(b,"object","lock","req-B","payload").status==
                    EnqueueStatus::Accepted,"B admission");
            auto lease_a=runtime.try_claim(a,"lock");
            auto lease_b=runtime.try_claim(b,"lock");
            require(bool(lease_a) && bool(lease_b),"initial claims");
            old_fence=lease_a->fence();
            const unsigned before=vfs.faults();
            vfs.arm(fault);
            require(rejects([&]{ runtime.complete(*lease_a,"uncommitted"); }),
                    "real VFS I/O failure must reject ACK");
            require(vfs.faults()==before+1,"targeted WAL VFS callback not invoked");
            require((a.incident_flags() & AckStorageFailure)!=0,
                    "faulted tenant must be quarantined");
            require(rejects([&]{ runtime.enqueue(a,"object","lock","new","no"); }),
                    "quarantined tenant admitted new work");
            require(rejects([&]{ runtime.retry(*lease_a); }),
                    "quarantined tenant retried uncertain ACK");
            // The healthy tenant uses a different WAL and remains usable.
            runtime.complete(*lease_b,"healthy");
            lease_b.reset();
            runtime.close_tenant(b);
            lease_a.reset(); // noexcept destructor must never terminate host
            require(rejects([&]{ runtime.close_tenant(a); }),
                    "quarantined tenant close unexpectedly succeeded");
        }
        {
            // Restart with the stock VFS. Failed write/sync may leave an
            // uncertain commit, but state+ACK must remain atomic.
            TenantRuntime restarted(root);
            auto a=restarted.open_tenant("A");
            require(restarted.enqueue(a,"object","lock","req-A","payload").status==
                    EnqueueStatus::Duplicate,"missing immutable request receipt");
            auto pending=restarted.try_claim(a,"lock");
            if (pending) {
                require(pending->fence()>old_fence,
                        "pending job must recover at a higher fence");
                auto snapshot=restarted.read_state(*pending);
                require(snapshot.version==0 && snapshot.bytes.empty(),
                        "state changed without atomic ACK");
                restarted.complete(*pending,"recovered");
                pending.reset();
            } else {
                // A failed fsync can leave the commit result ambiguous.
                // If ACK is already visible, the state write MUST also be
                // visible atomically; never assume a failed sync rolled back.
                require(restarted.enqueue(a,"object","lock","probe","read").status==
                        EnqueueStatus::Accepted,"probe admission");
                auto probe=restarted.try_claim(a,"lock");
                require(bool(probe),"probe queue missing");
                const auto snapshot=restarted.read_state(*probe);
                require(snapshot.version==1 && snapshot.bytes=="uncommitted",
                        "ACK visible without its committed state");
                restarted.complete(*probe,"uncommitted");
                probe.reset();
            }
            restarted.close_tenant(a);
            auto b=restarted.open_tenant("B");
            require(restarted.enqueue(b,"object","lock","req-B","payload").status==
                    EnqueueStatus::Duplicate,"healthy tenant receipt missing");
            restarted.close_tenant(b);
        }
    } catch (...) { std::filesystem::remove_all(root); throw; }
    std::filesystem::remove_all(root);
}
static void exercise_enqueue_or_claim_fault(FaultVfs::Point fault, bool at_claim) {
    auto root=(std::filesystem::temp_directory_path()/"ores-durable-vfs-admission-XXXXXX").string();
    if (!::mkdtemp(root.data())) throw std::runtime_error("mkdtemp failed");
    try {
        {
            FaultVfs vfs("tenant-41.sqlite-wal");
            TenantRuntime runtime(root);
            const auto a=runtime.open_tenant("A");
            const auto b=runtime.open_tenant("B");
            require(runtime.enqueue(b,"object","lock","req-B","payload").status==
                    EnqueueStatus::Accepted,"B admission before A WAL fault");
            if (at_claim)
                require(runtime.enqueue(a,"object","lock","req-A","payload").status==
                        EnqueueStatus::Accepted,"A admission before claim fault");
            const unsigned before=vfs.faults();
            vfs.arm(fault);
            if (at_claim) {
                require(rejects([&]{ (void)runtime.try_claim(a,"lock"); }),
                        "VFS claim failure unexpectedly succeeded");
                require((a.incident_flags() & ores::durable::ClaimStorageFailure)!=0,
                        "claim failure did not quarantine tenant A");
            } else {
                require(rejects([&]{ (void)runtime.enqueue(a,"object","lock","req-A","payload"); }),
                        "VFS admission failure unexpectedly succeeded");
                require((a.incident_flags() & ores::durable::AdmissionStorageFailure)!=0,
                        "admission failure did not quarantine tenant A");
            }
            require(vfs.faults()==before+1,"targeted A WAL callback not triggered");
            require(rejects([&]{ (void)runtime.enqueue(a,"object","lock","new","no"); }),
                    "quarantined tenant A admitted new work");
            require(rejects([&]{ (void)runtime.try_claim(a,"lock"); }),
                    "quarantined tenant A admitted a new lease");
            auto healthy=runtime.try_claim(b,"lock");
            require(bool(healthy),"tenant B claim after A WAL failure");
            runtime.complete(*healthy,"healthy");
            healthy.reset();
            runtime.close_tenant(b);
        }
        {
            TenantRuntime restarted(root);
            auto a=restarted.open_tenant("A");
            // An xSync failure can return an error AFTER a commit became
            // durable. Both original receipt states are permissible.
            const auto receipt=restarted.enqueue(a,"object","lock","req-A","payload");
            require(receipt.status==EnqueueStatus::Duplicate ||
                    receipt.status==EnqueueStatus::Accepted,
                    "restart admission/dedupe state incoherent");
            auto claim=restarted.try_claim(a,"lock");
            require(bool(claim),"restart claim of unacknowledged A job missing");
            require(restarted.read_state(*claim).version==0,
                    "unexpected state change from failed admission/claim");
            restarted.complete(*claim,"recovered");
            claim.reset();
            restarted.close_tenant(a);
            auto b=restarted.open_tenant("B");
            require(restarted.enqueue(b,"object","lock","req-B","payload").status==
                    EnqueueStatus::Duplicate,"tenant B lost immutable receipt");
            restarted.close_tenant(b);
        }
    } catch (...) { std::filesystem::remove_all(root); throw; }
    std::filesystem::remove_all(root);
}
int main() {
    exercise(FaultVfs::Point::WalWrite);
    exercise(FaultVfs::Point::WalSync);
    exercise_enqueue_or_claim_fault(FaultVfs::Point::WalWrite,false);
    exercise_enqueue_or_claim_fault(FaultVfs::Point::WalSync,false);
    exercise_enqueue_or_claim_fault(FaultVfs::Point::WalWrite,true);
    exercise_enqueue_or_claim_fault(FaultVfs::Point::WalSync,true);
}
