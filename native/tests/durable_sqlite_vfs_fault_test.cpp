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
            // Restart with the stock VFS. The failed ACK must NOT be
            // represented as a committed state change or invisible job.
            TenantRuntime restarted(root);
            auto a=restarted.open_tenant("A");
            auto pending=restarted.try_claim(a,"lock");
            require(bool(pending) && pending->fence()>old_fence,
                    "recover pending job at strictly higher fence");
            auto snapshot=restarted.read_state(*pending);
            require(snapshot.version==0 && snapshot.bytes.empty(),
                    "failed ACK must not publish object state");
            restarted.complete(*pending,"recovered");
            pending.reset();
            restarted.close_tenant(a);
            auto b=restarted.open_tenant("B");
            require(restarted.enqueue(b,"object","lock","req-B","payload").status==
                    EnqueueStatus::Duplicate,"healthy tenant receipt missing");
            restarted.close_tenant(b);
        }
    } catch (...) { std::filesystem::remove_all(root); throw; }
    std::filesystem::remove_all(root);
}
int main() {
    exercise(FaultVfs::Point::WalWrite);
    exercise(FaultVfs::Point::WalSync);
}
