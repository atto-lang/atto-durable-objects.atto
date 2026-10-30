#include "ores/durable/tenant_runtime.hpp"
#include <cassert>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>
#include <atomic>
#include <sys/wait.h>
#include <unistd.h>

using namespace ores::durable;
template<class F> bool rejects(F&& f) {try {f();} catch (const std::exception&) {return true;}return false;}
static_assert(!std::is_copy_constructible<TenantRuntime::Lease>::value,"exclusive lease must not copy");
static_assert(!std::is_default_constructible<TenantRuntime::Lease>::value,"guest cannot mint a lease");

int main(int argc, char** argv) {
    if (argc == 3 && std::string(argv[1])=="--crash-child") {
        Limits q;
        q.max_tenants=2;q.max_lock_queues=2;q.max_jobs_per_lock=2;
        q.max_jobs_per_tenant=4;q.max_request_bytes=64;q.max_state_bytes=64;
        TenantRuntime rt(argv[2],q);
        auto tenant=rt.open_tenant("tenant-A");
        const auto admitted=rt.enqueue(tenant,"counter","lock-1","crash-work","recover");
        assert(admitted.status==EnqueueStatus::Accepted);
        auto lease=rt.try_claim(tenant,"lock-1");
        assert(lease && lease->payload()=="recover");
        // Simulate a process crash after persistent admission/claim, before ACK.
        // Do not invoke lease, SQLite, or VM destructors.
        _exit(42);
    }

    const auto root=std::filesystem::temp_directory_path()/
        ("ores-durable-test-"+std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directory(root);
    std::filesystem::permissions(root, std::filesystem::perms::owner_all,
                                  std::filesystem::perm_options::replace);
    try {
        Limits config;
        config.max_tenants=2;
        config.max_jobs_per_lock=2;
        config.max_jobs_per_tenant=4;
        config.max_lock_queues=2;
        config.max_request_bytes=64;
        config.max_state_bytes=64;
        {
            TenantRuntime runtime(root,config);
            auto tenant_a=runtime.open_tenant("tenant-A");
            auto tenant_b=runtime.open_tenant("tenant-B");
            assert(rejects([&] {runtime.open_tenant("tenant-A");}));
            assert(rejects([&] {runtime.open_tenant("tenant-C");}));
            assert(rejects([&] {runtime.enqueue(tenant_a,"bad\nkey","k","req","x");}));
            auto one=runtime.enqueue(tenant_a,"counter","lock-1","req-1","add 1");
            assert(one.status==EnqueueStatus::Accepted && one.sequence>0);
            assert(runtime.enqueue(tenant_a,"counter","lock-1","req-1","add 1").status==EnqueueStatus::Duplicate);
            assert(rejects([&] {runtime.enqueue(tenant_a,"counter","lock-1","req-1","changed");}));
            assert(rejects([&] {runtime.enqueue(tenant_a,"counter","lock-2","req-2","changed");}));
            assert(runtime.enqueue(tenant_a,"counter","lock-1","req-2","add 2").status==EnqueueStatus::Accepted);
            assert(runtime.enqueue(tenant_a,"counter","lock-1","req-3","add 3").status==EnqueueStatus::LockFull);
            assert(runtime.enqueue(tenant_a,"other","lock-2","req-4","a").status==EnqueueStatus::Accepted);
            assert(rejects([&] {runtime.enqueue(tenant_a,"other","lock-3","req-5","x");}));
            assert(runtime.enqueue(tenant_b,"counter","lock-1","req-1","tenant b").status==EnqueueStatus::Accepted);
            assert(runtime.pending(tenant_a,"lock-1")==2);
            assert(runtime.pending(tenant_b,"lock-1")==1);
            auto first=runtime.try_claim(tenant_a,"lock-1");
            assert(first.has_value() && first->payload()=="add 1");
            assert(first->fence()==1);
            assert(runtime.read_state(*first).version==0);
            assert(!runtime.try_claim(tenant_a,"lock-1").has_value()); // same lock serialization
            auto cross=runtime.try_claim(tenant_a,"lock-2");
            assert(cross && cross->payload()=="a"); // different lock concurrently active
            auto b=runtime.try_claim(tenant_b,"lock-1");
            assert(b && b->payload()=="tenant b");
            assert(rejects([&] {runtime.close_tenant(tenant_a);}));
            runtime.complete(*first,"1");
            assert(rejects([&] {runtime.complete(*first,"2");}));
            first.reset();
            auto second=runtime.try_claim(tenant_a,"lock-1");
            assert(second && second->payload()=="add 2");
            assert(runtime.read_state(*second).version==1);
            assert(runtime.read_state(*second).bytes=="1");
            // Exception/unwind automatically retries once without losing queue priority.
            const auto before=second->fence();
            second.reset();
            auto retried=runtime.try_claim(tenant_a,"lock-1");
            assert(retried && retried->fence()>before);
            runtime.complete(*retried,"3");
            retried.reset();
            runtime.complete(*cross,"other-state"); cross.reset();
            assert(runtime.pending(tenant_a,"lock-1")==0);
            // Fully independent object state in a separate tenant store.
            assert(runtime.read_state(*b).bytes.empty());
            runtime.complete(*b,"tenant-b-state"); b.reset();
            runtime.close_tenant(tenant_a);
            assert(rejects([&] {runtime.enqueue(tenant_a,"counter","lock-1","new","x");}));
            runtime.close_tenant(tenant_b);
        }
        {
            TenantRuntime after_restart(root,config);
            const auto a=after_restart.open_tenant("tenant-A");
            const auto b=after_restart.open_tenant("tenant-B");
            assert(after_restart.enqueue(a,"counter","lock-1","req-1","add 1").status==EnqueueStatus::Duplicate);
            assert(after_restart.enqueue(b,"counter","lock-1","req-1","tenant b").status==EnqueueStatus::Duplicate);
            after_restart.enqueue(a,"counter","lock-1","req-new","read");
            auto lease=after_restart.try_claim(a,"lock-1");
            assert(lease);
            assert(after_restart.read_state(*lease).bytes=="3");
            assert(after_restart.read_state(*lease).version==2);
            after_restart.complete(*lease,"4");lease.reset();
            after_restart.close_tenant(a);
            after_restart.close_tenant(b);
        }
        // This process genuinely execs the --crash-child mode. The child
        // claims persisted work and exits via _exit(42), without unwinding
        // a lease or closing SQLite. Reopen must fence the abandoned claim.
        {
            const auto child_root = root / "crash-recovery";
            const pid_t child = ::fork();
            if (child < 0) throw std::runtime_error("fork crash child failed");
            if (child == 0) {
                const auto child_path = child_root.string();
                ::execl(argv[0], argv[0], "--crash-child", child_path.c_str(),
                        static_cast<char*>(nullptr));
                _exit(127);
            }
            int status = 0;
            pid_t waited;
            do { waited = ::waitpid(child, &status, 0); }
            while (waited == -1 && errno == EINTR);
            assert(waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 42);
            TenantRuntime recovered(child_root, config);
            auto tenant = recovered.open_tenant("tenant-A");
            assert(recovered.pending(tenant, "lock-1") == 1);
            auto lease = recovered.try_claim(tenant, "lock-1");
            assert(lease && lease->payload() == "recover" && lease->fence() > 1);
            recovered.complete(*lease, "completed-after-crash");
            lease.reset();
            assert(recovered.pending(tenant, "lock-1") == 0);
            assert(recovered.enqueue(tenant, "counter", "lock-1", "crash-work",
                                     "recover").status == EnqueueStatus::Duplicate);
            recovered.close_tenant(tenant);
        }
        // Security regression: refuse a symlinked store root even when its
        // destination is owner-only. An attacker must not redirect WAL paths.
        {
            const auto alias = root / "store-alias";
            std::filesystem::create_directory_symlink(root, alias);
            assert(rejects([&] { TenantRuntime bad(alias, config); }));
            std::filesystem::remove(alias);
        }
        // Refuse group-readable roots; never chmod an existing path behind
        // the operator's back.
        {
            const auto readable = root / "group-readable";
            std::filesystem::create_directory(readable);
            std::filesystem::permissions(
                readable, std::filesystem::perms::owner_all |
                    std::filesystem::perms::group_read,
                std::filesystem::perm_options::replace);
            assert(rejects([&] { TenantRuntime bad(readable, config); }));
        }
        // A tenant DB must be an owner-only regular file, never a symlink.
        {
            const auto private_root = root / "symlink-db";
            TenantRuntime guarded(private_root, config);
            std::filesystem::create_symlink(private_root / "target",
                                            private_root / "tenant-74.sqlite");
            assert(rejects([&] { guarded.open_tenant("t"); }));
        }
        // The same durable tenant must not have two concurrent VM leaders.
        {
            TenantRuntime p1(root,config),p2(root,config);
            auto a=p1.open_tenant("tenant-A");
            assert(rejects([&] {p2.open_tenant("tenant-A");}));
            p1.close_tenant(a);
            auto a2=p2.open_tenant("tenant-A");
            p2.close_tenant(a2);
        }
    } catch (...) {std::filesystem::remove_all(root);throw;}
    std::filesystem::remove_all(root);
}
