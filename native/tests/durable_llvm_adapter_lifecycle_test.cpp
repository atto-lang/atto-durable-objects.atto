#include "ores/durable/llvm_oresvm_adapter.hpp"
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

using ores::durable::EnqueueStatus;
using ores::durable::LlvmDurableIsolates;

template<class F> static bool rejects(F&& f) {
    try { f(); } catch (const std::exception&) { return true; }
    return false;
}
static void require(bool condition, const char* what) {
    if (!condition) throw std::runtime_error(what);
}

int main() {
    // mkdtemp creates a private directory without a predictable-path race.
    auto pattern=(std::filesystem::temp_directory_path()/"ores-durable-llvm-XXXXXX").string();
    if (!::mkdtemp(pattern.data())) throw std::runtime_error("mkdtemp failed");
    try {
        LlvmDurableIsolates host(pattern);
        auto a=host.open_tenant("tenant-a",4096,4);
        auto b=host.open_tenant("tenant-b",4096,4);
        require(host.enqueue(a,"counter","lock-a","req-1","payload").status
                ==EnqueueStatus::Accepted,"durable admission");
        auto lease=host.try_claim(a,"lock-a");
        require(lease.has_value(),"durable lease admitted");

        // The VM must stop admitting guest/queue work BEFORE storage retirement.
        // Failed close because of a live lease must keep the store registered,
        // while allowing the in-flight lease to be settled on the next call.
        require(rejects([&] { host.close_tenant(a); }),"close must reject live lease");
        require(rejects([&] { host.enqueue(a,"counter","lock-a","req-2","x"); }),
                "shutting-down VM must not admit durable enqueue");
        require(rejects([&] { (void)host.try_claim(a,"lock-a"); }),
                "shutting-down VM must not admit another durable lease");
        require(host.enqueue(b,"counter","lock-b","req-b","healthy").status
                ==EnqueueStatus::Accepted,"other tenant still progressing");
        host.retry(*lease);
        lease.reset();
        // Simulate a destroy-time interruption after confirmed store close.
        // A retry must skip store shutdown, not use a retired SQLite ticket.
        host.inject_vm_destroy_failure_once_for_test();
        require(rejects([&] { host.close_tenant(a); }),
                "injected VM destroy interruption must be reported");
        require(rejects([&] { host.enqueue(a,"counter","lock-a","req-3","x"); }),
                "retired store must reject admission");
        require(rejects([&] { (void)host.pending(a,"lock-a"); }),
                "retired store must reject reads");
        host.close_tenant(a);
        require(rejects([&] { (void)host.snapshot(a); }),"closed ticket must be stale");
        // No stale tenant registration or VM generation may survive.
        auto reopened=host.open_tenant("tenant-a",4096,4);
        host.close_tenant(reopened);
        host.close_tenant(b);

        // Force store-open failure AFTER isolate creation. The VM registration
        // must roll back, permitting another open of the exact same tenant ID.
        const auto lockfile=std::filesystem::path(pattern)/"tenant-6661696c.sqlite.lock";
        { std::ofstream f(lockfile); f<<"unsafe lock mode"; }
        require(::chmod(lockfile.c_str(),0644)==0,"chmod fault fixture");
        require(rejects([&] { (void)host.open_tenant("fail",4096,4); }),
                "reject insecure lock file");
        std::filesystem::remove(lockfile);
        auto fresh=host.open_tenant("fail",4096,4);
        host.close_tenant(fresh);
    } catch (...) {
        std::filesystem::remove_all(pattern);
        throw;
    }
    std::filesystem::remove_all(pattern);
}
