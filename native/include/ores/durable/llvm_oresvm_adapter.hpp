#pragma once

// Optional bridge compiled only against the *original* LLVM runtime headers
// from oreslang-source.llvm PR #1158 or its compatible successor.
// Do not mirror those private headers into this repository.
#include "ores/durable/tenant_runtime.hpp"
#include "oreslang/runtime/OresVM.hpp"

#include <memory>
#include <optional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace ores::durable {

// Host control-plane adapter. Exactly one OresVM IsolateHandle per tenant;
// many durable lock_id queues share that single isolate, never a VM per lock.
// This requires the actual OresVM runtime library and is not a guest ABI.
class LlvmDurableIsolates final {
    using VM = oreslang::runtime::OresVM;
    struct Tenant final {
        std::string id;
        VM::IsolateHandle vm;
        TenantRuntime::TenantHandle store;
        Tenant(std::string name, VM::IsolateHandle isolate,
               TenantRuntime::TenantHandle durable)
            : id(std::move(name)), vm(std::move(isolate)), store(std::move(durable)) {}
    };
public:
    class Handle final {
        friend class LlvmDurableIsolates;
        std::weak_ptr<Tenant> tenant_;
        explicit Handle(const std::shared_ptr<Tenant>& t) : tenant_(t) {}
    public:
        Handle() = default;
    };

    explicit LlvmDurableIsolates(std::filesystem::path dir, Limits limits = {})
        : queues_(std::move(dir),limits) {}
    LlvmDurableIsolates(const LlvmDurableIsolates&) = delete;
    LlvmDurableIsolates& operator=(const LlvmDurableIsolates&) = delete;

    Handle open_tenant(const std::string& id,
                       std::uint32_t heap_bytes, std::uint32_t max_actors) {
        std::lock_guard<std::mutex> guard(mu_);
        if(tenants_.count(id)) throw std::logic_error("tenant already has a VM isolate");
        oreslang::runtime::IsolateConfig cfg;
        cfg.tenant_id=id;
        cfg.heap_limit_bytes=heap_bytes;
        cfg.max_actors=max_actors;
        cfg.mode=oreslang::runtime::IsolateMode::TrustedInProcess;
        auto isolate=vm_.create_isolate(std::move(cfg));
        std::optional<TenantRuntime::TenantHandle> store;
        try {
            // Start belongs inside the cleanup region: a failed start must
            // not leave a created isolate permanently occupying the tenant ID.
            vm_.start(isolate);
            store.emplace(queues_.open_tenant(id));
            // These handles are weak, copyable tickets. Keep the originals
            // intact until insertion succeeds so rollback never uses a
            // moved-from VM or durable-store ticket.
            auto tenant=std::make_shared<Tenant>(id,isolate,*store);
            const auto [it, inserted]=tenants_.emplace(id,tenant);
            if (!inserted) throw std::logic_error("duplicate durable VM tenant");
            return Handle(it->second);
        } catch (...) {
            // No guest work or durable lease can have been admitted yet.
            // Both registries must be cleaned up, independently. If cleanup
            // fails, the host must treat the tenant as unavailable; never
            // report a successful open or reuse a stale ticket.
            bool cleanup_ok=true;
            if (store) {
                try { queues_.close_tenant(*store); }
                catch (...) { cleanup_ok=false; }
            }
            try {
                auto phase=vm_.snapshot(isolate).phase;
                if (phase==oreslang::runtime::IsolatePhase::Created ||
                    phase==oreslang::runtime::IsolatePhase::Running)
                    vm_.begin_shutdown(isolate);
                phase=vm_.snapshot(isolate).phase;
                if (phase==oreslang::runtime::IsolatePhase::Quiescing)
                    vm_.complete_shutdown(isolate);
                vm_.destroy(isolate);
            } catch (...) { cleanup_ok=false; }
            if (!cleanup_ok)
                throw std::runtime_error(
                    "durable tenant activation failed and cleanup was incomplete: "+id);
            throw;
        }
    }

    Receipt enqueue(const Handle& h, const std::string& object,
                    const std::string& lock_id, const std::string& request_id,
                    const std::string& payload) {
        std::lock_guard<std::mutex> guard(mu_);
        auto t=checked(h);
        require_running(*t);
        return queues_.enqueue(t->store,object,lock_id,request_id,payload);
    }
    std::optional<TenantRuntime::Lease> try_claim(const Handle& h,
                                                   const std::string& lock_id) {
        std::lock_guard<std::mutex> guard(mu_);
        auto t=checked(h);
        require_running(*t);
        return queues_.try_claim(t->store,lock_id);
    }
    StateSnapshot read_state(const TenantRuntime::Lease& lease) const {
        return queues_.read_state(lease);
    }
    void complete(TenantRuntime::Lease& lease, const std::string& next) {
        queues_.complete(lease,next);
    }
    void retry(TenantRuntime::Lease& lease) { queues_.retry(lease); }
    std::size_t pending(const Handle& h, const std::string& lock_id) const {
        std::lock_guard<std::mutex> guard(mu_);
        return queues_.pending(checked(h)->store,lock_id);
    }
    oreslang::runtime::IsolateSnapshot snapshot(const Handle& h) const {
        std::lock_guard<std::mutex> guard(mu_);
        return vm_.snapshot(checked(h)->vm);
    }
    void close_tenant(const Handle& h) {
        std::lock_guard<std::mutex> guard(mu_);
        auto t=checked(h);
        // Close guest admission and physically drain actor/guest pins BEFORE
        // storage retirement. A failure leaves the tenant registered and
        // non-running, so a later call can finish shutdown safely.
        auto phase=vm_.snapshot(t->vm).phase;
        if (phase==oreslang::runtime::IsolatePhase::Created ||
            phase==oreslang::runtime::IsolatePhase::Running)
            vm_.begin_shutdown(t->vm);
        phase=vm_.snapshot(t->vm).phase;
        if (phase==oreslang::runtime::IsolatePhase::Quiescing)
            vm_.complete_shutdown(t->vm); // rejects live actor/guest pins
        if (vm_.snapshot(t->vm).phase!=oreslang::runtime::IsolatePhase::Stopped)
            throw std::logic_error("durable VM isolate did not stop");
        queues_.close_tenant(t->store); // rejects live durable leases
        vm_.destroy(t->vm);
        tenants_.erase(t->id);
    }
private:
    void require_running(const Tenant& t) const {
        if (vm_.snapshot(t.vm).phase!=oreslang::runtime::IsolatePhase::Running)
            throw std::logic_error("durable VM isolate is shutting down");
    }
    std::shared_ptr<Tenant> checked(const Handle& h) const {
        auto t=h.tenant_.lock();
        if(!t) throw std::logic_error("expired LLVM durable isolate ticket");
        const auto it=tenants_.find(t->id);
        if(it==tenants_.end()||it->second!=t)
            throw std::logic_error("foreign or retired LLVM durable isolate ticket");
        return t;
    }
    mutable std::mutex mu_;
    VM vm_;
    TenantRuntime queues_;
    std::unordered_map<std::string,std::shared_ptr<Tenant>> tenants_;
};

} // namespace ores::durable
