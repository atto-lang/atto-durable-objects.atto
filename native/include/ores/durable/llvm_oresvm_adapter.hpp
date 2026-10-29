#pragma once

// Optional bridge compiled only against the *original* LLVM runtime headers
// from oreslang-source.llvm PR #1158 or its compatible successor.
// Do not mirror those private headers into this repository.
#include "ores/durable/tenant_runtime.hpp"
#include "oreslang/runtime/OresVM.hpp"

#include <memory>
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
        vm_.start(isolate);
        try {
            auto store=queues_.open_tenant(id);
            auto tenant=std::make_shared<Tenant>(id,std::move(isolate),std::move(store));
            tenants_.emplace(id,tenant);
            return Handle(tenant);
        } catch (...) {
            vm_.begin_shutdown(isolate);
            vm_.complete_shutdown(isolate);
            vm_.destroy(isolate);
            throw;
        }
    }

    Receipt enqueue(const Handle& h, const std::string& object,
                    const std::string& lock_id, const std::string& request_id,
                    const std::string& payload) {
        std::lock_guard<std::mutex> guard(mu_);
        auto t=checked(h);
        return queues_.enqueue(t->store,object,lock_id,request_id,payload);
    }
    std::optional<TenantRuntime::Lease> try_claim(const Handle& h,
                                                   const std::string& lock_id) {
        std::lock_guard<std::mutex> guard(mu_);
        return queues_.try_claim(checked(h)->store,lock_id);
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
        queues_.close_tenant(t->store); // fails if any active durable lease
        vm_.begin_shutdown(t->vm);
        vm_.complete_shutdown(t->vm); // fails on actor/guest execution pins
        vm_.destroy(t->vm);
        tenants_.erase(t->id);
    }
private:
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
