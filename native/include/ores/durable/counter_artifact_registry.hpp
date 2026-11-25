#pragma once
// Trusted deployment allowlist for the scalar counter profile, NOT network
// principal authentication or a signed artifact distribution protocol.
#include "ores/durable/counter_process_guest.hpp"

namespace ores::durable {
class CounterArtifactRegistry final {
public:
    struct Binding {
        std::string tenant, name_space, actor_type;
        std::string artifact, sha256;
    };
private:
    using Scope=std::tuple<std::string,std::string,std::string>;
    const std::string executable_;
    const std::chrono::milliseconds budget_;
    std::map<Scope,CounterProcessGuest> admitted_;
    static bool label(const std::string& s) {
        return !s.empty() && s.size()<=256 && s.find('\0')==std::string::npos;
    }
public:
    CounterArtifactRegistry(std::string executable,std::chrono::milliseconds budget,
                            const std::vector<Binding>& bindings)
        :executable_(std::move(executable)),budget_(budget) {
        if(bindings.empty() || bindings.size()>4096)
            throw std::invalid_argument("invalid artifact registry size");
        for(const auto& b:bindings) {
            if(!label(b.tenant) || !label(b.name_space) || b.actor_type!="Counter")
                throw std::invalid_argument("invalid artifact scope");
            auto runner=CounterProcessGuest::fuel_worker(executable_,b.artifact,b.sha256,budget_);
            if(!admitted_.emplace(Scope{b.tenant,b.name_space,b.actor_type},std::move(runner)).second)
                throw std::invalid_argument("duplicate artifact scope");
        }
    }
    bool contains(const SyncKey& key) const {
        return admitted_.find(Scope{key.tenant,key.name_space,key.actor_type})!=admitted_.end();
    }
    std::int64_t operator()(const SyncKey& key,std::int64_t n,std::int64_t op,std::int64_t d) const {
        // No path, digest or worker selector is accepted from the request.
        auto it=admitted_.find(Scope{key.tenant,key.name_space,key.actor_type});
        if(it==admitted_.end()) throw std::logic_error("no admitted artifact for actor scope");
        if(CounterDirectory::current_guest_guard_fd()<3)
            throw std::logic_error("artifact execution requires a controller guard");
        return it->second(n,op,d);
    }
};
} // namespace ores::durable
