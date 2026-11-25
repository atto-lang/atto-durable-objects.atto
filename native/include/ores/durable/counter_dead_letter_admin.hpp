#pragma once
#include "ores/durable/actor_sync.hpp"
#include <functional>

namespace ores::durable {
// Trusted host adapter: upstream MUST authenticate operator identity.
// Policy is separate from actor grants and must be thread-safe.
// This is not a network endpoint or transactional operator revocation.
class CounterDeadLetterAdmin final {
public:
    enum class Action { inspect, redrive, archive };
    using Policy=std::function<bool(std::string_view,const SyncKey&,Action)>;
private:
    SqliteActorSync& store_;
    const Policy policy_;
    void require(std::string_view operator_id,const SyncKey& key,Action action) const {
        if(operator_id.empty() || operator_id.size()>256 ||
           operator_id.find('\0')!=std::string_view::npos ||
           !policy_(operator_id,key,action))
            throw std::logic_error("dead-letter operator unauthorized");
    }
public:
    CounterDeadLetterAdmin(SqliteActorSync& store,Policy policy)
        :store_(store),policy_(std::move(policy)) {
        if(!policy_) throw std::invalid_argument("missing operator policy");
    }
    std::vector<SyncDeadLetter> inspect(std::string_view operator_id,const SyncKey& key,
                                         std::int64_t after=0,std::size_t limit=64) {
        require(operator_id,key,Action::inspect);
        auto page=store_.dead_letters(key,after,limit);
        require(operator_id,key,Action::inspect);
        return page;
    }
    SyncArchive archive(std::string_view operator_id,const SyncKey& key,std::int64_t source) {
        require(operator_id,key,Action::archive);
        const auto epoch=store_.snapshot(key).epoch;
        require(operator_id,key,Action::archive);
        auto result=store_.archive_dead_letter(key,epoch,source,operator_id);
        require(operator_id,key,Action::archive);
        return result;
    }
    SyncRedrive redrive(std::string_view operator_id,const SyncKey& key,std::int64_t source) {
        require(operator_id,key,Action::redrive);
        // This read grants no new ownership. If activation races it, the
        // write transaction rejects the stale epoch. No guest is launched.
        const auto epoch=store_.snapshot(key).epoch;
        require(operator_id,key,Action::redrive);
        auto result=store_.redrive_dead_letter(key,epoch,source,operator_id);
        // Revocation here can suppress the reply but cannot undo admission.
        require(operator_id,key,Action::redrive);
        return result;
    }
};
} // namespace ores::durable
