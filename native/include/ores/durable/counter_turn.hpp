#pragma once
#include "ores/durable/actor_sync.hpp" // prerequisite: durable objects PR #9
#include <charconv>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace ores::durable {
// Trusted host integration seam only. Caller MUST authenticate key, acquire
// its exclusive non-reentrant turn permit and run on an isolated IO worker.
// The permit spans guest invocation and commit; no untrusted native callback.
// Epoch is an externally granted fence, not an authorization credential.
class CounterTurn final {
public:
    static constexpr std::int64_t maximum = 1000000000;
    static constexpr std::string_view schema = "atto.counter.scalar.v1";

    // Fixed local prototype encoding, not the public snapshot or RPC format.
    static std::int64_t decode(std::string_view bytes) {
        if (bytes.empty() || bytes.size() > 10 ||
            (bytes.size() > 1 && bytes.front() == '0') ||
            bytes.front() < '0' || bytes.front() > '9')
            throw std::logic_error("noncanonical counter snapshot");
        std::int64_t n = 0;
        const auto p = std::from_chars(bytes.data(), bytes.data()+bytes.size(), n);
        if (p.ec != std::errc{} || p.ptr != bytes.data()+bytes.size() || n < 0 || n > maximum)
            throw std::logic_error("invalid counter snapshot");
        return n;
    }

    // Alarm v0: deterministic increment by one using the same admitted
    // guest transition. A terminal domain overflow is consumed with an
    // explicit rejected receipt, not retried forever. No arbitrary effects.
    template<class Guest>
    static std::optional<SyncCommit> run_alarm(SqliteActorSync& store,const SyncAlarm& alarm,
                                               std::int64_t epoch,std::int64_t now_ms,Guest&& guest) {
        if(alarm.key.actor_type!="Counter") throw std::invalid_argument("unexpected alarm actor type");
        if(!store.alarm_pending(alarm,epoch,now_ms)) return std::nullopt;
        const auto snapshot=store.snapshot(alarm.key);
        if(snapshot.epoch!=epoch) throw std::logic_error("stale alarm owner");
        std::int64_t committed=0;
        if(snapshot.revision==0) {
            if(!snapshot.schema.empty() || !snapshot.bytes.empty())
                throw std::logic_error("unexpected uninitialized snapshot");
        } else {
            if(snapshot.schema!=schema) throw std::logic_error("counter schema migration required");
            committed=decode(snapshot.bytes);
        }
        const auto next=guest(committed,1,1);
        const bool overflow=committed==maximum;
        if((overflow && next!=-1) || (!overflow && next!=committed+1))
            throw std::logic_error("invalid alarm guest transition");
        const auto state=std::to_string(overflow?committed:next);
        const std::string reply=overflow?"alarm_overflow":state;
        return store.commit(alarm.key,epoch,snapshot.revision,
            SqliteActorSync::alarm_operation_id(alarm.generation),
            "alarm:"+std::to_string(alarm.due_ms),schema,state,reply,false,&alarm,now_ms);
    }

    // Guest runner receives a copied scalar and produces a proposed value.
    // It cannot commit, emit receipts or see SQLite handles through this API.
    // -1 = application rejection, not a committed result. Transport errors
    // and ambiguous commit outcomes propagate; the caller must retry same ID.
    template<class Guest>
    static std::optional<SyncCommit> run(SqliteActorSync& store, const SyncKey& key,
                                         std::int64_t epoch, std::string_view request_id,
                                         std::int64_t op, std::int64_t delta, Guest&& guest,
                                         bool inject_precommit_failure = false) {
        if (key.actor_type != "Counter")
            throw std::invalid_argument("unexpected actor type");
        if (op != 1 && op != 2) throw std::invalid_argument("invalid counter operation");
        if ((op == 1 && (delta <= 0 || delta > maximum)) || (op == 2 && delta != 0))
            throw std::invalid_argument("invalid counter delta");
        const auto command = std::to_string(op) + ":" + std::to_string(delta);
        if (auto prior = store.lookup_receipt(key, epoch, request_id, command, schema)) {
            (void)decode(prior->reply);
            return prior;
        }
        const auto snapshot = store.snapshot(key);
        if (snapshot.epoch != epoch) throw std::logic_error("stale counter owner");
        std::int64_t committed = 0;
        if (snapshot.revision == 0) {
            if (!snapshot.schema.empty() || !snapshot.bytes.empty())
                throw std::logic_error("unexpected uninitialized snapshot");
        } else {
            if (snapshot.schema != schema)
                throw std::logic_error("counter schema migration required");
            committed = decode(snapshot.bytes);
        }
        const std::int64_t next = guest(committed, op, delta);
        if (next == -1) {
            if (op == 1 && delta > maximum - committed) return std::nullopt;
            throw std::logic_error("guest rejected a valid counter command");
        }
        if (next < 0 || next > maximum || (op == 2 && next != committed) ||
            (op == 1 && (delta > maximum - committed || next != committed + delta)))
            throw std::logic_error("invalid guest transition");
        const auto proposed = std::to_string(next);
        // CAS and immutable receipt check are repeated inside SQLite commit.
        // Return only the store's committed reply, never the speculative one.
        auto receipt = store.commit(key, epoch, snapshot.revision, request_id,
                                    command, schema, proposed, proposed, inject_precommit_failure);
        (void)decode(receipt.reply);
        return receipt;
    }
};
} // namespace ores::durable
