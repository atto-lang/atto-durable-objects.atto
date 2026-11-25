#pragma once
#include "ores/durable/counter_turn.hpp"
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <mutex>
#include <thread>
#include <tuple>
#include <utility>

namespace ores::durable {
// Single trusted controller prototype. External exclusive process ownership
// and authentication are prerequisites; not a distributed lease authority.
// One bounded storage worker, FIFO for all objects; no guest carrier waits.
// The returned future is a host receipt, NOT the AttoLang susp/await ABI.
class CounterDirectory final {
public:
    using Guest = std::function<std::int64_t(std::int64_t,std::int64_t,std::int64_t)>;
    using Reply = std::optional<SyncCommit>;
    using Authorize = std::function<bool(const SyncKey&)>;
private:
    using Key = std::tuple<std::string,std::string,std::string,std::string>;
    SqliteActorSync store_;
    const Guest guest_;
    const Authorize authorize_;
    const std::size_t capacity_, active_limit_;
    std::map<Key,std::int64_t> active_; // only the worker touches this
    std::mutex mu_;
    std::condition_variable wake_;
    std::deque<std::packaged_task<void()>> queue_;
    bool closed_=false;
    std::thread worker_;
    static Key identity(const SyncKey& k) {
        return {k.tenant,k.name_space,k.actor_type,k.object_id};
    }
    static void check(const std::string& s) {
        if(s.empty() || s.size()>256 || s.find('\0')!=std::string::npos)
            throw std::invalid_argument("invalid identity component");
    }
    void validate(const SyncKey& k) const {
        check(k.tenant);check(k.name_space);check(k.actor_type);check(k.object_id);
        if(k.actor_type!="Counter" || !authorize_(k)) throw std::logic_error("unauthorized actor");
    }
    void loop() {
        for(;;) {
            std::packaged_task<void()> job;
            {
                std::unique_lock<std::mutex> lock(mu_);
                wake_.wait(lock,[&]{return closed_ || !queue_.empty();});
                if(queue_.empty() && closed_) return;
                job=std::move(queue_.front()); queue_.pop_front();
            }
            job();
        }
    }
    template<class F> auto submit(F f) -> std::future<decltype(f())> {
        using R=decltype(f());
        std::packaged_task<R()> inner(std::move(f));
        auto result=inner.get_future();
        {
            std::lock_guard<std::mutex> lock(mu_);
            if(closed_) throw std::logic_error("directory closed");
            if(queue_.size()>=capacity_) throw std::length_error("directory queue full");
            queue_.emplace_back([job=std::move(inner)]() mutable {job();});
        }
        wake_.notify_one();
        return result;
    }
public:
    CounterDirectory(const std::string& db, Guest guest, Authorize authorize,
                     std::size_t capacity=128,std::size_t active_limit=128)
        : store_(db), guest_(std::move(guest)), authorize_(std::move(authorize)),
          capacity_(capacity),active_limit_(active_limit) {
        if(!guest_ || !authorize_ || !capacity_ || !active_limit_)
            throw std::invalid_argument("invalid directory configuration");
        worker_=std::thread([this]{loop();});
    }
    CounterDirectory(const CounterDirectory&)=delete;
    CounterDirectory& operator=(const CounterDirectory&)=delete;
    ~CounterDirectory() {
        {std::lock_guard<std::mutex> lock(mu_);closed_=true;}
        wake_.notify_all();
        worker_.join(); // trusted control-plane shutdown only; drains accepted jobs
    }
    std::future<Reply> send(SyncKey k,std::string request,std::int64_t op,std::int64_t delta) {
        validate(k);check(request);
        if((op!=1 && op!=2) || (op==1 && (delta<=0 || delta>CounterTurn::maximum)) ||
           (op==2 && delta!=0)) throw std::invalid_argument("invalid command");
        return submit([this,k=std::move(k),request=std::move(request),op,delta]{
            validate(k); // authority checked again when the queued turn starts
            const auto id=identity(k);
            auto it=active_.find(id);
            if(it==active_.end()) {
                if(active_.size()>=active_limit_) throw std::length_error("activation quota exhausted");
                it=active_.emplace(id,store_.activate_next(k)).first;
            }
            try {
                auto r=CounterTurn::run(store_,k,it->second,request,op,delta,guest_);
                // No response for revoked authorization or stale ownership.
                validate(k);
                if(store_.snapshot(k).epoch!=it->second) throw std::logic_error("stale publication epoch");
                return r;
            } catch(...) {
                // Unknown outcome: discard local activation; next authorized
                // request recovers from authoritative committed receipts.
                active_.erase(id); throw;
            }
        });
    }
    // Explicit FIFO passivation barrier; never evict an executing turn.
    // Automatic idle policy is separate. No state is destroyed on disk.
    std::future<bool> passivate(SyncKey k) {
        validate(k);
        return submit([this,k=std::move(k)]{validate(k);return active_.erase(identity(k))!=0;});
    }
};
} // namespace ores::durable
