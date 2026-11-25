// HOST_ONLY: tests directory lifecycle and scheduling, not an AttoLang guest.
#include "ores/durable/counter_directory.hpp"
#include <cassert>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <vector>
#include <unistd.h>
using namespace ores::durable;
using I=std::int64_t;
template<class E,class F> void rejects(F f) {
    bool failed=false;try{f();}catch(const E&){failed=true;}assert(failed);
}
int main() {
    char dir[]="/tmp/atto-directory-XXXXXX";assert(mkdtemp(dir)!=nullptr);
    const auto db=std::string(dir)+"/actor.db";
    const SyncKey a{"tenant","counters","Counter","same"};
    const SyncKey b{"other","counters","Counter","same"};
    const SyncKey c{"tenant","counters","Counter","second"};
    std::atomic<int> running{0},calls{0};
    std::atomic<bool> allowed{true};
    const auto auth=[&](const SyncKey& k){return allowed && k.tenant=="tenant";};
    const auto guest=[&](I n,I op,I d)->I{
        assert(running.fetch_add(1)==0);++calls;
        const auto result=op==2?n:d>CounterTurn::maximum-n?-1:n+d;
        assert(running.fetch_sub(1)==1);return result;
    };
    {
        CounterDirectory directory(db,guest,auth,128,1);
        rejects<std::logic_error>([&]{(void)directory.send(b,"denied",1,1);});
        std::vector<std::future<CounterDirectory::Reply>> futures;
        for(int i=0;i<100;++i) futures.push_back(directory.send(a,"inc-"+std::to_string(i),1,1));
        for(int i=0;i<100;++i) {
            auto r=futures[i].get();assert(r && r->revision==i+1 && r->reply==std::to_string(i+1));
        }
        auto replay=directory.send(a,"inc-0",1,1).get();
        assert(replay && replay->duplicate && replay->reply=="1" && calls==100);
        rejects<std::length_error>([&]{directory.send(c,"quota",1,1).get();});
        assert(directory.passivate(a).get());
        assert(!directory.passivate(a).get());
        auto second=directory.send(c,"new",1,1).get();assert(second && second->reply=="1");
        assert(directory.passivate(c).get());
        auto next=directory.send(a,"resume",1,1).get();assert(next && next->reply=="101");
        allowed=false;
        rejects<std::logic_error>([&]{(void)directory.send(a,"revoked",1,1);});
        allowed=true;
    }
    {
        CounterDirectory directory(db,guest,auth,1,1);
        auto r=directory.send(a,"resume",1,1).get();assert(r && r->duplicate && r->reply=="101");
        // A fresh controller allocates the next persisted epoch.
        assert(directory.send(a,"last",1,1).get()->reply=="102");
    }
    {
        SqliteActorSync store(db);assert(store.snapshot(a).epoch==3);
        assert(store.snapshot(a).bytes=="102");assert(store.snapshot(c).bytes=="1");
    }
    // Deterministically hold the worker, fill its bounded queue, and reject
    // a third turn without admitting it. The queued turn must run next.
    {
        std::promise<void> entered,release;
        auto ready=entered.get_future();auto gate=release.get_future();
        int count=0;
        CounterDirectory directory(db,[&](I n,I,I d)->I{
            if(++count==1){entered.set_value();gate.get();}return n+d;
        },auth,1,1);
        auto first=directory.send(a,"held",1,1);ready.get();
        auto second=directory.send(a,"queued",1,1);
        bool full=false;
        try{(void)directory.send(a,"too-many",1,1);}catch(const std::length_error&){full=true;}
        release.set_value();
        assert(full && first.get()->reply=="103" && second.get()->reply=="104");
    }
    std::filesystem::remove_all(dir);
}
