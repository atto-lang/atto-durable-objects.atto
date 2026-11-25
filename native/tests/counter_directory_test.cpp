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
        // The same canonical store cannot be owned by two cooperating
        // directory instances, even through a relative path alias.
        rejects<std::logic_error>([&]{
            CounterDirectory competing(std::string(dir)+"/./actor.db",guest,auth);
        });

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
    // Two workers: distinct object B progresses while A is held; A's
    // queued successor and passivation cannot overtake the live turn.
    {
        using namespace std::chrono_literals;
        std::promise<void> entered,release;
        auto ready=entered.get_future();auto gate=release.get_future();
        std::atomic<int> count{0};
        CounterDirectory directory(db,[&](I n,I,I d)->I{
            if(n==104) {++count;entered.set_value();gate.get();}
            return n+d;
        },auth,16,4,2,20ms);
        auto first=directory.send(a,"parallel-held",1,1);
        const bool started=ready.wait_for(3s)==std::future_status::ready;
        auto next=directory.send(a,"parallel-next",1,1);
        auto eviction=directory.passivate(a);
        auto independent=directory.send(c,"parallel-other",1,1);
        const bool progressed=independent.wait_for(3s)==std::future_status::ready;
        const bool held=next.wait_for(80ms)==std::future_status::timeout &&
                        eviction.wait_for(0ms)==std::future_status::timeout;
        release.set_value();
        assert(started && progressed && held);
        assert(first.get()->reply=="105");
        assert(next.get()->reply=="106");
        assert(eviction.get());
        assert(independent.get()->reply=="2");
        assert(count==1);
        const auto before=directory.send(a,"idle-before",1,1).get();
        assert(before && before->reply=="107");
        SqliteActorSync observer(db);
        const auto epoch=observer.snapshot(a).epoch;
        // Wait for observed quiescent idle eviction, not a guessed sleep.
        const auto deadline=CounterDirectory::Clock::now()+3s;
        while(directory.is_active(a) && CounterDirectory::Clock::now()<deadline)
            std::this_thread::sleep_for(5ms);
        assert(!directory.is_active(a));
        auto after=directory.send(a,"idle-after",1,1).get();
        assert(after && after->reply=="108");
        assert(observer.snapshot(a).epoch>epoch);
    }
    // Durable alarm recovery: schedule with poller disabled, close the
    // controller, then reopen and automatically activate the due object.
    {
        using namespace std::chrono_literals;
        const auto alarm_db=std::string(dir)+"/alarms.db";
        {
            CounterDirectory directory(alarm_db,guest,auth);
            assert(directory.set_alarm(a,0,0).get()==1);
            assert(directory.set_alarm(c,0,0).get()==1);
            assert(directory.set_alarm(c,1,std::nullopt).get()==2);
            assert(directory.passivate(a).get());
            assert(!directory.is_active(a));
            rejects<std::invalid_argument>([&]{(void)directory.send(a,"@alarm:1",1,1);});
        }
        {
            CounterDirectory directory(alarm_db,guest,auth,16,4,2,10ms,10ms);
            SqliteActorSync observer(alarm_db);
            const auto deadline=CounterDirectory::Clock::now()+5s;
            while(observer.snapshot(a).revision==0 && CounterDirectory::Clock::now()<deadline)
                std::this_thread::sleep_for(5ms);
            assert(observer.snapshot(a).bytes=="1");
            assert(observer.snapshot(a).epoch>=2);
            assert(observer.due_alarms(0).empty());
            assert(observer.snapshot(c).revision==0);
            assert(!directory.alarm_fault());
            // A full counter produces a terminal rejected alarm receipt,
            // not a repeated mutation or an infinite poison-message loop.
            assert(directory.send(a,"fill",1,999999999).get()->reply=="1000000000");
            assert(directory.alarm_generation(a).get()==1);
            assert(directory.set_alarm(a,1,0).get()==2);
            const auto until=CounterDirectory::Clock::now()+5s;
            while(!observer.due_alarms(0).empty() && CounterDirectory::Clock::now()<until)
                std::this_thread::sleep_for(5ms);
            assert(observer.due_alarms(0).empty());
            const auto snap=observer.snapshot(a);
            assert(snap.revision==3 && snap.bytes=="1000000000");
            const auto receipt=observer.lookup_receipt(a,snap.epoch,"@alarm:2","alarm:0",CounterTurn::schema);
            assert(receipt && receipt->reply=="alarm_overflow");
            assert(!directory.alarm_fault());
        }
        {
            // Another restart cannot execute an already consumed alarm.
            CounterDirectory directory(alarm_db,guest,auth,16,4,2,10ms,10ms);
            SqliteActorSync observer(alarm_db);
            assert(observer.due_alarms(0).empty());
            assert(directory.send(a,"verify-read",2,0).get()->reply=="1000000000");
            assert(observer.snapshot(a).revision==4);
        }
    }
    std::filesystem::remove_all(dir);
}
