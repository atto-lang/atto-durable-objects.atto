// HOST_ONLY: tests directory lifecycle and scheduling, not an AttoLang guest.
#include "ores/durable/counter_directory.hpp"
#include "ores/durable/counter_dead_letter_admin.hpp"
#include <cassert>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>
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
    // Persist ACKs with dispatch disabled, restart, and drain the FIFO.
    {
        using namespace std::chrono_literals;
        const auto mail_db=std::string(dir)+"/mail.db";
        std::int64_t first_sequence=0;
        {
            CounterDirectory directory(mail_db,guest,auth);
            const auto first=directory.admit(a,"durable-1",1,1).get();
            first_sequence=first.sequence;
            assert(first_sequence>0 && !first.receipt);
            const auto second=directory.admit(a,"durable-2",1,2).get();
            assert(second.sequence>first_sequence && !second.receipt);
            assert(directory.admit(a,"durable-1",1,1).get().sequence==first_sequence);
            rejects<std::logic_error>([&]{directory.admit(a,"durable-1",1,3).get();});
            rejects<std::logic_error>([&]{(void)directory.admit(b,"denied",1,1);});
            assert(directory.admit(c,"independent",1,5).get().sequence>0);
            assert(directory.passivate(a).get()==false || !directory.is_active(a));
        }
        {
            CounterDirectory directory(mail_db,guest,auth,16,4,2,10ms,0ms,10ms);
            SqliteActorSync observer(mail_db);
            const auto until=CounterDirectory::Clock::now()+5s;
            while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<until)
                std::this_thread::sleep_for(5ms);
            assert(observer.pending_mail().empty());
            assert(observer.snapshot(a).bytes=="3" && observer.snapshot(a).revision==2);
            assert(observer.snapshot(c).bytes=="5");
            auto replay=directory.admit(a,"durable-1",1,1).get();
            assert(replay.sequence==0 && replay.receipt && replay.receipt->reply=="1");
            assert(directory.admit(a,"durable-2",1,2).get().receipt->reply=="3");
            assert(!directory.mail_fault());
            // Overflow must complete with a terminal receipt and not block
            // the later admitted read at the head of the durable FIFO.
            assert(directory.admit(a,"fill",1,999999997).get().sequence>0);
            assert(directory.admit(a,"overflow",1,1).get().sequence>0);
            assert(directory.admit(a,"after-overflow",2,0).get().sequence>0);
            const auto end=CounterDirectory::Clock::now()+5s;
            while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<end)
                std::this_thread::sleep_for(5ms);
            assert(observer.pending_mail().empty());
            assert(directory.admit(a,"overflow",1,1).get().receipt->reply=="mail_overflow");
            assert(directory.admit(a,"after-overflow",2,0).get().receipt->reply=="1000000000");
            assert(observer.snapshot(a).revision==5 && !directory.mail_fault());
        }
    }
    // A classified failing guest must not halt unrelated actors or block
    // its own later mail after the bounded retry budget is exhausted.
    {
        using namespace std::chrono_literals;
        const auto retry_db=std::string(dir)+"/retry.db";
        {
            CounterDirectory directory(retry_db,guest,auth);
            assert(directory.admit(a,"poison",1,7).get().sequence>0);
            assert(directory.admit(a,"next",1,2).get().sequence>0);
            assert(directory.admit(c,"healthy",1,5).get().sequence>0);
        }
        std::atomic<int> failures{0};
        {
            CounterDirectory directory(retry_db,[&](I n,I op,I d)->I{
                if(d==7){++failures;throw CounterDirectory::RetryableGuestFailure();}
                return op==2?n:n+d;
            },auth,16,4,2,10ms,0ms,10ms);
            SqliteActorSync observer(retry_db);
            const auto deadline=CounterDirectory::Clock::now()+5s;
            while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<deadline)
                std::this_thread::sleep_for(5ms);
            assert(observer.pending_mail().empty());
            assert(failures==3 && !directory.mail_fault());
            assert(observer.snapshot(a).bytes=="2" && observer.snapshot(a).revision==1);
            assert(observer.snapshot(c).bytes=="5");
            auto dead=directory.admit(a,"poison",1,7).get();
            assert(dead.dead_letter && !dead.receipt);
        }
        {
            CounterDirectory directory(retry_db,guest,auth);
            auto dead=directory.admit(a,"poison",1,7).get();
            assert(dead.dead_letter && !dead.receipt);
            rejects<std::logic_error>([&]{directory.admit(a,"poison",1,8).get();});
        }
    }
    // Controller death inside the host-only guest callback must count once
    // on recovery, without running that abandoned attempt again.
    {
        using namespace std::chrono_literals;
        const auto crash_db=std::string(dir)+"/controller-crash.db";
        {
            CounterDirectory directory(crash_db,guest,auth);
            assert(directory.admit(a,"crash",1,7).get().sequence>0);
            assert(directory.admit(a,"next",1,2).get().sequence>0);
            assert(directory.admit(c,"healthy",1,5).get().sequence>0);
        }
        const pid_t child=fork();
        assert(child>=0);
        if(child==0) {
            try {
                CounterDirectory directory(crash_db,[](I n,I op,I d)->I {
                    if(d==7) _exit(46);
                    return op==2?n:n+d;
                },auth,16,4,2,10ms,0ms,10ms);
                std::this_thread::sleep_for(3s);
                _exit(47);
            } catch(...) {_exit(48);}
        }
        int status=0;
        assert(waitpid(child,&status,0)==child && WIFEXITED(status) && WEXITSTATUS(status)==46);
        {
            SqliteActorSync observer(crash_db);
            const auto heads=observer.pending_mail();
            assert(!heads.empty() && heads.front().operation_id=="crash" &&
                   heads.front().failures==0 && heads.front().attempt_epoch>0);
            assert(observer.snapshot(a).revision==0);
        }
        std::atomic<int> retries{0};
        {
            CounterDirectory directory(crash_db,[&](I n,I op,I d)->I {
                if(d==7){++retries;throw CounterDirectory::RetryableGuestFailure();}
                return op==2?n:n+d;
            },auth,16,4,2,10ms,0ms,10ms);
            SqliteActorSync observer(crash_db);
            const auto deadline=CounterDirectory::Clock::now()+5s;
            while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<deadline)
                std::this_thread::sleep_for(5ms);
            assert(observer.pending_mail().empty() && retries==2 && !directory.mail_fault());
            assert(directory.admit(a,"crash",1,7).get().dead_letter);
            assert(observer.snapshot(a).revision==1 && observer.snapshot(a).bytes=="2");
            assert(observer.snapshot(c).revision==1 && observer.snapshot(c).bytes=="5");
        }
    }
    // A durable head that is still physically executing cannot hold up
    // an independent ready object, nor permit its own successor to run.
    {
        using namespace std::chrono_literals;
        const auto parallel_db=std::string(dir)+"/parallel-mail.db";
        {
            CounterDirectory directory(parallel_db,guest,auth);
            assert(directory.admit(a,"held",1,7).get().sequence>0);
            assert(directory.admit(a,"next",1,2).get().sequence>0);
            assert(directory.admit(c,"other",1,5).get().sequence>0);
        }
        std::promise<void> entered,release;
        auto started=entered.get_future();auto gate=release.get_future();
        {
            CounterDirectory directory(parallel_db,[&](I n,I op,I d)->I {
                if(d==7){entered.set_value();gate.wait();}
                return op==2?n:n+d;
            },auth,16,4,2,10ms,0ms,10ms);
            SqliteActorSync observer(parallel_db);
            const bool began=started.wait_for(3s)==std::future_status::ready;
            bool independent=false,exclusive=false;
            try {
                const auto deadline=CounterDirectory::Clock::now()+3s;
                while(CounterDirectory::Clock::now()<deadline) {
                    if(observer.snapshot(c).revision==1){independent=true;break;}
                    std::this_thread::sleep_for(5ms);
                }
                exclusive=observer.snapshot(a).revision==0;
            } catch(...) {release.set_value();throw;}
            release.set_value();
            assert(began && independent && exclusive);
            const auto end=CounterDirectory::Clock::now()+5s;
            while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<end)
                std::this_thread::sleep_for(5ms);
            assert(observer.pending_mail().empty() && !directory.mail_fault());
            assert(observer.snapshot(a).revision==2 && observer.snapshot(a).bytes=="9");
            assert(observer.snapshot(c).bytes=="5");
        }
    }
    // Single worker: an earlier backlog from many hot-tenant objects
    // must not hide the quiet tenant's head behind the whole backlog.
    {
        using namespace std::chrono_literals;
        const auto fair_db=std::string(dir)+"/fair-directory.db";
        const SyncKey quiet{"quiet","counters","Counter","one"};
        const auto allow=[](const SyncKey&){return true;};
        {
            CounterDirectory directory(fair_db,guest,allow,128,32);
            for(int i=0;i<12;++i) {
                const SyncKey hot{"hot","counters","Counter",std::to_string(i)};
                assert(directory.admit(hot,"first",1,1).get().sequence>0);
            }
            assert(directory.admit(quiet,"first",1,5).get().sequence>0);
        }
        std::vector<std::string> order;
        std::mutex order_mu;
        {
            CounterDirectory directory(fair_db,
                [&](const SyncKey& k,I n,I op,I d)->I {
                    std::lock_guard<std::mutex> lock(order_mu);
                    order.push_back(k.tenant);
                    return op==2?n:n+d;
                },allow,128,32,1,10ms,0ms,10ms);
            SqliteActorSync observer(fair_db);
            const auto end=CounterDirectory::Clock::now()+5s;
            while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<end)
                std::this_thread::sleep_for(5ms);
            assert(observer.pending_mail().empty() && !directory.mail_fault());
            assert(observer.snapshot(quiet).bytes=="5");
        }
        assert(order.size()==13 && order[0]=="hot" && order[1]=="quiet");
    }
    // Hold a real scheduler turn while an independent trusted admin
    // changes the persisted grant. External callback policy stays true:
    // only the transactional fence can reject the proposed result.
    {
        using namespace std::chrono_literals;
        const auto strict_db=std::string(dir)+"/strict-directory.db";
        const auto policy=[](std::string_view,const SyncKey&,I){return true;};
        const auto authorize=[](const SyncKey&){return true;};
        {
            CounterDirectory directory(strict_db,CounterDirectory::KeyedGuest(
                [](const SyncKey&,I n,I op,I d)->I{return op==2?n:n+d;}),
                authorize,16,4,1,10ms,0ms,0ms,"",true,policy,true);
            SqliteActorSync admin(strict_db);
            assert(admin.set_principal_policy(a,"alice",1,0,true)==1);
            assert(admin.set_principal_policy(a,"bob",1,0,true)==1);
            assert(directory.admit_bound(a,"@principal:alice",1,7,"alice",[]{}).get().sequence>0);
            assert(directory.admit_bound(a,"@principal:bob",1,2,"bob",[]{}).get().sequence>0);
        }
        std::promise<void> entered,release;
        auto ready=entered.get_future();auto gate=release.get_future();
        {
            CounterDirectory directory(strict_db,CounterDirectory::KeyedGuest(
                [&](const SyncKey&,I n,I op,I d)->I{
                    if(d==7){entered.set_value();gate.wait();}
                    return op==2?n:n+d;
                }),authorize,16,4,1,10ms,0ms,10ms,"",true,policy,true);
            SqliteActorSync admin(strict_db);
            const bool began=ready.wait_for(3s)==std::future_status::ready;
            try {
                assert(admin.set_principal_policy(a,"alice",1,1,false)==2);
                assert(admin.set_principal_policy(a,"alice",1,2,true)==3);
            } catch(...) {release.set_value();throw;}
            release.set_value();assert(began);
            const auto deadline=CounterDirectory::Clock::now()+5s;
            while(!admin.pending_mail().empty() && CounterDirectory::Clock::now()<deadline)
                std::this_thread::sleep_for(5ms);
            assert(admin.pending_mail().empty() && !directory.mail_fault());
            assert(admin.snapshot(a).bytes=="2" && admin.snapshot(a).revision==1);
            auto terminal=directory.admit_bound(a,"@principal:alice",1,7,"alice",[]{}).get();
            assert(terminal.authorization_denied && !terminal.receipt);
            rejects<std::logic_error>([&]{(void)directory.send_authorized(a,"raw",1,1,[]{});});
        }
    }
    // Operator authority is independent of the original principal grant.
    // A redriven exhausted message must execute only under the new identity.
    {
        using namespace std::chrono_literals;
        const auto redrive_db=std::string(dir)+"/admin-redrive.db";
        const auto policy=[](std::string_view,const SyncKey&,I){return true;};
        const auto authorize=[](const SyncKey&){return true;};
        std::int64_t source=0;
        {
            CounterDirectory directory(redrive_db,CounterDirectory::KeyedGuest(
                [](const SyncKey&,I,I,I)->I{throw CounterDirectory::RetryableGuestFailure();}),
                authorize,16,4,1,10ms,0ms,10ms,"",true,policy,true);
            SqliteActorSync admin(redrive_db);
            admin.set_principal_policy(a,"alice",1,0,true);
            source=directory.admit_bound(a,"@principal:failed",1,2,"alice",[]{}).get().sequence;
            const auto until=CounterDirectory::Clock::now()+5s;
            while(!admin.pending_mail().empty() && CounterDirectory::Clock::now()<until)
                std::this_thread::sleep_for(5ms);
            assert(admin.pending_mail().empty() && !directory.mail_fault());
            assert(admin.dead_letters(a).size()==1);
        }
        {
            CounterDirectory directory(redrive_db,CounterDirectory::KeyedGuest(
                [](const SyncKey&,I n,I op,I d)->I{return op==2?n:n+d;}),
                authorize,16,4,1,10ms,0ms,10ms,"",true,policy,true);
            SqliteActorSync store(redrive_db);
            CounterDeadLetterAdmin admin(store,[&](std::string_view who,const SyncKey& k,CounterDeadLetterAdmin::Action action){
                return k.tenant==a.tenant && (who=="operator" ||
                    (who=="reader" && action==CounterDeadLetterAdmin::Action::inspect));
            });
            assert(admin.inspect("reader",a).size()==1);
            rejects<std::logic_error>([&]{admin.inspect("stranger",a);});
            rejects<std::logic_error>([&]{admin.redrive("reader",a,source);});
            const auto replacement=admin.redrive("operator",a,source);
            assert(replacement.new_sequence>source);
            const auto until=CounterDirectory::Clock::now()+5s;
            while(!store.pending_mail().empty() && CounterDirectory::Clock::now()<until)
                std::this_thread::sleep_for(5ms);
            assert(store.pending_mail().empty() && !directory.mail_fault());
            assert(store.snapshot(a).revision==1 && store.snapshot(a).bytes=="2");
            assert(admin.redrive("operator",a,source).duplicate);
            assert(admin.inspect("reader",a)[0].redrive_sequence==replacement.new_sequence);
            rejects<std::logic_error>([&]{admin.archive("reader",a,source);});
            assert(!admin.archive("operator",a,source).duplicate);
            assert(admin.archive("operator",a,source).duplicate);
            const auto archived=admin.inspect("reader",a);
            assert(archived.size()==1 && archived[0].archived &&
                   archived[0].redrive_sequence==replacement.new_sequence);
            assert(admin.redrive("operator",a,source).duplicate);
            assert(directory.admit_bound(a,"@principal:failed",1,2,"alice",[]{}).get().dead_letter);
            assert(store.snapshot(a).revision==1);
        }
    }
    // A revoke/regrant during a physically executing alarm cannot
    // commit state even when the external policy callback remains true.
    {
        using namespace std::chrono_literals;
        const auto db2=std::string(dir)+"/strict-alarm-race.db";
        const auto scope=[](const SyncKey&){return true;};
        const auto policy=[](std::string_view,const SyncKey&,I){return true;};
        {
            CounterDirectory directory(db2,CounterDirectory::KeyedGuest(
                [](const SyncKey&,I n,I,I d)->I{return n+d;}),
                scope,16,4,1,10ms,0ms,0ms,"",true,policy,true);
            SqliteActorSync admin(db2);
            admin.set_principal_policy(a,"alice",1,0,true);
            admin.set_principal_policy(c,"bob",1,0,true);
            rejects<std::logic_error>([&]{
                (void)directory.schedule_principal_alarm(a,0,0,"alice","operator",
                    []{throw std::logic_error("operator denied");});
            });
            assert(directory.schedule_principal_alarm(a,0,0,"alice","operator",[]{}).get()==1);
            assert(directory.schedule_principal_alarm(c,0,1,"bob","operator",[]{}).get()==1);
        }
        std::promise<void> entered,release;
        auto ready=entered.get_future();auto gate=release.get_future();
        {
            CounterDirectory directory(db2,CounterDirectory::KeyedGuest(
                [&](const SyncKey& k,I n,I,I d)->I{
                    if(k.object_id==a.object_id){entered.set_value();gate.wait();}
                    return n+d;
                }),scope,16,4,1,10ms,10ms,0ms,"",true,policy,true);
            SqliteActorSync admin(db2);
            const bool began=ready.wait_for(3s)==std::future_status::ready;
            try {
                admin.set_principal_policy(a,"alice",1,1,false);
                admin.set_principal_policy(a,"alice",1,2,true);
            } catch(...) {release.set_value();throw;}
            release.set_value();assert(began);
            const auto until=CounterDirectory::Clock::now()+5s;
            while(!admin.due_alarms(1).empty() && CounterDirectory::Clock::now()<until)
                std::this_thread::sleep_for(5ms);
            assert(admin.due_alarms(1).empty() && !directory.alarm_fault());
            assert(admin.snapshot(a).revision==0 && admin.snapshot(c).bytes=="1");
            assert(admin.principal_alarm_authority(a)->status==SyncAlarmStatus::denied);
            assert(admin.principal_alarm_authority(c)->status==SyncAlarmStatus::fired);
        }
    }
    std::filesystem::remove_all(dir);
}
