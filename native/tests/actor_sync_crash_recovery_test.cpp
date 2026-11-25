#include "ores/durable/actor_sync.hpp"
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

using namespace ores::durable;
namespace {
void check_exit(pid_t child, int expected) {
    int status=0;
    assert(waitpid(child,&status,0)==child);
    assert(WIFEXITED(status));
    assert(WEXITSTATUS(status)==expected);
}
}
int main() {
    char dir[]="/tmp/ores-sync-recovery-XXXXXX";
    assert(mkdtemp(dir)!=nullptr);
    const std::string db=std::string(dir)+"/actor.db";
    const SyncKey key{"tenant","orders","Order","42"};
    {
        SqliteActorSync initial(db);
        initial.activate(key,1);
    }

    // A process exits after the transaction committed but BEFORE the
    // application receives the commit result. The caller must retry the
    // identical operation id without making a second state transition.
    pid_t committed=fork();
    assert(committed>=0);
    if(committed==0) {
        try {
            SqliteActorSync writer(db);
            const auto result=writer.commit(key,1,0,"request-1","charge-10","order.v1","charged","receipt-1");
            if(result.duplicate || result.revision!=1) _exit(11);
            // No guest reply is sent. The process disappears after COMMIT.
            _exit(42);
        } catch(...) { _exit(12); }
    }
    check_exit(committed,42);
    {
        SqliteActorSync recovered(db);
        assert(recovered.snapshot(key).revision==1);
        assert(recovered.snapshot(key).bytes=="charged");
        const auto found=recovered.lookup_receipt(key,1,"request-1","charge-10","order.v1");
        assert(found && found->duplicate && found->revision==1 && found->reply=="receipt-1");
        assert(!recovered.lookup_receipt(key,1,"unseen","charge-10","order.v1"));
        for (const auto& mismatch : {std::pair<const char*,const char*>{"charge-11","order.v1"},
                                     {"charge-10","order.v2"}}) {
            bool denied=false;
            try { (void)recovered.lookup_receipt(key,1,"request-1",mismatch.first,mismatch.second); }
            catch(const std::logic_error&) { denied=true; }
            assert(denied);
        }
        const auto retry=recovered.commit(key,1,0,"request-1","charge-10","order.v1","duplicate","bad-receipt");
        assert(retry.duplicate && retry.revision==1 && retry.reply=="receipt-1");
        bool rejected=false;
        try { recovered.commit(key,1,1,"request-1","charge-11","order.v1","corrupt","bad"); }
        catch(const std::logic_error&) { rejected=true; }
        assert(rejected);
        const auto events=recovered.changes_after(key,0);
        assert(events.size()==1 && events[0].revision==1 && events[0].operation_id=="request-1");
    }

    // Pre-commit injected failure must roll back all three durable records.
    // The failed worker then exits, and a new process replays the command.
    pid_t aborted=fork();
    assert(aborted>=0);
    if(aborted==0) {
        try {
            SqliteActorSync writer(db);
            writer.commit(key,1,1,"request-2","credit-2","order.v1","credited","receipt-2",true);
            _exit(13);
        } catch(const std::runtime_error&) { _exit(43); }
        catch(...) { _exit(14); }
    }
    check_exit(aborted,43);
    {
        SqliteActorSync recovered(db);
        assert(recovered.snapshot(key).revision==1);
        assert(recovered.changes_after(key,0).size()==1);
        const auto result=recovered.commit(key,1,1,"request-2","credit-2","order.v1","credited","receipt-2");
        assert(!result.duplicate && result.revision==2);
        assert(recovered.changes_after(key,0).size()==2);
        recovered.activate(key,2);
        bool rejected=false;
        try { recovered.commit(key,1,2,"request-3","stale","order.v1","bad","bad"); }
        catch(const std::logic_error&) { rejected=true; }
        assert(rejected);
        // The idempotency key remains scoped to the stable logical actor,
        // not to the transient epoch of its latest activation.
        bool stale_lookup=false;
        try { (void)recovered.lookup_receipt(key,1,"request-2","credit-2","order.v1"); }
        catch(const std::logic_error&) { stale_lookup=true; }
        assert(stale_lookup);
        const auto current=recovered.lookup_receipt(key,2,"request-2","credit-2","order.v1");
        assert(current && current->reply=="receipt-2" && current->revision==2);
        const auto replay=recovered.commit(key,2,0,"request-2","credit-2","order.v1","bad","bad");
        assert(replay.duplicate && replay.revision==2 && replay.reply=="receipt-2");
    }
    // Trusted controller allocates epochs atomically without resetting state.
    {
        SqliteActorSync first(db), second(db);
        assert(first.activate_next(key)==3);
        assert(second.activate_next(key)==4);
        assert(first.snapshot(key).epoch==4);
        assert(first.snapshot(key).revision==2);
        const SyncKey fresh{"tenant","orders","Order","fresh"};
        assert(second.activate_next(fresh)==1);
        assert(first.snapshot(fresh).revision==0);
        bool denied=false;
        try { (void)first.lookup_receipt(key,3,"request-2","credit-2","order.v1"); }
        catch(const std::logic_error&) { denied=true; }
        assert(denied);
    }
    // Alarm generation survives cancellation, consumption and reopen.
    {
        SqliteActorSync store(db);
        const SyncKey alarm_key{"tenant","timers","Counter","one"};
        const auto epoch=store.activate_next(alarm_key);
        assert(store.alarm_generation(alarm_key,epoch)==0);
        assert(store.set_alarm(alarm_key,epoch,0,100)==1);
        assert(store.due_alarms(99).empty());
        auto due=store.due_alarms(100);
        assert(due.size()==1 && due[0].generation==1);
        assert(store.set_alarm(alarm_key,epoch,1,200)==2);
        assert(!store.alarm_pending(due[0],epoch,200));
        bool stale=false;
        try { store.set_alarm(alarm_key,epoch,1,std::nullopt); }
        catch(const std::logic_error&) {stale=true;}
        assert(stale);
        assert(store.set_alarm(alarm_key,epoch,2,std::nullopt)==3);
        assert(store.due_alarms(200).empty());
        assert(store.set_alarm(alarm_key,epoch,3,300)==4);
        bool reserved=false;
        try { store.commit(alarm_key,epoch,0,"@alarm:4","alarm:300","counter","1","1"); }
        catch(const std::invalid_argument&) {reserved=true;}
        assert(reserved);
        due=store.due_alarms(300);
        assert(due.size()==1);
        bool rolled=false;
        try { store.commit(alarm_key,epoch,0,"@alarm:4","alarm:300","counter","1","1",true,&due[0],300); }
        catch(const std::runtime_error&) {rolled=true;}
        assert(rolled && store.snapshot(alarm_key).revision==0);
        assert(store.alarm_pending(due[0],epoch,300));
    }
    {
        SqliteActorSync store(db);
        const SyncKey alarm_key{"tenant","timers","Counter","one"};
        auto due=store.due_alarms(300);
        assert(due.size()==1 && due[0].generation==4);
        const auto epoch=store.activate_next(alarm_key);
        bool stale=false;
        try { store.commit(alarm_key,epoch-1,0,"@alarm:4","alarm:300","counter","1","1",false,&due[0],300); }
        catch(const std::logic_error&) {stale=true;}
        assert(stale);
        const auto receipt=store.commit(alarm_key,epoch,0,"@alarm:4","alarm:300","counter","1","1",false,&due[0],300);
        assert(!receipt.duplicate && receipt.revision==1);
        assert(store.due_alarms(300).empty());
        assert(store.alarm_generation(alarm_key,epoch)==4);
        const auto replay=store.commit(alarm_key,epoch,0,"@alarm:4","alarm:300","counter","bad","bad",false,&due[0],300);
        assert(replay.duplicate && replay.reply=="1" && store.snapshot(alarm_key).bytes=="1");
        assert(store.set_alarm(alarm_key,epoch,4,400)==5);
    }
    // An admission ACK survives loss of the admitting process before execution.
    const SyncKey mailbox{"tenant","mail","Counter","mailbox"};
    {
        SqliteActorSync store(db);
        store.activate_next(mailbox);
    }
    const pid_t admitted=fork();
    assert(admitted>=0);
    if(admitted==0) {
        try {
            SqliteActorSync store(db);
            auto first=store.admit_mail(mailbox,1,"m1","1:1","counter");
            auto second=store.admit_mail(mailbox,1,"m2","1:2","counter");
            if(first.sequence<=0 || second.sequence<=first.sequence) _exit(17);
            _exit(44);
        } catch(...) {_exit(18);}
    }
    check_exit(admitted,44);
    {
        SqliteActorSync store(db);
        const auto epoch=store.activate_next(mailbox);
        const auto heads=store.pending_mail();
        assert(heads.size()==1 && heads[0].operation_id=="m1");
        const auto first=heads[0];
        assert(store.admit_mail(mailbox,epoch,"m1","1:1","counter").sequence==first.sequence);
        bool conflict=false;
        try {store.admit_mail(mailbox,epoch,"m1","1:2","counter");}
        catch(const std::logic_error&) {conflict=true;}
        assert(conflict);
        const auto second=store.admit_mail(mailbox,epoch,"m2","1:2","counter");
        SyncMail later{mailbox,second.sequence,"m2","1:2","counter"};
        assert(!store.mail_pending(later,epoch));
        for(int mode=0;mode<3;++mode) {
            bool denied=false;
            try {
                if(mode==0) store.commit(mailbox,epoch,0,"m1","1:1","counter","1","1");
                if(mode==1) store.commit(mailbox,epoch,0,"m2","1:2","counter","2","2",false,nullptr,0,&later);
                if(mode==2) store.commit(mailbox,epoch-1,0,"m1","1:1","counter","1","1",false,nullptr,0,&first);
            } catch(const std::logic_error&) {denied=true;}
            assert(denied);
        }
        bool rolled=false;
        try {store.commit(mailbox,epoch,0,"m1","1:1","counter","1","1",true,nullptr,0,&first);}
        catch(const std::runtime_error&) {rolled=true;}
        assert(rolled && store.snapshot(mailbox).revision==0 && store.mail_pending(first,epoch));
        auto result=store.commit(mailbox,epoch,0,"m1","1:1","counter","1","1",false,nullptr,0,&first);
        assert(result.revision==1 && !result.duplicate);
        auto replay=store.admit_mail(mailbox,epoch,"m1","1:1","counter");
        assert(replay.sequence==0 && replay.receipt && replay.receipt->reply=="1");
        assert(store.mail_pending(later,epoch));
    }
    {
        SqliteActorSync store(db);
        auto heads=store.pending_mail();
        assert(heads.size()==1 && heads[0].operation_id=="m2");
        const auto epoch=store.activate_next(mailbox);
        auto r=store.commit(mailbox,epoch,1,"m2","1:2","counter","3","3",false,nullptr,0,&heads[0]);
        assert(r.revision==2 && store.pending_mail().empty());
        assert(store.snapshot(mailbox).bytes=="3");
    }
    {
        SyncLimits limits;limits.inbox_entries_per_actor=1;limits.inbox_entries_per_database=1;
        SqliteActorSync store(db,limits);
        const auto epoch=store.activate_next(mailbox);
        auto first=store.admit_mail(mailbox,epoch,"q1","1:1","counter");
        assert(first.sequence>0);
        assert(store.admit_mail(mailbox,epoch,"q1","1:1","counter").sequence==first.sequence);
        bool full=false;
        try {store.admit_mail(mailbox,epoch,"q2","1:1","counter");}
        catch(const std::length_error&) {full=true;}
        assert(full);
    }
    // Retry metadata persists, preserves FIFO, and fences stale attempts.
    const auto retry_db=std::string(dir)+"/retry.db";
    const SyncKey poison{"tenant","mail","Counter","poison"};
    const SyncKey healthy{"tenant","mail","Counter","healthy"};
    SyncMail attempt{};
    {
        SyncLimits limits;limits.dedupe_entries_per_actor=2;limits.outbox_entries_per_actor=2;
        SqliteActorSync store(retry_db,limits);
        auto epoch=store.activate_next(poison);
        store.admit_mail(poison,epoch,"bad","1:1","counter");
        store.admit_mail(poison,epoch,"next","1:2","counter");
        bool reserved=false;
        try {store.commit(poison,epoch,0,"transient","1:1","counter","1","1");}
        catch(const std::length_error&) {reserved=true;}
        assert(reserved);
        auto other=store.activate_next(healthy);
        store.admit_mail(healthy,other,"ok","1:3","counter");
        attempt=store.pending_mail(64,0).front();
        assert(!store.fail_mail(attempt,epoch,0));
        bool stale=false;
        try {store.fail_mail(attempt,epoch,0);}
        catch(const std::logic_error&) {stale=true;}
        assert(stale);
        auto ready=store.pending_mail(64,99);
        assert(ready.size()==1 && ready[0].key.object_id=="healthy");
        auto done=store.commit(healthy,other,0,"ok","1:3","counter","3","3",false,nullptr,0,&ready[0]);
        assert(done.revision==1);
        assert(store.pending_mail(64,99).empty());
    }
    {
        SqliteActorSync store(retry_db);
        auto epoch=store.activate_next(poison);
        assert(store.pending_mail(64,99).empty());
        auto ready=store.pending_mail(64,100);
        assert(ready.size()==1 && ready[0].failures==1);
        assert(!store.fail_mail(ready[0],epoch,100));
        assert(store.pending_mail(64,299).empty());
        ready=store.pending_mail(64,300);
        assert(ready.size()==1 && ready[0].failures==2);
        assert(store.fail_mail(ready[0],epoch,300));
        auto dead=store.admit_mail(poison,epoch,"bad","1:1","counter");
        assert(dead.dead_letter && !dead.receipt && dead.sequence==attempt.sequence);
        assert(store.snapshot(poison).revision==0);
        bool blocked=false;
        try {store.commit(poison,epoch,0,"bad","1:1","counter","1","1");}
        catch(const std::logic_error&) {blocked=true;}
        assert(blocked);
        ready=store.pending_mail(64,300);
        assert(ready.size()==1 && ready[0].operation_id=="next");
        assert(store.commit(poison,epoch,0,"next","1:2","counter","2","2",false,nullptr,0,&ready[0]).revision==1);
        assert(store.pending_mail().empty());
    }
    std::filesystem::remove_all(dir);
}
