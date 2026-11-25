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
    std::filesystem::remove_all(dir);
}
