// Real compiled guest integration, including an intentionally looping turn.
#include "ores/durable/counter_process_guest.hpp"
#include <cassert>
#include <atomic>
#include <filesystem>
#include <cstdlib>
using namespace ores::durable;
using namespace std::chrono_literals;
int main(int argc,char** argv) {
    assert(argc==5 || argc==4);
    const bool fuel=argc==4;
    auto make_runner=[&](std::string digest){
        if(fuel) return CounterProcessGuest::fuel_worker(argv[1],argv[2],std::move(digest),5s);
        return CounterProcessGuest(argv[1],argv[2],argv[3],std::move(digest),1500ms);
    };
    auto runner=make_runner(argv[argc-1]);
    assert(runner(0,1,2)==2);
    bool stopped=false;
    const auto start=CounterDirectory::Clock::now();
    try {(void)runner(0,1,7);}
    catch(const CounterDirectory::RetryableGuestFailure& e) {
        stopped=true;
        assert(e.reason==(fuel?CounterDirectory::FailureReason::fuel:CounterDirectory::FailureReason::deadline));
    }
    assert(stopped);
    if(!fuel) assert(CounterDirectory::Clock::now()-start>=1500ms);
    auto wrong=make_runner(std::string(64,'0'));
    bool invalid=false;
    try {(void)wrong(0,1,2);}
    catch(const CounterDirectory::RetryableGuestFailure&) {assert(false);}
    catch(const std::runtime_error&) {invalid=true;}
    assert(invalid);
    int status=0;
    assert(::waitpid(-1,&status,WNOHANG)==-1 && errno==ECHILD);
    char tmp[]="/tmp/atto-process-mail-XXXXXX";
    assert(mkdtemp(tmp));
    const auto db=std::string(tmp)+"/mail.db";
    const SyncKey a{"tenant","counters","Counter","poison"}, b{"tenant","counters","Counter","healthy"};
    const auto auth=[](const SyncKey& k){return k.tenant=="tenant";};
    std::atomic<int> timeouts{0};
    {
        CounterDirectory directory(db,runner,auth);
        assert(directory.admit(a,"hang",1,7).get().sequence>0);
        assert(directory.admit(a,"after",1,2).get().sequence>0);
        assert(directory.admit(b,"healthy",1,5).get().sequence>0);
    }
    {
        CounterDirectory directory(db,[&](auto n,auto op,auto d){
            try {return runner(n,op,d);}
            catch(const CounterDirectory::RetryableGuestFailure& e) {
                assert(e.reason==(fuel?CounterDirectory::FailureReason::fuel:CounterDirectory::FailureReason::deadline));
                ++timeouts;throw;
            }
        },auth,16,4,2,10ms,0ms,10ms);
        SqliteActorSync observer(db);
        const auto deadline=CounterDirectory::Clock::now()+25s;
        while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<deadline)
            std::this_thread::sleep_for(10ms);
        assert(observer.pending_mail().empty());
        assert(timeouts==3 && !directory.mail_fault());
        assert(observer.snapshot(a).bytes=="2" && observer.snapshot(a).revision==1);
        assert(observer.snapshot(b).bytes=="5" && observer.snapshot(b).revision==1);
        const auto dead=directory.admit(a,"hang",1,7).get();
        assert(dead.dead_letter && !dead.receipt);
        assert(directory.admit(a,"after",1,2).get().receipt->reply=="2");
    }
    assert(::waitpid(-1,&status,WNOHANG)==-1 && errno==ECHILD);
    std::filesystem::remove_all(tmp);
    return 0;
}
