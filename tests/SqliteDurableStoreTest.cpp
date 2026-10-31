#include "SqliteDurableStore.hpp"
#include <cassert>
#include <cstdio>
#include <atomic>
#include <thread>
#include <stdexcept>
#include <string>
#include <unistd.h>

using namespace oreslang::durable;
template<class E, class F> static void rejects(F&& f) {
    bool hit = false; try {f();} catch(const E&) {hit=true;} assert(hit);
}
int main() {
    char path[]="/tmp/ores_durable_XXXXXX";
    const int fd = mkstemp(path);
    assert(fd >= 0);
    close(fd);
    const std::string file(path);
    const ObjectKey a{"tenant-A", "payments", "counter"};
    const ObjectKey b{"tenant-B", "payments", "counter"};
    {
        SqliteDurableStore s(file);
        s.activate(a, 1);
        s.activate(b, 1);
        auto first=s.apply(a, 1, "op1", "balance", "100", "credited", Alarm{"reminder", 1000});
        assert(!first.deduplicated && first.reply=="credited");
        auto dup=s.apply(a, 1, "op1", "balance", "100", "should-not-write", Alarm{"reminder", 1000});
        assert(dup.deduplicated && dup.reply=="credited");
        rejects<std::logic_error>([&]{s.apply(a,1,"op1","balance","999","ignored",Alarm{"reminder",1000});});
        rejects<std::logic_error>([&]{s.apply(a,1,"op1","balance","100","ignored",Alarm{"reminder",9000});});
        rejects<std::logic_error>([&]{s.apply(a,1,"op1","other","100","ignored",Alarm{"reminder",1000});});
        rejects<std::logic_error>([&]{s.apply(a,1,"op1","balance","100","ignored");});
        assert(s.get(a, "balance")=="100");
        assert(s.alarm_time(a, "reminder")==1000);
        assert(!s.get(b, "balance"));
        rejects<std::runtime_error>([&]{s.apply(a,1,"op2","balance","200","x",{},true);});
        assert(s.get(a,"balance")=="100");
        auto applied=s.apply(a,1,"op2","balance","200","x");
        assert(!applied.deduplicated && s.get(a,"balance")=="200");
        rejects<std::logic_error>([&]{s.activate(a,1);});
        s.activate(a,2);
        rejects<std::logic_error>([&]{s.apply(a,1,"op3","balance","300","x");});
        s.apply(a,2,"op3","balance","300","done");
        s.apply(b,1,"op1","balance","800","done-b");
        std::atomic<int> repeats{0};
        std::thread first_writer([&]{if(s.apply(b,1,"concurrent","balance","801","ok").deduplicated) ++repeats;});
        std::thread second_writer([&]{if(s.apply(b,1,"concurrent","balance","801","ok").deduplicated) ++repeats;});
        first_writer.join(); second_writer.join();
        assert(repeats==1);
        assert(s.get(b,"balance")=="801");
        rejects<std::invalid_argument>([&]{s.apply(a,2,"","balance","x","x");});
        rejects<std::invalid_argument>([&]{s.apply(a,2,"bad\noperation","balance","x","x");});
    }
    {
        SqliteDurableStore reopened(file);
        assert(reopened.get(a,"balance")=="300");
        assert(reopened.get(b,"balance")=="801");
        assert(reopened.alarm_time(a,"reminder")==1000);
        auto replay=reopened.apply(a,2,"op3","balance","300","no");
        assert(replay.deduplicated && replay.reply=="done");
        assert(reopened.get(a,"balance")=="300");
        rejects<std::logic_error>([&]{reopened.apply(a,2,"op3","balance","400","no");});
        rejects<std::logic_error>([&]{reopened.apply(b,2,"x","balance","1","1");});
    }
    std::remove((file+"-wal").c_str());
    std::remove((file+"-shm").c_str());
    std::remove(file.c_str());
}
