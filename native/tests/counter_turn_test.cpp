// HOST_ONLY integration seam test. The callback below is a test double,
// not compiled AttoLang. Build after storage PR #9 is integrated.
#include "ores/durable/counter_turn.hpp"
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

using namespace ores::durable;
using I = std::int64_t;
template<class F> void rejects(F f) {
    bool denied=false;
    try { f(); } catch(const std::logic_error&) { denied=true; }
    assert(denied);
}
int main() {
    char dir[]="/tmp/atto-counter-turn-XXXXXX";
    assert(mkdtemp(dir)!=nullptr);
    const std::string db=std::string(dir)+"/counter.db";
    const SyncKey key{"a","counters","Counter","same"};
    const SyncKey other{"b","counters","Counter","same"};
    const auto transition=[](I n,I op,I delta)->I {
        return op==2 ? n : delta>CounterTurn::maximum-n ? -1 : n+delta;
    };
    const auto never=[](I,I,I)->I { throw std::logic_error("replay must not execute guest"); };
    {
        SqliteActorSync store(db);
        store.activate(key,1);
        store.activate(other,1);
        for(int i=1;i<=100;++i) {
            auto r=CounterTurn::run(store,key,1,"inc-"+std::to_string(i),1,1,transition);
            assert(r && !r->duplicate && r->revision==i && r->reply==std::to_string(i));
        }
        auto r=CounterTurn::run(store,key,1,"inc-1",1,1,never);
        assert(r && r->duplicate && r->reply=="1");
        rejects([&]{CounterTurn::run(store,key,1,"inc-1",1,2,never);});
        rejects([&]{CounterTurn::run(store,key,1,"bad",1,1,[](I,I,I)->I{return 999;});});
        assert(store.snapshot(key).bytes=="100");
        assert(store.snapshot(other).revision==0);
        bool failed=false;
        try { CounterTurn::run(store,key,1,"lost",1,1,transition,true); }
        catch(const std::runtime_error&) {failed=true;}
        assert(failed && store.snapshot(key).revision==100);
        assert(!store.lookup_receipt(key,1,"lost","1:1",CounterTurn::schema));
        assert(store.changes_after(key,99).size()==1);
    }
    // Commit then terminate without an application response.
    const pid_t child=fork();
    assert(child>=0);
    if(child==0) {
        try {
            SqliteActorSync store(db);
            auto r=CounterTurn::run(store,key,1,"max",1,999999900,transition);
            _exit(r && r->reply=="1000000000" ? 42 : 2);
        } catch(...) { _exit(3); }
    }
    int status=0;
    assert(waitpid(child,&status,0)==child);
    assert(WIFEXITED(status) && WEXITSTATUS(status)==42);
    {
        SqliteActorSync store(db);
        store.activate(key,2);
        rejects([&]{CounterTurn::run(store,key,1,"max",1,999999900,never);});
        auto r=CounterTurn::run(store,key,2,"max",1,999999900,never);
        assert(r && r->duplicate && r->reply=="1000000000");
        assert(!CounterTurn::run(store,key,2,"overflow",1,1,transition));
        assert(store.snapshot(key).revision==101);
        auto read=CounterTurn::run(store,key,2,"read",2,0,transition);
        assert(read && read->reply=="1000000000");
        auto isolated=CounterTurn::run(store,other,1,"max",1,1,transition);
        assert(isolated && isolated->reply=="1");
        assert(store.snapshot(other).bytes=="1");
        assert(store.snapshot(key).bytes=="1000000000");
    }
    for(const auto* bad: {"","00","-1","+1","1000000001","1x"})
        rejects([&]{(void)CounterTurn::decode(bad);});
    std::filesystem::remove_all(dir);
}
