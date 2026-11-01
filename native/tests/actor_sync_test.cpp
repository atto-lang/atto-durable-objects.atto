#include "ores/durable/actor_sync.hpp"
#include <cassert>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

using namespace ores::durable;
template <typename Error, typename Fn> void rejects(Fn fn) {
    bool caught=false;
    try { fn(); } catch (const Error&) { caught=true; }
    assert(caught);
}
static void byte(int fd, char v) {
    assert(write(fd,&v,1)==1);
}
static char byte(int fd) {
    char v=0;
    assert(read(fd,&v,1)==1);
    return v;
}
int main() {
    char dir[]="/tmp/ores-actor-sync-XXXXXX";
    assert(mkdtemp(dir)!=nullptr);
    const std::string primary=std::string(dir)+"/primary.db";
    const std::string secondary=std::string(dir)+"/replica.db";
    const SyncKey key{"tenantA","carts","Cart","cart1"};
    const SyncKey foreign{"tenantB","carts","Cart","cart1"};
    {
        SqliteActorSync source(primary);
        source.activate(key,1);
        source.activate(foreign,1);
        assert(source.snapshot(key).revision==0);
        rejects<std::logic_error>([&]{source.activate(key,1);});
        rejects<std::invalid_argument>([&]{source.activate({std::string("bad\0tenant",10),"carts","Cart","x"},2);});
        rejects<std::invalid_argument>([&]{source.changes_after(key,0,0);});
        rejects<std::logic_error>([&]{source.commit(key,2,0,"op-x","cmd","s1","bad","reply");});
        rejects<std::runtime_error>([&]{source.commit(key,1,0,"fail","cmd","s1","bad","reply",true);});
        assert(source.snapshot(key).revision==0);
        assert(source.changes_after(key,0).empty());
    }
    // The follower is a separate OS process with its own SQLite connections
    // and its own persisted replica. This is a host-reference transport via
    // SQLite polling, not compiled Oreslang guest messaging or distributed HA.
    int ready[2], go[2];
    assert(pipe(ready)==0 && pipe(go)==0);
    pid_t pid=fork();
    assert(pid>=0);
    if(pid==0) {
        close(ready[0]); close(go[1]);
        try {
            SqliteActorSync source(primary);
            SqliteActorSync replica(secondary);
            auto first=source.snapshot(key);
            replica.bootstrap_projection("replica1",key,first);
            byte(ready[1],'R');
            if(byte(go[0])!='G') _exit(3);
            const auto events=source.changes_after(key,first.revision);
            if(events.size()!=2) _exit(4);
            for(const auto& e:events) {
                if(!replica.apply_projection("replica1",key,e)) _exit(5);
            }
            if(replica.apply_projection("replica1",key,events.back())) _exit(6);
            const auto end=replica.projection("replica1",key);
            if(end.revision!=2 || end.bytes!="two" || end.schema!="cart.v1") _exit(7);
        } catch(...) { _exit(8); }
        close(ready[1]); close(go[0]);
        _exit(0);
    }
    close(ready[1]); close(go[0]);
    assert(byte(ready[0])=='R');
    {
        SqliteActorSync source(primary);
        auto one=source.commit(key,1,0,"op-1","add one","cart.v1","one","reply-one");
        assert(one.revision==1 && !one.duplicate);
        auto dup=source.commit(key,1,0,"op-1","add one","cart.v1","bad","no");
        assert(dup.duplicate && dup.revision==1 && dup.reply=="reply-one");
        rejects<std::logic_error>([&]{source.commit(key,1,1,"op-1","different","cart.v1","bad","no");});
        rejects<std::logic_error>([&]{source.commit(key,1,0,"op-2","add two","cart.v1","two","r2");});
        assert(source.commit(key,1,1,"op-2","add two","cart.v1","two","reply-two").revision==2);
        assert(source.snapshot(foreign).revision==0);
        rejects<ResyncRequired>([&]{source.changes_after(key,3);});
    }
    byte(go[1],'G');
    close(go[1]); close(ready[0]);
    int status=0;
    assert(waitpid(pid,&status,0)==pid);
    assert(WIFEXITED(status) && WEXITSTATUS(status)==0);
    {
        SqliteActorSync source(primary), replica(secondary);
        const auto end=replica.projection("replica1",key);
        assert(end.revision==2 && end.bytes=="two");
        source.activate(key,2);
        rejects<std::logic_error>([&]{source.commit(key,1,2,"stale","x","cart.v1","three","r3");});
        assert(source.commit(key,2,2,"op-3","three","cart.v1","three","ok").revision==3);
        auto events=source.changes_after(key,2);
        assert(events.size()==1 && events[0].revision==3);
        rejects<ResyncRequired>([&]{replica.apply_projection("replica1",key,{2,5,"op-5","cart.v1","five"});});
        assert(replica.apply_projection("replica1",key,events[0]));
        assert(!replica.apply_projection("replica1",key,events[0]));
        rejects<std::logic_error>([&]{replica.apply_projection("replica1",key,{2,3,"op-3","cart.v1","conflict"});});
        source.compact(key,2,2);
        rejects<ResyncRequired>([&]{source.changes_after(key,1);});
        assert(source.changes_after(key,2).size()==1);
        rejects<std::logic_error>([&]{source.compact(key,1,3);});
        rejects<std::logic_error>([&]{source.compact(key,2,1);});
        rejects<std::length_error>([&]{source.commit(key,2,3,"large","x","cart.v1",std::string((1<<20)+1,'a'),"ok");});
        assert(source.snapshot(key).revision==3);
    }
    std::filesystem::remove_all(dir);
}
