#include "ores/durable/actor_sync.hpp"
#include "ores/durable/scalar_turn_v1.hpp"
#include <cassert>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sys/wait.h>
#include <unistd.h>

using namespace ores::durable;
using I=std::int64_t;
using C=BoundedCheckpointV2;
using K=C::Kind;
const std::vector<C::Field> fields{{"count",K::Int},{"open",K::Bool},{"name",K::String},{"payload",K::Bytes}};
const C codec("Ledger",fields);
const std::string digest(64,'a'); // fixture only, not an artifact credential
const std::string name("A\0\xc3\xa9\xf0\x9f\x98\x80",8);
const C::Bytes payload{0,255,128};
static std::string state(I n,bool open){return codec.encode({n,open,name,payload});}
const BoundedTurnV2 turn("Ledger",fields,digest,state(0,true));
const SyncKey key{"t","s","Ledger","v2"};
template<class F> static void reject(F&& f){bool denied=false;try{f();}catch(const std::logic_error&){denied=true;}assert(denied);}
static std::string guest(std::string bytes,std::string command){
    const auto old=codec.decode(bytes);
    assert(command==std::string("inc\0",4));
    assert(std::get<std::string>(old[2])==name&&std::get<C::Bytes>(old[3])==payload);
    return state(std::get<I>(old[0])+1,!std::get<bool>(old[1]));
}
int main(){
    char dir[]="/tmp/atto-bounded-turn-XXXXXX";assert(mkdtemp(dir));
    const auto db=std::string(dir)+"/actor.db",command=std::string("inc\0",4);
    {SqliteActorSync s(db);s.activate(key,1);}
    const auto child=fork();assert(child>=0);
    if(child==0){try{SqliteActorSync s(db);const auto r=turn.run(s,key,1,"first",command,guest);if(r.revision!=1||r.reply!=state(1,false))_exit(10);kill(getpid(),SIGKILL);_exit(11);}catch(...){_exit(12);}}
    int status=0;assert(waitpid(child,&status,0)==child&&WIFSIGNALED(status)&&WTERMSIG(status)==SIGKILL);
    {
        SqliteActorSync s(db);assert(s.snapshot(key).bytes==state(1,false));
        assert(s.snapshot(key).schema==turn.schema());
        int calls=0;
        auto counted=[&](std::string a,std::string b){++calls;return guest(std::move(a),std::move(b));};
        auto replay=turn.run(s,key,1,"first",command,counted);assert(replay.duplicate&&replay.reply==state(1,false)&&calls==0);
        reject([&]{(void)turn.run(s,key,1,"first","inc",counted);});assert(calls==0);
        const ScalarCheckpointV1 old_codec("Ledger",{{"count",ScalarCheckpointV1::Kind::Int}});
        const ScalarTurnV1 old_turn("Ledger",{{"count",ScalarCheckpointV1::Kind::Int}},digest,old_codec.encode({I(0)}));
        reject([&]{(void)old_turn.run(s,key,1,"old",command,counted);});assert(calls==0);
        const BoundedTurnV2 wrong("Ledger",fields,std::string(64,'b'),state(0,true));
        reject([&]{(void)wrong.run(s,key,1,"wrong",command,counted);});assert(calls==0);
        reject([&]{(void)turn.run(s,key,1,"bad",command,[&](auto,auto){return old_codec.encode({I(1)});});});
        reject([&]{(void)turn.run(s,key,1,"large",command,[](auto,auto){return std::string(65537,'x');});});
        bool rollback=false;try{(void)turn.run(s,key,1,"second",command,counted,true);}catch(const std::runtime_error&){rollback=true;}
        assert(rollback&&s.snapshot(key).revision==1&&calls==1);
        s.activate(key,2);reject([&]{(void)turn.run(s,key,1,"stale",command,counted);});
        const auto second=turn.run(s,key,2,"second",command,counted);
        assert(second.revision==2&&second.reply==state(2,true)&&calls==2);
        assert(s.changes_after(key,0).size()==2);
        // The exact schema token remains distinct even for the same digest.
        assert(old_turn.schema()!=turn.schema());
    }
    {
        SqliteActorSync s(db);
        assert(s.snapshot(key).bytes==state(2,true));
        const auto r=turn.run(s,key,2,"second",command,[](auto,auto)->std::string{throw std::logic_error("duplicate dispatched");});
        assert(r.duplicate&&r.reply==state(2,true));
        const SyncKey large_key{"t","s","Large","v2"};
        const std::vector<C::Field> large_fields{{"a",K::Bytes},{"b",K::Bytes},{"c",K::Bytes},{"d",K::Bytes}};
        const C large("Large",large_fields);
        const auto initial=large.encode({C::Bytes{},C::Bytes{},C::Bytes{},C::Bytes{}});
        const auto full=large.encode({C::Bytes(16384,255),C::Bytes(16384,128),C::Bytes(16384,0),C::Bytes(16337,1)});
        assert(full.size()==65536);
        const BoundedTurnV2 large_turn("Large",large_fields,digest,initial);
        s.activate(large_key,1);
        const auto r2=large_turn.run(s,large_key,1,"full","store",[&](auto previous,auto){assert(previous==initial);return full;});
        assert(r2.revision==1&&r2.reply==full&&s.snapshot(large_key).bytes==full);
    }
    std::filesystem::remove_all(dir);
    std::cout<<"BOUNDED_TURN_V2_SQLITE_RECOVERY_PASS commits=3 physical_kills=1 replays=2 rollback=1 max_snapshot=65536\n";
}
