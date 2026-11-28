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
using K=ScalarCheckpointV1::Kind;
const std::vector<ScalarCheckpointV1::Field> fields{{"balance",K::Int},{"open",K::Bool},{"last",K::Int}};
const ScalarCheckpointV1 codec("Ledger",fields);
const std::string digest(64,'a'); // fixture only: not an authenticated artifact
const ScalarTurnV1 turn("Ledger",fields,digest,codec.encode({I(0),true,I(0)}));
const SyncKey key{"t","s","Ledger","one"};
static std::string state(I n,bool open){return codec.encode({n,open,n});}
static std::string guest(std::string bytes,std::string cmd){
    auto v=codec.decode(bytes);assert(cmd=="inc");
    return state(std::get<I>(v[0])+1,!std::get<bool>(v[1]));
}
template<class F> void reject(F f){bool denied=false;try{f();}catch(const std::logic_error&){denied=true;}assert(denied);}
int main(){
    char dir[]="/tmp/atto-scalar-turn-XXXXXX";assert(mkdtemp(dir));
    const auto path=std::string(dir)+"/actor.db";
    {SqliteActorSync s(path);s.activate(key,1);}
    const auto child=fork();assert(child>=0);
    if(child==0){try{SqliteActorSync s(path);auto r=turn.run(s,key,1,"first","inc",guest);if(r.revision!=1||r.reply!=state(1,false))_exit(10);kill(getpid(),SIGKILL);_exit(11);}catch(...){_exit(12);}}
    int status=0;assert(waitpid(child,&status,0)==child&&WIFSIGNALED(status)&&WTERMSIG(status)==SIGKILL);
    {
        SqliteActorSync s(path);assert(s.snapshot(key).bytes==state(1,false));
        int calls=0;auto counted=[&](std::string a,std::string b){++calls;return guest(std::move(a),std::move(b));};
        auto replay=turn.run(s,key,1,"first","inc",counted);assert(replay.duplicate&&replay.revision==1&&calls==0);
        reject([&]{(void)turn.run(s,key,1,"first","other",counted);});assert(calls==0);
        bool rolled=false;try{(void)turn.run(s,key,1,"second","inc",counted,true);}catch(const std::runtime_error&){rolled=true;}
        assert(rolled&&s.snapshot(key).revision==1&&calls==1);
        reject([&]{(void)turn.run(s,key,1,"bad","inc",[](auto,auto){return std::string("bad");});});assert(s.snapshot(key).revision==1);
        auto r=turn.run(s,key,1,"second","inc",counted);assert(r.revision==2&&r.reply==state(2,true));
        assert(s.changes_after(key,0).size()==2);
        s.activate(key,2);reject([&]{(void)turn.run(s,key,1,"second","inc",counted);});
        replay=turn.run(s,key,2,"first","inc",counted);assert(replay.duplicate&&replay.reply==state(1,false)&&calls==2);
        const ScalarTurnV1 wrong("Ledger",fields,std::string(64,'b'),state(0,true));
        reject([&]{(void)wrong.run(s,key,2,"new","inc",counted);});
        // In-place same-revision state change during guest execution must
        // not be overwritten by a proposal based on the previous bytes.
        sqlite3* raw=nullptr;assert(sqlite3_open(path.c_str(),&raw)==SQLITE_OK);
        auto rewrite=[&](const std::string& bytes){sqlite3_stmt* p=nullptr;assert(sqlite3_prepare_v2(raw,"UPDATE sync_objects SET snapshot=? WHERE tenant='t' AND space='s' AND actor_type='Ledger' AND id='one'",-1,&p,nullptr)==SQLITE_OK);assert(sqlite3_bind_blob(p,1,bytes.data(),static_cast<int>(bytes.size()),SQLITE_TRANSIENT)==SQLITE_OK);assert(sqlite3_step(p)==SQLITE_DONE);assert(sqlite3_changes(raw)==1);assert(sqlite3_finalize(p)==SQLITE_OK);};
        reject([&]{(void)turn.run(s,key,2,"third","inc",[&](auto a,auto b){rewrite(state(99,false));return guest(a,b);});});
        assert(s.snapshot(key).revision==2&&s.snapshot(key).bytes==state(99,false));
        // A current-revision replay cannot launder changed or corrupt state.
        reject([&]{(void)turn.run(s,key,2,"second","inc",counted);});
        rewrite("broken");reject([&]{(void)turn.run(s,key,2,"first","inc",counted);});
        rewrite(state(2,true));assert(sqlite3_close(raw)==SQLITE_OK);
        auto third=turn.run(s,key,2,"third","inc",counted);assert(third.revision==3&&third.reply==state(3,false));
        assert(s.changes_after(key,0).size()==3);
    }
    std::filesystem::remove_all(dir);
    std::cout<<"SCALAR_TURN_V1_SQLITE_RECOVERY_PASS commits=3 physical_kills=1 replays=2 rollback=1\n";
}
