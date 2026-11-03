#include "ores/durable/actor_sync.hpp"
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

using namespace ores::durable;

template<class Error, class F>
static void rejects(F&& fn) {
    bool failed=false;
    try { fn(); } catch(const Error&) { failed=true; }
    assert(failed);
}

int main() {
    char dir[]="/tmp/ores-sync-migration-XXXXXX";
    assert(mkdtemp(dir)!=nullptr);
    const std::string path=std::string(dir)+"/legacy.db";
    sqlite3* raw=nullptr;
    assert(sqlite3_open_v2(path.c_str(), &raw,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,nullptr)==SQLITE_OK);
    // A reference database created by an older revision had no durable
    // per-consumer resync bit. Upgrade must preserve its logical actor.
    const char* legacy =
        "CREATE TABLE sync_projection("
        "consumer TEXT NOT NULL, tenant TEXT NOT NULL, space TEXT NOT NULL,"
        "actor_type TEXT NOT NULL, id TEXT NOT NULL, epoch INTEGER NOT NULL,"
        "revision INTEGER NOT NULL, op_id TEXT NOT NULL DEFAULT '',"
        "schema_fp TEXT NOT NULL, snapshot BLOB NOT NULL,"
        "PRIMARY KEY(consumer,tenant,space,actor_type,id)) WITHOUT ROWID;"
        "INSERT INTO sync_projection(consumer,tenant,space,actor_type,id,epoch,"
        "revision,op_id,schema_fp,snapshot) VALUES("
        "'reader','tenant','space','Cart','42',1,7,'old-op','cart.v1','seven');";
    char* error=nullptr;
    const int creation=sqlite3_exec(raw,legacy,nullptr,nullptr,&error);
    sqlite3_free(error);
    assert(creation==SQLITE_OK);
    assert(sqlite3_close(raw)==SQLITE_OK);

    const SyncKey key{"tenant","space","Cart","42"};
    {
        SqliteActorSync upgraded(path);
        assert(upgraded.projection("reader",key).revision==7);
        rejects<ResyncRequired>([&]{
            upgraded.apply_projection("reader",key,{2,8,"new-op","cart.v1","eight"});
        });
        // An old-epoch delta must also fail after higher epoch discovery.
        rejects<ResyncRequired>([&]{
            upgraded.apply_projection("reader",key,{1,8,"stale","cart.v1","stale"});
        });
        rejects<ResyncRequired>([&]{upgraded.projection("reader",key);});
        // Identical bootstrap cannot be used as a backdoor to clear a fence.
        upgraded.bootstrap_projection("reader",key,{1,7,"cart.v1","seven"});
        rejects<ResyncRequired>([&]{upgraded.projection("reader",key);});
    }
    {
        SqliteActorSync reopened(path);
        rejects<ResyncRequired>([&]{
            reopened.apply_projection("reader",key,{1,8,"stale-2","cart.v1","stale"});
        });
        assert(reopened.resync_projection("reader",key,{2,7,"cart.v1","seven"}));
        assert(!reopened.resync_projection("reader",key,{2,7,"cart.v1","seven"}));
        assert(reopened.apply_projection("reader",key,{2,8,"new-op","cart.v1","eight"}));
        assert(!reopened.apply_projection("reader",key,{2,8,"new-op","cart.v1","eight"}));
        assert(reopened.projection("reader",key).bytes=="eight");
    }
    std::filesystem::remove_all(dir);
}
