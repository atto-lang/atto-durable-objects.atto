#include "ores/durable/actor_sync.hpp"
#include <atomic>
#include <cassert>
#include <thread>
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
        // Repeated announcements must only raise the minimum required epoch.
        rejects<ResyncRequired>([&]{
            upgraded.apply_projection("reader",key,{4,8,"newer","cart.v1","eight"});
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
        rejects<std::logic_error>([&]{
            reopened.resync_projection("reader",key,{2,7,"cart.v1","seven"});
        });
        rejects<std::logic_error>([&]{
            reopened.resync_projection("reader",key,{3,7,"cart.v1","seven"});
        });
        assert(reopened.resync_projection("reader",key,{4,7,"cart.v1","seven"}));
        assert(!reopened.resync_projection("reader",key,{4,7,"cart.v1","seven"}));
        assert(reopened.apply_projection("reader",key,{4,8,"new-op","cart.v1","eight"}));
        assert(!reopened.apply_projection("reader",key,{4,8,"new-op","cart.v1","eight"}));
        assert(reopened.projection("reader",key).bytes=="eight");
    }
    // A partially upgraded database may have a boolean poison flag
    // without recording the newer generation. No secure migration can
    // invent that missing epoch; reject instead of clearing the flag.
    {
        const std::string unsafe_db=std::string(dir)+"/unknown-fence.db";
        sqlite3* legacy_db=nullptr;
        assert(sqlite3_open(unsafe_db.c_str(),&legacy_db)==SQLITE_OK);
        const char* flagged=
            "CREATE TABLE sync_projection("
            "consumer TEXT NOT NULL, tenant TEXT NOT NULL, space TEXT NOT NULL,"
            "actor_type TEXT NOT NULL, id TEXT NOT NULL, epoch INTEGER NOT NULL,"
            "revision INTEGER NOT NULL, op_id TEXT NOT NULL DEFAULT '',"
            "schema_fp TEXT NOT NULL, snapshot BLOB NOT NULL,"
            "resync_required INTEGER NOT NULL DEFAULT 0,"
            "PRIMARY KEY(consumer,tenant,space,actor_type,id)) WITHOUT ROWID;"
            "INSERT INTO sync_projection VALUES("
            "'reader','tenant','space','Cart','42',1,7,'','cart.v1','seven',1);";
        char* message=nullptr;
        const int rc=sqlite3_exec(legacy_db,flagged,nullptr,nullptr,&message);
        sqlite3_free(message);
        assert(rc==SQLITE_OK);
        assert(sqlite3_close(legacy_db)==SQLITE_OK);
        rejects<std::logic_error>([&]{SqliteActorSync unsafe(unsafe_db);});
    }
    // Two separate SQLite connections discover the same legacy schema at
    // approximately the same time. BEGIN IMMEDIATE must serialize the full
    // inspection/ALTER sequence: neither opener may lose a column-add race.
    {
        const std::string race_db=std::string(dir)+"/concurrent-open.db";
        sqlite3* raw_db=nullptr;
        assert(sqlite3_open(race_db.c_str(),&raw_db)==SQLITE_OK);
        char* error=nullptr;
        const int rc=sqlite3_exec(raw_db,legacy,nullptr,nullptr,&error);
        sqlite3_free(error);
        assert(rc==SQLITE_OK);
        assert(sqlite3_close(raw_db)==SQLITE_OK);
        std::atomic<int> success{0}, rejected{0};
        auto opener=[&] {
            try {
                SqliteActorSync migrated(race_db);
                if (migrated.projection("reader",key).revision==7) ++success;
                else ++rejected;
            } catch (...) { ++rejected; }
        };
        std::thread a(opener), b(opener);
        a.join(); b.join();
        assert(success==2 && rejected==0);
        SqliteActorSync reopened(race_db);
        assert(reopened.projection("reader",key).epoch==1);
    }
    // A refused migration cannot leave newly created reference tables or
    // half of a two-column schema upgrade visible to another connection.
    {
        const std::string refused=std::string(dir)+"/unknown-fence.db";
        sqlite3* db=nullptr;
        assert(sqlite3_open(refused.c_str(),&db)==SQLITE_OK);
        sqlite3_stmt* query=nullptr;
        assert(sqlite3_prepare_v2(db,
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='sync_objects'",
            -1,&query,nullptr)==SQLITE_OK);
        assert(sqlite3_step(query)==SQLITE_ROW);
        assert(sqlite3_column_int(query,0)==0);
        assert(sqlite3_finalize(query)==SQLITE_OK);
        bool has_epoch=false;
        assert(sqlite3_prepare_v2(db,"PRAGMA table_info(sync_projection)",-1,&query,nullptr)==SQLITE_OK);
        while (sqlite3_step(query)==SQLITE_ROW) {
            const char* name=reinterpret_cast<const char*>(sqlite3_column_text(query,1));
            if (name && std::string(name)=="resync_epoch") has_epoch=true;
        }
        assert(sqlite3_finalize(query)==SQLITE_OK);
        assert(!has_epoch);
        assert(sqlite3_close(db)==SQLITE_OK);
    }
    std::filesystem::remove_all(dir);
}
