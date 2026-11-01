#include "ores/durable/actor_sync.hpp"
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>

using namespace ores::durable;

template <class Error, class F> static void rejects(F&& call) {
    bool rejected = false;
    try { call(); } catch (const Error&) { rejected = true; }
    assert(rejected);
}
int main() {
    char dir[] = "/tmp/ores-sync-hardening-XXXXXX";
    assert(mkdtemp(dir) != nullptr);
    const std::string primary = std::string(dir) + "/source.db";
    const std::string secondary = std::string(dir) + "/projection.db";
    const SyncKey actor{"tenant1","session","Actor","a1"};
    const SyncKey other{"tenant1","session","Actor","a2"};
    SyncLimits limits;
    limits.state_bytes = 8;
    limits.command_bytes = 8;
    limits.reply_bytes = 8;
    limits.feed_bytes = 8; // one full snapshot plus op/schema labels
    limits.outbox_entries_per_actor = 2;
    limits.dedupe_entries_per_actor = 3;
    rejects<std::invalid_argument>([&]{ auto invalid=limits; invalid.state_bytes=0; SqliteActorSync bad(primary,invalid); });
    rejects<std::invalid_argument>([&]{ auto invalid=limits; invalid.feed_bytes=7; SqliteActorSync bad(primary,invalid); });
    {
        SqliteActorSync source(primary,limits);
        SqliteActorSync replica(secondary,limits);
        source.activate(actor,1);
        source.activate(other,1);
        replica.bootstrap_projection("worker",actor,source.snapshot(actor));
        rejects<std::logic_error>([&]{replica.resync_projection("uninitialized",actor,source.snapshot(actor));});
        assert(source.commit(actor,1,0,"o1","c1","s1","aa","ok").revision==1);
        assert(source.commit(actor,1,1,"o2","c2","s1","bb","ok").revision==2);
        assert(source.commit(other,1,0,"o1","c1","s1","aa","ok").revision==1);
        const auto page = source.changes_after(actor,0);
        assert(page.size()==1 && page[0].revision==1);
        assert(source.changes_after(actor,1).size()==1);
        assert(replica.apply_projection("worker",actor,page[0]));
        assert(!replica.apply_projection("worker",actor,page[0]));
        rejects<std::logic_error>([&]{replica.apply_projection("worker",actor,{1,1,"forged","s1","aa"});});
        rejects<ResyncRequired>([&]{replica.apply_projection("worker",actor,{1,3,"o3","s1","cc"});});
        rejects<std::length_error>([&]{source.commit(actor,1,2,"o3","c3","s1","cc","ok");});
        assert(source.snapshot(actor).revision==2);
        assert(source.commit(actor,1,2,"o2","c2","s1","bad","bad").duplicate);
        rejects<std::logic_error>([&]{source.commit(actor,1,2,"o2","c2","s2","bad","bad");});
        source.compact(actor,1,1);
        rejects<ResyncRequired>([&]{source.changes_after(actor,0);});
        assert(source.commit(actor,1,2,"o3","c3","s1","cc","ok").revision==3);
        assert(source.snapshot(actor).bytes=="cc");
        rejects<std::length_error>([&]{source.commit(actor,1,3,"o4","c4","s1","dd","ok");});
        assert(source.snapshot(actor).revision==3);
        const auto ss = source.snapshot(actor);
        assert(replica.resync_projection("worker",actor,ss));
        assert(!replica.resync_projection("worker",actor,ss));
        rejects<std::logic_error>([&]{replica.resync_projection("worker",actor,{1,2,"s1","bb"});});
        rejects<std::logic_error>([&]{replica.resync_projection("worker",actor,{1,3,"s1","evil"});});
        assert(replica.projection("worker",actor).revision==3);
        source.activate(actor,2);
        rejects<std::logic_error>([&]{source.commit(actor,1,3,"late","cmd","s1","dd","ok");});
        assert(replica.resync_projection("worker",actor,source.snapshot(actor)));
        rejects<ResyncRequired>([&]{replica.apply_projection("worker",actor,{1,4,"stale","s1","dd"});});
        rejects<std::length_error>([&]{source.commit(other,1,1,"big","cmd","s1",std::string(9,'x'),"ok");});
    }
    {
        SqliteActorSync source(primary,limits), replica(secondary,limits);
        assert(source.snapshot(actor).epoch==2);
        assert(source.snapshot(actor).revision==3);
        assert(replica.projection("worker",actor).epoch==2);
        assert(replica.projection("worker",actor).bytes=="cc");
        assert(source.commit(actor,2,0,"o1","c1","s1","ignored","ok").duplicate);
        rejects<std::logic_error>([&]{source.commit(actor,2,0,"o1","forged","s1","ignored","ok");});
    }
    // Two independent DB connections racing on the exact same actor/revision.
    {
        const SyncKey race{"tenant2","tests","Race","one"};
        SqliteActorSync a(primary), b(primary);
        a.activate(race,1);
        std::atomic<int> accepted{0}, conflicts{0};
        auto worker = [&](SqliteActorSync& db, const char* id) {
            try { auto r = db.commit(race,1,0,id,id,"race.v1",id,"ok");
                  if (!r.duplicate) ++accepted; }
            catch (const std::logic_error&) { ++conflicts; }
        };
        std::thread left(worker,std::ref(a),"left");
        std::thread right(worker,std::ref(b),"right");
        left.join(); right.join();
        assert(accepted==1 && conflicts==1);
        assert(a.snapshot(race).revision==1);
        assert(a.changes_after(race,0).size()==1);
    }
    std::filesystem::remove_all(dir);
}
