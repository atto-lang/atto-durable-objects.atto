#include "ores/durable/actor_sync.hpp"
#include <atomic>
#include <chrono>
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
    limits.feed_bytes = 520; // fits max state + two 256-byte identity labels
    limits.feed_entries = 1; // force paging for deterministic tests
    limits.outbox_entries_per_actor = 2;
    limits.dedupe_entries_per_actor = 3;
    limits.actor_keys_per_database = 2;
    limits.projection_consumers_per_actor = 1;
    rejects<std::invalid_argument>([&]{ auto invalid=limits; invalid.state_bytes=0; SqliteActorSync bad(primary,invalid); });
    rejects<std::invalid_argument>([&]{ auto invalid=limits; invalid.feed_bytes=519; SqliteActorSync bad(primary,invalid); });
    rejects<std::invalid_argument>([&]{ auto invalid=limits; invalid.feed_entries=0; SqliteActorSync bad(primary,invalid); });
    {
        SqliteActorSync source(primary,limits);
        SqliteActorSync replica(secondary,limits);
        source.activate(actor,1);
        source.activate(other,1);
        rejects<std::length_error>([&]{source.activate({"tenant1","session","Actor","a3"},1);});
        replica.bootstrap_projection("worker",actor,source.snapshot(actor));
        replica.bootstrap_projection("worker",actor,source.snapshot(actor));
        rejects<std::length_error>([&]{
            replica.bootstrap_projection("second-reader",actor,source.snapshot(actor));
        });
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
        // A checkpoint has no recorded event ID. It must not accept a forged
        // same-revision event merely because its snapshot bytes match.
        rejects<std::logic_error>([&]{
            replica.apply_projection("worker",actor,{1,3,"fabricated","s1","cc"});
        });
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
    // Reopen with stricter quotas must fail before exposing even a read
    // handle; an existing database cannot silently bypass new admission.
    {
        auto smaller=limits;
        smaller.actor_keys_per_database=1;
        rejects<std::length_error>([&]{SqliteActorSync reject(primary,smaller);});
        smaller=limits;
        smaller.outbox_entries_per_actor=1;
        rejects<std::length_error>([&]{SqliteActorSync reject(primary,smaller);});
        smaller=limits;
        smaller.dedupe_entries_per_actor=2;
        rejects<std::length_error>([&]{SqliteActorSync reject(primary,smaller);});
        smaller=limits;
        smaller.state_bytes=1;
        rejects<std::length_error>([&]{SqliteActorSync reject(primary,smaller);});
        SqliteActorSync valid(primary,limits);
        assert(valid.snapshot(actor).revision==3);
    }
    // Exact-byte budget pagination leaves the second event unread, even
    // though the SQL LIMIT is larger than one. The unfinished SELECT must be
    // finalized before COMMIT; a following page must remain accessible.
    {
        SyncLimits byte_limits;
        byte_limits.state_bytes=8;
        byte_limits.command_bytes=8;
        byte_limits.reply_bytes=8;
        byte_limits.feed_bytes=520;
        byte_limits.feed_entries=8;
        byte_limits.outbox_entries_per_actor=2;
        const std::string db=std::string(dir)+"/byte-pages.db";
        SqliteActorSync source(db,byte_limits);
        const SyncKey key{"tenant","space","Actor","big-labels"};
        source.activate(key,1);
        const std::string label(250,'q');
        const std::string schema(250,'s');
        source.commit(key,1,0,label+"1","command",schema,"one","ok");
        source.commit(key,1,1,label+"2","command",schema,"two","ok");
        auto first=source.changes_after(key,0,8);
        assert(first.size()==1 && first.front().revision==1);
        auto second=source.changes_after(key,first.back().revision,8);
        assert(second.size()==1 && second.front().revision==2);
        assert(source.changes_after(key,2,8).empty());
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
    // Separate SQLite connections: polling uses a read-only WAL snapshot
    // while 100 bounded commits progress. No sleeps or unbounded waits.
    {
        const SyncKey stream{"tenant3","session","Updates","clock"};
        SqliteActorSync writer(primary), reader(primary);
        writer.activate(stream,1);
        std::atomic<bool> finished{false};
        std::atomic<int> writer_errors{0};
        std::thread producer([&]{
            try {
                for (std::int64_t i=1;i<=100;++i) {
                    const auto id=std::to_string(i);
                    writer.commit(stream,1,i-1,id,id,"tick.v1",id,"ok");
                }
            } catch(...) { ++writer_errors; }
            finished=true;
        });
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(15);
        std::int64_t seen=0;
        while(seen<100 && std::chrono::steady_clock::now()<deadline) {
            const auto changes=reader.changes_after(stream,seen,16);
            for(const auto& change:changes) {
                assert(change.revision==seen+1);
                assert(change.bytes==std::to_string(change.revision));
                seen=change.revision;
            }
            if(changes.empty()) std::this_thread::yield();
            if(finished && writer_errors) break;
        }
        producer.join();
        assert(writer_errors==0 && seen==100 && reader.snapshot(stream).revision==100);
    }
    std::filesystem::remove_all(dir);
}
