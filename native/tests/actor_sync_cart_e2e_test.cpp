#include "ores/durable/actor_sync.hpp"
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

using namespace ores::durable;

template<class E, class F> static void rejects(F&& fn) {
    bool failed=false;
    try { fn(); } catch(const E&) { failed=true; }
    assert(failed);
}

int main() {
    char dir[]="/tmp/ores-cart-sync-XXXXXX";
    assert(mkdtemp(dir)!=nullptr);
    const std::string source_db=std::string(dir)+"/source.db";
    const std::string replica_db=std::string(dir)+"/replica.db";
    const SyncKey cart{"tenant-a","sales","Cart","customer-123"};
    const SyncKey unrelated{"tenant-b","sales","Cart","customer-123"};

    // The source and replica are independent durable databases. Their only
    // shared state is an explicitly transported, trusted host snapshot/change.
    {
        SqliteActorSync source(source_db), replica(replica_db);
        source.activate(cart,1);
        source.activate(unrelated,1);
        replica.bootstrap_projection("projection-a",cart,source.snapshot(cart));
        assert(source.commit(cart,1,0,"add-1","sku+1","cart.v1","sku=1","accepted-1").revision==1);
        auto first=source.changes_after(cart,0);
        assert(first.size()==1);
        assert(replica.apply_projection("projection-a",cart,first.front()));
        assert(replica.projection("projection-a",cart).bytes=="sku=1");
        // Same logical ID under another tenant is not the same actor key.
        assert(source.snapshot(unrelated).revision==0);
        assert(source.snapshot(unrelated).bytes=="");
    }
    {
        // Process-local activations, caches, and handles were destroyed.
        SqliteActorSync source(source_db), replica(replica_db);
        assert(source.snapshot(cart).revision==1);
        assert(source.snapshot(cart).bytes=="sku=1");
        const auto retried=source.commit(cart,1,0,"add-1","sku+1","cart.v1","WRONG","WRONG");
        assert(retried.duplicate && retried.revision==1 && retried.reply=="accepted-1");
        assert(source.changes_after(cart,0).size()==1);
        assert(source.commit(cart,1,1,"add-2","sku+1","cart.v1","sku=2","accepted-2").revision==2);
        source.compact(cart,1,1);
        rejects<ResyncRequired>([&]{source.changes_after(cart,0);});
        // The reader missed revision 2. It cannot fill the gap with a
        // fabricated event or skip it silently.
        rejects<ResyncRequired>([&]{
            replica.apply_projection("projection-a",cart,{1,3,"made-up","cart.v1","sku=3"});
        });
        assert(replica.resync_projection("projection-a",cart,source.snapshot(cart)));
        assert(replica.projection("projection-a",cart).revision==2);
        rejects<std::logic_error>([&]{
            replica.apply_projection("projection-a",cart,{1,2,"forged","cart.v1","sku=2"});
        });
        assert(source.commit(cart,1,2,"add-3","sku+1","cart.v1","sku=3","accepted-3").revision==3);
        const auto third=source.changes_after(cart,2);
        assert(third.size()==1 && third.front().revision==3);
        assert(replica.apply_projection("projection-a",cart,third.front()));
        assert(!replica.apply_projection("projection-a",cart,third.front()));
        assert(replica.projection("projection-a",cart).bytes=="sku=3");
        source.activate(cart,2);
        rejects<std::logic_error>([&]{
            source.commit(cart,1,3,"add-4","sku+1","cart.v1","sku=4","invalid");
        });
        // Idempotency is tied to logical identity, not activation epoch.
        assert(source.commit(cart,2,1,"add-2","sku+1","cart.v1","ignored","ignored").duplicate);
        assert(source.snapshot(cart).revision==3);
        assert(source.snapshot(unrelated).revision==0);
    }
    std::filesystem::remove_all(dir);
}
