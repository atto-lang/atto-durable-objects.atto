#include "ores/durable/actor_sync.hpp"
#include "ores/durable/workflow_effect.hpp"
#include <cassert>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

using namespace ores::durable;
template<class E,class F> static void rejects(F&& fn) {
    bool caught=false;
    try { fn(); } catch(const E&) { caught=true; }
    assert(caught);
}

int main() {
    char dir[]="/tmp/ores-workflow-replay-XXXXXX";
    assert(mkdtemp(dir)!=nullptr);
    const std::string db=std::string(dir)+"/workflow.db";
    const SyncKey key{"tenant","jobs","Workflow","invoice-1"};
    const WorkflowEffect charge{1,WorkflowEffectKind::activity,"charge","amount=10"};
    const WorkflowEffect timer{2,WorkflowEffectKind::timer,"retry","wait=10"};
    const WorkflowEffect cancel{3,WorkflowEffectKind::cancel,"stop","reason=user"};
    assert(charge.canonical()==charge.canonical());
    const WorkflowEffect modified{1,WorkflowEffectKind::activity,"charge","amount=11"};
    const WorkflowEffect ambiguous_a{1,WorkflowEffectKind::activity,"a","x|2:y"};
    const WorkflowEffect ambiguous_b{1,WorkflowEffectKind::activity,"a","x|"};
    assert(charge.canonical()!=modified.canonical());
    assert(ambiguous_a.canonical()!=ambiguous_b.canonical());
    rejects<std::invalid_argument>([]{ WorkflowEffect{0,WorkflowEffectKind::activity,"ok",""}.canonical(); });
    rejects<std::invalid_argument>([]{ WorkflowEffect{1,static_cast<WorkflowEffectKind>(9),"ok",""}.canonical(); });
    rejects<std::invalid_argument>([]{ WorkflowEffect{1,WorkflowEffectKind::timer,"bad/name",""}.canonical(); });
    rejects<std::length_error>([]{ WorkflowEffect{1,WorkflowEffectKind::activity,"valid",std::string(4097,'x')}.canonical(); });
    {
        SqliteActorSync store(db);
        store.activate(key,1);
        auto first=store.commit(key,1,0,"workflow-effect-1",charge.canonical(),
                                "workflow.effect.v1","running:1","charged");
        assert(!first.duplicate&&first.revision==1&&first.reply=="charged");
        rejects<std::logic_error>([&]{
            store.commit(key,1,1,"workflow-effect-1",
                         WorkflowEffect{1,WorkflowEffectKind::activity,"charge","amount=11"}.canonical(),
                         "workflow.effect.v1","forged","ignored");
        });
        assert(store.snapshot(key).revision==1);
    }
    // Destroy and reopen durable state as a separate logical runtime lifetime.
    {
        SqliteActorSync restored(db);
        const auto snap=restored.snapshot(key);
        assert(snap.revision==1&&snap.bytes=="running:1");
        const auto repeated=restored.commit(key,1,0,"workflow-effect-1",charge.canonical(),
                                            "workflow.effect.v1","wrong","wrong");
        assert(repeated.duplicate&&repeated.revision==1&&repeated.reply=="charged");
        assert(restored.commit(key,1,1,"workflow-effect-2",timer.canonical(),
                               "workflow.effect.v1","waiting","scheduled").revision==2);
        assert(restored.commit(key,1,2,"workflow-effect-3",cancel.canonical(),
                               "workflow.effect.v1","cancelled","cancelled").revision==3);
        restored.activate(key,2);
        rejects<std::logic_error>([&]{
            restored.commit(key,1,3,"workflow-effect-4",
                            WorkflowEffect{4,WorkflowEffectKind::child,"late",""}.canonical(),
                            "workflow.effect.v1","BAD","BAD");
        });
        assert(restored.snapshot(key).bytes=="cancelled");
        assert(restored.changes_after(key,0).size()==3);
    }
    {
        SqliteActorSync recovered(db);
        assert(recovered.snapshot(key).epoch==2);
        assert(recovered.snapshot(key).revision==3);
        assert(recovered.snapshot(key).bytes=="cancelled");
        auto replay=recovered.commit(key,2,2,"workflow-effect-3",cancel.canonical(),
                                     "workflow.effect.v1","bad","bad");
        assert(replay.duplicate&&replay.reply=="cancelled"&&replay.revision==3);
    }
    std::filesystem::remove_all(dir);
}
