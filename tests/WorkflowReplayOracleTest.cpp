#include "WorkflowReplayOracle.hpp"
#include <cassert>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

using namespace oreslang::durable;
template <class E, class F> static void rejects(F&& f) {
    bool caught = false;
    try { f(); } catch (const E&) { caught = true; }
    assert(caught);
}
static WorkflowCommand cmd(unsigned long long n, std::string id,
                           std::string kind = "activity",
                           std::string digest = std::string(64, 'a')) {
    return {n, std::move(id), std::move(kind), std::move(digest)};
}
int main() {
    WorkflowReplayOracle first(3);
    assert(first.admit(cmd(1, "reserve")) == ReplayAdmission::NewlyRecorded);
    assert(first.admit(cmd(2, "timer_1", "timer")) == ReplayAdmission::NewlyRecorded);
    first.require_full_replay();
    auto history = first.snapshot();
    assert(history.size() == 2);
    WorkflowReplayOracle recovered(3, history);
    rejects<std::logic_error>([&]{ recovered.require_full_replay(); });
    rejects<std::logic_error>([&]{ recovered.admit(cmd(1, "wrong")); });
    assert(recovered.cursor() == 0);
    assert(recovered.admit(cmd(1, "reserve")) == ReplayAdmission::Replayed);
    rejects<std::logic_error>([&]{ recovered.admit(cmd(2, "timer_1", "timer",
                                                        std::string(64,'b'))); });
    assert(recovered.cursor() == 1);
    assert(recovered.admit(cmd(2, "timer_1", "timer")) == ReplayAdmission::Replayed);
    recovered.require_full_replay();
    rejects<std::invalid_argument>([&]{ recovered.admit(cmd(4, "skip")); });
    rejects<std::logic_error>([&]{ recovered.admit(cmd(3, "reserve")); });
    rejects<std::invalid_argument>([&]{ recovered.admit(cmd(3, "bad/path")); });
    rejects<std::invalid_argument>([&]{ recovered.admit(cmd(3, "x", "unknown")); });
    rejects<std::invalid_argument>([&]{ recovered.admit(cmd(3, "x", "signal", "AA")); });
    assert(recovered.snapshot() == history);
    assert(recovered.admit(cmd(3, "notify", "signal")) == ReplayAdmission::NewlyRecorded);
    rejects<std::length_error>([&]{ recovered.admit(cmd(4, "overflow")); });
    assert(recovered.snapshot().size() == 3);
    rejects<std::invalid_argument>([]{ WorkflowReplayOracle invalid(0); });
    rejects<std::invalid_argument>([]{ WorkflowReplayOracle invalid(2,
        {cmd(1, "same"), cmd(2, "same")}); });
    rejects<std::invalid_argument>([]{ WorkflowReplayOracle invalid(2,
        {cmd(2, "noncontiguous")}); });
    return 0;
}
