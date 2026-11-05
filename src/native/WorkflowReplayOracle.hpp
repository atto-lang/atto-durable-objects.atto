#pragma once
// Host-only deterministic workflow command replay oracle.
// No guest ABI, side-effect execution, persistence, or scheduler ownership.
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace oreslang::durable {
struct WorkflowCommand {
    std::uint64_t sequence;
    std::string id;
    std::string kind; // exactly activity, timer, or signal
    std::string payload_sha256;
    bool operator==(const WorkflowCommand& other) const {
        return sequence == other.sequence && id == other.id &&
               kind == other.kind && payload_sha256 == other.payload_sha256;
    }
};
enum class ReplayAdmission { Replayed, NewlyRecorded };

class WorkflowReplayOracle final {
public:
    explicit WorkflowReplayOracle(std::size_t maximum,
                                  std::vector<WorkflowCommand> committed = {})
        : maximum_(maximum), committed_(std::move(committed)) {
        if (maximum_ == 0 || maximum_ > 65536 || committed_.size() > maximum_)
            throw std::invalid_argument("invalid workflow replay budget");
        for (std::size_t index = 0; index < committed_.size(); ++index) {
            validate(committed_[index], index + 1);
            if (!ids_.insert(committed_[index].id).second)
                throw std::invalid_argument("duplicate historical workflow command ID");
        }
    }
    // Replay must consume committed history in exact order before any new
    // command may be appended. No callback or external effect runs here.
    ReplayAdmission admit(const WorkflowCommand& command) {
        validate(command, cursor_ + 1);
        if (cursor_ < committed_.size()) {
            if (!(committed_[cursor_] == command))
                throw std::logic_error("workflow replay divergence");
            ++cursor_;
            return ReplayAdmission::Replayed;
        }
        if (committed_.size() == maximum_)
            throw std::length_error("workflow replay command budget exceeded");
        if (ids_.find(command.id) != ids_.end())
            throw std::logic_error("workflow command ID reused");
        committed_.push_back(command);
        ids_.insert(command.id);
        ++cursor_;
        return ReplayAdmission::NewlyRecorded;
    }
    // This snapshot is only a value to hand to an independently durable WAL
    // writer; producing it does NOT perform a durable commit or ACK.
    std::vector<WorkflowCommand> snapshot() const { return committed_; }
    void require_full_replay() const {
        if (cursor_ != committed_.size())
            throw std::logic_error("workflow replay skipped committed commands");
    }
    std::size_t cursor() const { return cursor_; }
private:
    static void validate(const WorkflowCommand& command, std::size_t expected) {
        if (expected == 0 || command.sequence != expected)
            throw std::invalid_argument("noncontiguous workflow sequence");
        if (command.id.empty() || command.id.size() > 128)
            throw std::invalid_argument("invalid workflow command ID");
        for (unsigned char c : command.id) {
            const bool valid = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                               (c >= '0' && c <= '9') || c == '-' || c == '_';
            if (!valid) throw std::invalid_argument("unsafe workflow command ID");
        }
        if (command.kind != "activity" && command.kind != "timer" && command.kind != "signal")
            throw std::invalid_argument("unknown workflow command kind");
        if (command.payload_sha256.size() != 64)
            throw std::invalid_argument("workflow payload must have SHA-256 identity");
        for (char c : command.payload_sha256) {
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
                throw std::invalid_argument("noncanonical workflow payload digest");
        }
    }
    const std::size_t maximum_;
    std::vector<WorkflowCommand> committed_;
    std::unordered_set<std::string> ids_;
    std::size_t cursor_ = 0;
};
} // namespace oreslang::durable
