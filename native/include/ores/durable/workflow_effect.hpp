#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace ores::durable {
// Canonical host reference for serializable workflow effect commands.
// No function pointers, guest closures, callbacks or object capabilities.
enum class WorkflowEffectKind : int { activity=1, timer=2, child=3, cancel=4 };

struct WorkflowEffect final {
    std::int64_t sequence;
    WorkflowEffectKind kind;
    std::string name;
    std::string payload;

    std::string canonical() const {
        if (sequence <= 0 || sequence > 4096)
            throw std::invalid_argument("workflow effect sequence outside quota");
        const int tag=static_cast<int>(kind);
        if (tag<1 || tag>4)
            throw std::invalid_argument("unsupported typed workflow effect");
        if (name.empty() || name.size()>128 || payload.size()>4096)
            throw std::length_error("workflow effect name/payload exceeds quota");
        for (unsigned char c : name) {
            if (!((c>='a'&&c<='z')||(c>='A'&&c<='Z')||
                  (c>='0'&&c<='9')||c=='-'||c=='_'))
                throw std::invalid_argument("workflow effect name is not canonical");
        }
        // Length framing makes embedded separators and binary payloads
        // unambiguous; identical effects have byte-identical replay keys.
        return "workflow.effect/1|" + std::to_string(sequence) + "|" +
            std::to_string(tag) + "|" + std::to_string(name.size()) + ":" + name +
            "|" + std::to_string(payload.size()) + ":" + payload;
    }
};
} // namespace ores::durable
