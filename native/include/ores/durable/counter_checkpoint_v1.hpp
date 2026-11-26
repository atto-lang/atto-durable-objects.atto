#pragma once
#include "ores/durable/scalar_checkpoint_v1.hpp"

namespace ores::durable {
// Keep the original counter-specific domain and database schema label;
// use the shared ordered scalar codec without changing on-disk bytes.
class CounterCheckpointV1 final {
    static const ScalarCheckpointV1& codec() {
        static const ScalarCheckpointV1 value("Counter",{{"count",ScalarCheckpointV1::Kind::Int}});
        if(value.fingerprint()!=fingerprint)throw std::logic_error("counter schema drift");
        return value;
    }
public:
    static constexpr std::int64_t maximum=1000000000;
    static constexpr std::uint64_t fingerprint=UINT64_C(0x46ec970556cc8237);
    static constexpr std::string_view schema="atto.counter.atto-st01.46ec970556cc8237";
    static std::string encode(std::int64_t n) {
        if(n<0||n>maximum)throw std::logic_error("counter checkpoint outside domain");
        return codec().encode({n});
    }
    static std::int64_t decode(std::string_view bytes) {
        const auto n=std::get<std::int64_t>(codec().decode(bytes).at(0));
        if(n<0||n>maximum)throw std::logic_error("counter checkpoint outside domain");
        return n;
    }
};
} // namespace ores::durable
