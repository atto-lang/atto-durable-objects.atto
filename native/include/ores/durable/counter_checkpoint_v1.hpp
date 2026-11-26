#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

namespace ores::durable {
// Canonical restricted Counter.count descriptor. No raw object addresses,
// native layout, host endianness, or implicit legacy-format conversion.
class CounterCheckpointV1 final {
    static std::uint64_t hash(std::string_view bytes) noexcept {
        std::uint64_t h=UINT64_C(0xcbf29ce484222325);
        for(unsigned char b:bytes){h^=b;h*=UINT64_C(0x100000001b3);}
        return h;
    }
    static void put(std::string& out,std::uint64_t n,unsigned width) {
        for(unsigned i=0;i<width;++i) out.push_back(static_cast<char>(static_cast<unsigned char>(n>>(8*i))));
    }
    static std::uint64_t get(std::string_view bytes,std::size_t offset,unsigned width) noexcept {
        std::uint64_t n=0;
        for(unsigned i=0;i<width;++i)n|=std::uint64_t(static_cast<unsigned char>(bytes[offset+i]))<<(8*i);
        return n;
    }
public:
    static constexpr std::int64_t maximum=1000000000;
    static constexpr std::uint64_t fingerprint=UINT64_C(0x46ec970556cc8237);
    static constexpr std::string_view schema="atto.counter.atto-st01.46ec970556cc8237";
    static std::string encode(std::int64_t n) {
        if(n<0||n>maximum)throw std::logic_error("counter checkpoint outside domain");
        std::string out="ATTOST01";
        out.push_back(1);put(out,fingerprint,8);put(out,1,2);out.push_back(1);
        put(out,static_cast<std::uint64_t>(n),8);put(out,hash(out),8);
        return out;
    }
    static std::int64_t decode(std::string_view bytes) {
        if(bytes.size()!=36||bytes.substr(0,8)!="ATTOST01"||bytes[8]!=1||
           get(bytes,9,8)!=fingerprint||get(bytes,17,2)!=1||bytes[19]!=1||
           get(bytes,28,8)!=hash(bytes.substr(0,28)))
            throw std::logic_error("invalid canonical counter checkpoint");
        const auto n=get(bytes,20,8);
        if(n>static_cast<std::uint64_t>(maximum))throw std::logic_error("counter checkpoint outside domain");
        return static_cast<std::int64_t>(n);
    }
};
} // namespace ores::durable
