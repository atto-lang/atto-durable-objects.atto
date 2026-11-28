#include "ores/durable/counter_checkpoint_v1.hpp"
#include <cassert>
#include <cstdint>
#include <iostream>
#include <string>
using ores::durable::CounterCheckpointV1;
template<class F> void rejects(F f) {
    bool denied=false;try{f();}catch(const std::logic_error&){denied=true;}
    assert(denied);
}
static std::string hex(std::string_view bytes) {
    const char* digits="0123456789abcdef";
    std::string s;
    for(unsigned char b:bytes){s+=digits[b>>4];s+=digits[b&15];}
    return s;
}
static void rehash(std::string& bytes) {
    std::uint64_t h=UINT64_C(0xcbf29ce484222325);
    for(unsigned char b:std::string_view(bytes).substr(0,28)){h^=b;h*=UINT64_C(0x100000001b3);}
    for(unsigned i=0;i<8;++i)bytes[28+i]=static_cast<char>(static_cast<unsigned char>(h>>(8*i)));
}
int main() {
    const auto zero=CounterCheckpointV1::encode(0);
    assert(hex(zero)=="4154544f53543031013782cc560597ec4601000100000000000000002b35b70ef92e387a");
    for(std::int64_t n=0;n<=10000;++n)
        assert(CounterCheckpointV1::decode(CounterCheckpointV1::encode(n))==n);
    assert(CounterCheckpointV1::decode(CounterCheckpointV1::encode(1000000000))==1000000000);
    rejects([]{(void)CounterCheckpointV1::encode(-1);});
    rejects([]{(void)CounterCheckpointV1::encode(1000000001);});
    for(std::size_t n=0;n<zero.size();++n)
        rejects([&]{(void)CounterCheckpointV1::decode(std::string_view(zero).substr(0,n));});
    rejects([&]{(void)CounterCheckpointV1::decode(zero+"x");});
    // Exhaustively flip each bit of the complete canonical record.
    for(std::size_t n=0;n<zero.size();++n)for(unsigned bit=0;bit<8;++bit){
        auto bad=zero;bad[n]=static_cast<char>(static_cast<unsigned char>(bad[n])^(1u<<bit));
        rejects([&]{(void)CounterCheckpointV1::decode(bad);});
    }
    // Valid recomputed checksums do not authorize a different schema,
    // format, field count, scalar type or numeric value.
    for(auto offset:{0,8,9,17,19,27}){
        auto bad=zero;bad[static_cast<std::size_t>(offset)]=static_cast<char>(0xff);rehash(bad);
        rejects([&]{(void)CounterCheckpointV1::decode(bad);});
    }
    auto negative=zero;for(unsigned i=20;i<28;++i)negative[i]=static_cast<char>(0xff);rehash(negative);
    rejects([&]{(void)CounterCheckpointV1::decode(negative);});
    rejects([]{(void)CounterCheckpointV1::decode("100");});
    std::cout<<"CANONICAL_COUNTER_CHECKPOINT_PASS roundtrips=10002 bitflips=288\n";
}
