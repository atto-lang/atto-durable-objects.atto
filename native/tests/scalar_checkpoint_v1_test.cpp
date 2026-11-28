#include "ores/durable/scalar_checkpoint_v1.hpp"
#include "ores/durable/counter_checkpoint_v1.hpp"
#include <cassert>
#include <iostream>
#include <limits>
#include <type_traits>
using ores::durable::ScalarCheckpointV1;
using ores::durable::CounterCheckpointV1;
using K=ScalarCheckpointV1::Kind;
using I=std::int64_t;
using V=ScalarCheckpointV1::Value;
template<class F> void rejects(F f){
    bool denied=false;try{f();}catch(const std::logic_error&){denied=true;}
    assert(denied);
}
static std::string hex(std::string_view bytes){
    std::string s;
    for(unsigned char b:bytes){s+="0123456789abcdef"[b>>4];s+="0123456789abcdef"[b&15];}
    return s;
}
static void rehash(std::string& bytes){
    std::uint64_t h=UINT64_C(0xcbf29ce484222325);
    for(unsigned char b:std::string_view(bytes).substr(0,bytes.size()-8)){h^=b;h*=UINT64_C(0x100000001b3);}
    for(unsigned i=0;i<8;++i)bytes[bytes.size()-8+i]=static_cast<char>(static_cast<unsigned char>(h>>(8*i)));
}
int main(){
    static_assert(!std::is_move_constructible_v<ScalarCheckpointV1>);
    static_assert(!std::is_copy_assignable_v<ScalarCheckpointV1>);
    const ScalarCheckpointV1 schema("Ledger",{{"balance",K::Int},{"open",K::Bool},{"last",K::Int}});
    const std::vector<V> limits{std::numeric_limits<I>::min(),true,std::numeric_limits<I>::max()};
    const auto bytes=schema.encode(limits);
    assert(schema.fingerprint()==UINT64_C(0x5974544af0efb8f9));
    assert(hex(bytes)=="4154544f5354303101f9b8eff04a5474590300010000000000000080020101ffffffffffffff7f4191e1156e4df645");
    assert(schema.decode(bytes)==limits);
    assert(ScalarCheckpointV1(schema).decode(bytes)==limits);
    assert(hex(CounterCheckpointV1::encode(0))=="4154544f53543031013782cc560597ec4601000100000000000000002b35b70ef92e387a");
    for(I i=-5000;i<=5000;++i){
        std::vector<V> v{i,i%2==0,-i};
        assert(schema.decode(schema.encode(v))==v);
    }
    for(std::size_t n=0;n<bytes.size();++n){
        rejects([&]{(void)schema.decode(std::string_view(bytes).substr(0,n));});
        for(unsigned bit=0;bit<8;++bit){
            auto bad=bytes;bad[n]=static_cast<char>(static_cast<unsigned char>(bad[n])^(1u<<bit));
            rejects([&]{(void)schema.decode(bad);});
        }
    }
    auto bad=bytes;bad[29]=2;rehash(bad);
    rejects([&]{(void)schema.decode(bad);});
    bad=bytes;bad[28]=1;rehash(bad);
    rejects([&]{(void)schema.decode(bad);});
    rejects([&]{(void)schema.decode(bytes+"x");});
    rejects([&]{(void)schema.encode({I(0),I(1),I(2)});});
    rejects([&]{(void)schema.encode({I(0),true});});
    rejects([&]{(void)ScalarCheckpointV1("Ledger",{{"balance",K::Int},{"balance",K::Bool}});});
    rejects([&]{(void)ScalarCheckpointV1("Ledger",{{"bad name",K::Int}});});
    rejects([&]{(void)ScalarCheckpointV1("Ledger",{{"a",static_cast<K>(9)}});});
    rejects([&]{(void)ScalarCheckpointV1("Other",{{"balance",K::Int},{"open",K::Bool},{"last",K::Int}}).decode(bytes);});
    rejects([&]{(void)ScalarCheckpointV1("Ledger",{{"last",K::Int},{"open",K::Bool},{"balance",K::Int}}).decode(bytes);});
    // Four immutable vectors from atto-platform-conformance's existing
    // contracts/actor-persistence-scalar-wire-v1.json (blob 863a9774e8c355abfd9d8f1258d5f5d53bc96434).
    const ScalarCheckpointV1 shared("Counter",{{"count",K::Int},{"enabled",K::Bool}});
    assert(hex(shared.encode({I(100),true}))=="4154544f5354303101c9a1af17fc8684bd020001640000000000000002018fd7fcf2f2744e29");
    assert(hex(shared.encode({std::numeric_limits<I>::min(),false}))=="4154544f5354303101c9a1af17fc8684bd0200010000000000000080020058ebbfc357901b30");
    assert(hex(ScalarCheckpointV1("EmptyActor",{}).encode({}))=="4154544f5354303101c3fbc65066197bff0000b3c015f0f5029da5");
    assert(hex(ScalarCheckpointV1("OtherCounter",{{"count",K::Int}}).encode({I(100)}))=="4154544f53543031010201d7f4b9c36b6c0100016400000000000000eb0202552d636527");
    const ScalarCheckpointV1 empty("Empty",{});
    assert(empty.encode({}).size()==27&&empty.decode(empty.encode({})).empty());
    std::vector<ScalarCheckpointV1::Field> fields;
    std::vector<V> values;
    for(int i=0;i<256;++i){fields.push_back({"f"+std::to_string(i),K::Int});values.emplace_back(I(i));}
    const ScalarCheckpointV1 maximum("Maximum",fields);
    assert(maximum.encode(values).size()==2331&&maximum.decode(maximum.encode(values))==values);
    fields.push_back({"extra",K::Bool});
    rejects([&]{(void)ScalarCheckpointV1("Maximum",fields);});
    std::cout<<"SCALAR_CHECKPOINT_V1_PASS roundtrips=10001 bitflips="<<bytes.size()*8<<" max_fields=256 shared_golden=4\n";
}
