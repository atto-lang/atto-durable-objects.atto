#include "ores/durable/bounded_checkpoint_v2.hpp"
#include <cassert>
#include <iostream>
using C=ores::durable::BoundedCheckpointV2;
using K=C::Kind;
using I=std::int64_t;
template<class F> void rejects(F f){bool denied=false;try{f();}catch(const std::logic_error&){denied=true;}assert(denied);}
std::string hex(std::string_view b){std::string out;for(unsigned char c:b){out+="0123456789abcdef"[c>>4];out+="0123456789abcdef"[c&15];}return out;}
void rehash(std::string& b){std::uint64_t h=UINT64_C(0xcbf29ce484222325);for(unsigned char c:std::string_view(b).substr(0,b.size()-8)){h^=c;h*=UINT64_C(0x100000001b3);}for(unsigned i=0;i<8;++i)b[b.size()-8+i]=static_cast<char>(static_cast<unsigned char>(h>>(8*i)));}
int main(){
 const C codec("Ledger",{{"count",K::Int},{"open",K::Bool},{"name",K::String},{"payload",K::Bytes}});
 const std::vector<C::Value> v{std::numeric_limits<I>::min(),true,std::string("A\0\xc3\xa9\xf0\x9f\x98\x80",8),C::Bytes{0,255,128}};
 const auto bytes=codec.encode(v);assert(codec.fingerprint()==UINT64_C(0xdb14dea8243c195b));
 assert(hex(bytes)=="4154544f53543032025b193c24a8de14db0400010000000000000080020103080000004100c3a9f09f9880040300000000ff8074c4cc420e041ac0");assert(codec.decode(bytes)==v);
 for(I i=-5000;i<=5000;++i){std::vector<C::Value> x{i,i%2==0,std::string(static_cast<std::size_t>((i+5000)%257),'a'),C::Bytes{static_cast<unsigned char>(i),0,255}};assert(codec.decode(codec.encode(x))==x);}
 for(std::size_t i=0;i<bytes.size();++i){rejects([&]{(void)codec.decode(std::string_view(bytes).substr(0,i));});for(unsigned bit=0;bit<8;++bit){auto bad=bytes;bad[i]=static_cast<char>(static_cast<unsigned char>(bad[i])^(1u<<bit));rejects([&]{(void)codec.decode(bad);});}}
 auto bad=bytes;bad[8]=1;rehash(bad);rejects([&]{(void)codec.decode(bad);});
 bad=bytes;bad[29]=2;rehash(bad);rejects([&]{(void)codec.decode(bad);});
 bad=bytes;bad[31]=static_cast<char>(255);bad[32]=static_cast<char>(255);bad[33]=static_cast<char>(255);bad[34]=static_cast<char>(255);rehash(bad);rejects([&]{(void)codec.decode(bad);});
 bad=bytes;bad[37]=static_cast<char>(0xc0);rehash(bad);rejects([&]{(void)codec.decode(bad);});
 bad=bytes;bad.insert(bad.size()-8,1,'x');rehash(bad);rejects([&]{(void)codec.decode(bad);});
 for(auto s:{std::string("\xc0\x80",2),std::string("\xed\xa0\x80",3),std::string("\xf4\x90\x80\x80",4),std::string("\xe2\x82",2),std::string("\x80",1),std::string("\xe0\x80\x80",3),std::string("\xf0\x80\x80\x80",4)})rejects([&]{(void)codec.encode({I(0),false,s,C::Bytes{}});});
 assert(codec.decode(codec.encode({I(0),false,std::string(16384,'x'),C::Bytes(16384,255)})).size()==4);
 rejects([&]{(void)codec.encode({I(0),false,std::string(16385,'x'),C::Bytes{}});});rejects([&]{(void)codec.encode({I(0),false,std::string{},C::Bytes(16385)});});
 const C large("Large",{{"a",K::Bytes},{"b",K::Bytes},{"c",K::Bytes},{"d",K::Bytes}});
 auto full=large.encode({C::Bytes(16384),C::Bytes(16384),C::Bytes(16384),C::Bytes(16337)});assert(full.size()==65536);assert(large.encode(large.decode(full))==full);
 rejects([&]{(void)large.encode({C::Bytes(16384),C::Bytes(16384),C::Bytes(16384),C::Bytes(16338)});});
 rejects([&]{(void)large.decode(std::string(65537,'x'));});
 const C wrong("Other",{{"count",K::Int},{"open",K::Bool},{"name",K::String},{"payload",K::Bytes}});rejects([&]{(void)wrong.decode(bytes);});
 rejects([&]{(void)codec.encode({I(0),false,C::Bytes{},std::string{}});});
 std::vector<C::Field> fields;std::vector<C::Value> vals;for(int i=0;i<256;++i){fields.push_back({"f"+std::to_string(i),K::Bool});vals.emplace_back(true);}const C many("Many",fields);assert(many.decode(many.encode(vals))==vals);fields.push_back({"extra",K::Bool});rejects([&]{(void)C("Many",fields);});
 const C empty("Empty",{});assert(empty.encode({}).size()==27&&empty.decode(empty.encode({})).empty());
 std::cout<<"BOUNDED_CHECKPOINT_V2_PASS roundtrips=10001 bitflips="<<bytes.size()*8<<" utf8_rejections=7 max_snapshot=65536 max_field=16384 max_fields=256\n";
}
