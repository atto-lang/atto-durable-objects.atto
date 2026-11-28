#include "ores/durable/bounded_checkpoint_v2.hpp"
#include <cassert>
#include <iostream>
using C=ores::durable::BoundedCheckpointV2;
using K=C::Kind;
static const C codec("Text",{{"value",K::String}});
static void rehash(std::string& b){
    std::uint64_t h=UINT64_C(0xcbf29ce484222325);
    for(unsigned char c:std::string_view(b).substr(0,b.size()-8)){h^=c;h*=UINT64_C(0x100000001b3);}
    for(unsigned i=0;i<8;++i)b[b.size()-8+i]=static_cast<char>(static_cast<unsigned char>(h>>(8*i)));
}
static void check(const std::string& raw,bool expected){
    bool accepted=false;
    try {const auto b=codec.encode({raw});assert(codec.decode(b)==std::vector<C::Value>{raw});accepted=true;}catch(const std::logic_error&){}
    assert(accepted==expected);
    auto b=codec.encode({std::string(raw.size(),'a')});
    b.replace(24,raw.size(),raw);rehash(b);
    accepted=false;
    try {assert(codec.decode(b)==std::vector<C::Value>{raw});accepted=true;}catch(const std::logic_error&){}
    assert(accepted==expected);
}
int main(){
    unsigned valid=0,invalid=0;
    for(unsigned a=0;a<256;++a){
        bool ok=a<128;check(std::string(1,static_cast<char>(a)),ok);ok?++valid:++invalid;
        for(unsigned b=0;b<256;++b){
            ok=(a<128&&b<128)||(a>=0xc2&&a<=0xdf&&b>=0x80&&b<=0xbf);
            check(std::string{static_cast<char>(a),static_cast<char>(b)},ok);ok?++valid:++invalid;
        }
    }
    for(const auto& s:{std::string("\xe0\xa0\x80",3),std::string("\xed\x9f\xbf",3),std::string("\xee\x80\x80",3),std::string("\xef\xbb\xbf",3),std::string("\xf0\x90\x80\x80",4),std::string("\xf4\x8f\xbf\xbf",4)})check(s,true);
    for(const auto& s:{std::string("\xe0\x9f\xbf",3),std::string("\xed\xa0\x80",3),std::string("\xf0\x8f\xbf\xbf",4),std::string("\xf4\x90\x80\x80",4),std::string("\xf5\x80\x80\x80",4),std::string("\xe1\x80",2),std::string("\xf1\x80\x80",3)})check(s,false);
    assert(valid+invalid==65792);
    std::cout<<"BOUNDED_V2_UTF8_ADVERSARIAL_PASS exhaustive="<<valid+invalid<<" valid="<<valid<<" invalid="<<invalid<<" additional_boundary_cases=13 encode_and_rehashed_decode=1\n";
}
