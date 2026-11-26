#pragma once
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <utility>

namespace ores::durable {
// Host codec for an already authenticated compile-time scalar schema.
// This is not a heap walker, graph serializer, or guest authority.
class ScalarCheckpointV1 final {
public:
    enum class Kind : unsigned char { Int=1, Bool=2 };
    struct Field { std::string name; Kind kind; };
    using Value=std::variant<std::int64_t,bool>;
private:
    std::string actor_;
    std::vector<Field> fields_;
    std::uint64_t fingerprint_=0;
    std::size_t size_=27;
    static void deny(){throw std::logic_error("invalid scalar checkpoint or schema");}
    static bool name_ok(std::string_view n) {
        if(n.empty()||n.size()>128)return false;
        auto alpha=[](unsigned char c){return (c>='A'&&c<='Z')||(c>='a'&&c<='z')||c=='_';};
        if(!alpha(n.front()))return false;
        for(unsigned char c:n)if(!alpha(c)&&!(c>='0'&&c<='9'))return false;
        return true;
    }
    static void put(std::string& out,std::uint64_t n,unsigned width) {
        for(unsigned i=0;i<width;++i)out.push_back(static_cast<char>(static_cast<unsigned char>(n>>(8*i))));
    }
    static std::uint64_t get(std::string_view s,std::size_t p,unsigned width) {
        if(p>s.size()||width>s.size()-p)deny();
        std::uint64_t n=0;
        for(unsigned i=0;i<width;++i)n|=std::uint64_t(static_cast<unsigned char>(s[p+i]))<<(8*i);
        return n;
    }
    static std::uint64_t hash(std::string_view s) {
        std::uint64_t h=UINT64_C(0xcbf29ce484222325);
        for(unsigned char b:s){h^=b;h*=UINT64_C(0x100000001b3);}
        return h;
    }
    static std::int64_t signed_value(std::uint64_t u) {
        if(u<=static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
            return static_cast<std::int64_t>(u);
        return -1-static_cast<std::int64_t>(~u);
    }
public:
    ScalarCheckpointV1(std::string actor,std::vector<Field> fields)
        :actor_(std::move(actor)),fields_(std::move(fields)) {
        if(!name_ok(actor_)||fields_.size()>256)deny();
        std::string sig="atto.actor.state.schema.v1";
        sig.push_back(0);put(sig,actor_.size(),4);sig+=actor_;
        for(std::size_t i=0;i<fields_.size();++i){
            const auto& f=fields_[i];
            if(!name_ok(f.name)||(f.kind!=Kind::Int&&f.kind!=Kind::Bool))deny();
            for(std::size_t j=0;j<i;++j)if(fields_[j].name==f.name)deny();
            put(sig,f.name.size(),4);sig+=f.name;sig.push_back(static_cast<char>(f.kind));
            size_+=f.kind==Kind::Int?9:2;
        }
        if(size_>4096)deny();
        fingerprint_=hash(sig);
    }
    std::uint64_t fingerprint() const noexcept {return fingerprint_;}
    std::string encode(const std::vector<Value>& values) const {
        if(values.size()!=fields_.size())deny();
        std::string out="ATTOST01";
        out.reserve(size_);
        out.push_back(1);put(out,fingerprint_,8);put(out,fields_.size(),2);
        for(std::size_t i=0;i<fields_.size();++i){
            auto kind=fields_[i].kind;
            out.push_back(static_cast<char>(kind));
            if(kind==Kind::Int){
                const auto* n=std::get_if<std::int64_t>(&values[i]);
                if(!n)deny();
                put(out,static_cast<std::uint64_t>(*n),8);
            }else{
                const auto* b=std::get_if<bool>(&values[i]);
                if(!b)deny();
                out.push_back(*b?1:0);
            }
        }
        put(out,hash(out),8);
        return out;
    }
    std::vector<Value> decode(std::string_view bytes) const {
        if(bytes.size()!=size_||bytes.substr(0,8)!="ATTOST01"||bytes[8]!=1||
           get(bytes,9,8)!=fingerprint_||get(bytes,17,2)!=fields_.size()||
           get(bytes,size_-8,8)!=hash(bytes.substr(0,size_-8)))deny();
        std::size_t cursor=19;
        std::vector<Value> out;out.reserve(fields_.size());
        for(const auto& f:fields_){
            if(static_cast<unsigned char>(bytes[cursor++])!=static_cast<unsigned char>(f.kind))deny();
            if(f.kind==Kind::Int){out.emplace_back(signed_value(get(bytes,cursor,8)));cursor+=8;}
            else{
                unsigned char b=bytes[cursor++];
                if(b>1)deny();
                out.emplace_back(b==1);
            }
        }
        if(cursor!=size_-8)deny();
        return out;
    }
};
} // namespace ores::durable
