// Test-only bounded IPC driver, not production isolation or authentication.
#include "ores/durable/actor_sync.hpp"
#include "ores/durable/scalar_turn_v1.hpp"
#include <charconv>
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
using namespace ores::durable;
using C=BoundedCheckpointV2;
using I=std::int64_t;
static std::string hex(std::string_view b){std::string s;for(unsigned char c:b){s+="0123456789abcdef"[c>>4];s+="0123456789abcdef"[c&15];}return s;}
static std::string unhex(std::string_view s,std::size_t max){
    if(s.size()%2||s.size()/2>max)throw std::invalid_argument("invalid hex length");
    auto nibble=[](char c)->unsigned{if(c>='0'&&c<='9')return c-'0';if(c>='a'&&c<='f')return c-'a'+10;throw std::invalid_argument("invalid hex");};
    std::string b;b.reserve(s.size()/2);
    for(std::size_t i=0;i<s.size();i+=2)b.push_back(static_cast<char>(static_cast<unsigned char>((nibble(s[i])<<4)|nibble(s[i+1]))));
    return b;
}
static I number(std::string_view s){I n=0;auto r=std::from_chars(s.data(),s.data()+s.size(),n);if(r.ec!=std::errc{}||r.ptr!=s.data()+s.size()||n<=0||std::to_string(n)!=s)throw std::invalid_argument("epoch");return n;}
int main(int argc,char** argv){
    try{
        if(argc!=7)throw std::invalid_argument("expected mode db epoch request command-hex manifest-digest");
        const std::string mode=argv[1];if(mode!="activate"&&mode!="turn"&&mode!="lose"&&mode!="rollback")throw std::invalid_argument("mode");
        const SyncKey key{"t","s","Ledger","compiled-v2"};
        const auto epoch=number(argv[3]);
        const std::vector<C::Field> fields{{"count",C::Kind::Int},{"name",C::Kind::String}};
        const C codec("Ledger",fields);
        const BoundedTurnV2 turn("Ledger",fields,argv[6],codec.encode({I(0),std::string{}}));
        SqliteActorSync store(argv[2]);
        if(mode=="activate"){store.activate(key,epoch);std::cout<<"ACTIVATED\n";return 0;}
        auto guest=[](std::string before,std::string command){
            std::cout<<"GUEST "<<hex(before)<<" "<<hex(command)<<"\n"<<std::flush;
            std::string line;
            for(char c;std::cin.get(c);){if(c=='\n')return unhex(line,65536);if(line.size()>=131072)throw std::invalid_argument("guest output exceeds bound");line.push_back(c);}
            throw std::runtime_error("guest response missing");
        };
        const auto receipt=turn.run(store,key,epoch,argv[4],unhex(argv[5],4096),guest,mode=="rollback");
        if(mode=="lose"){if(kill(getpid(),SIGKILL)!=0)return 3;_exit(3);}
        std::cout<<"COMMITTED "<<receipt.revision<<" "<<hex(receipt.reply)<<" "<<(receipt.duplicate?1:0)<<"\n";
        return 0;
    }catch(const std::exception& e){std::cerr<<e.what()<<"\n";return 1;}
}
