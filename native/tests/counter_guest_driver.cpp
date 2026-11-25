// Test-only host driver. Never expose this CLI to a remote caller.
// Trusted fixture selects identity/epoch; no production auth or scheduler.
// The process owns the entire turn and waits on a test-only pipe.
#include "ores/durable/counter_turn.hpp"
#include <charconv>
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>

using namespace ores::durable;
using I = std::int64_t;
static I number(std::string_view s) {
    if (s.empty() || s.size()>20) throw std::invalid_argument("invalid number");
    I n=0;
    auto r=std::from_chars(s.data(),s.data()+s.size(),n);
    if (r.ec!=std::errc{} || r.ptr!=s.data()+s.size() || std::to_string(n)!=s)
        throw std::invalid_argument("noncanonical number");
    return n;
}
int main(int argc,char** argv) {
    try {
        if (argc!=10) throw std::invalid_argument("expected nine arguments");
        const std::string mode=argv[1];
        if (mode!="activate" && mode!="turn" && mode!="lose" && mode!="rollback")
            throw std::invalid_argument("invalid mode");
        const SyncKey key{argv[3],"counters","Counter",argv[4]};
        const auto epoch=number(argv[5]);
        const auto op=number(argv[7]);
        const auto delta=number(argv[8]);
        if (std::string_view(argv[9])!="fixture-v1") throw std::invalid_argument("fixture protocol");
        SqliteActorSync store(argv[2]);
        if (mode=="activate") {
            store.activate(key,epoch);
            std::cout<<"ACTIVATED\n"<<std::flush;
            return 0;
        }
        const auto run_guest=[](I committed,I operation,I amount)->I {
            std::cout<<"GUEST "<<committed<<" "<<operation<<" "<<amount<<"\n"<<std::flush;
            // Read a bounded line without allocating arbitrarily large input.
            std::string line;
            for (char c; std::cin.get(c);) {
                if (c=='\n') return number(line);
                if (line.size()==20) throw std::invalid_argument("guest result too long");
                line.push_back(c);
            }
            throw std::runtime_error("guest result missing");
        };
        auto result=CounterTurn::run(store,key,epoch,argv[6],op,delta,run_guest,mode=="rollback");
        if (!result) {
            std::cout<<"REJECTED\n"<<std::flush;
            return 0;
        }
        // Simulate lost response only after the authoritative store commits.
        if (mode=="lose") {
            // Hard kill after COMMIT, before any application reply or cleanup.
            if (::kill(::getpid(), SIGKILL)!=0) return 3;
            _exit(3); // unreachable if the signal was delivered
        }
        std::cout<<"COMMITTED "<<result->revision<<" "<<result->reply<<" "
                 <<(result->duplicate?1:0)<<"\n"<<std::flush;
        return 0;
    } catch(const std::exception& e) {
        std::cerr<<e.what()<<"\n";
        return 1;
    }
}
