#pragma once
#include "ores/durable/scalar_checkpoint_v1.hpp"
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace ores::durable {
// Trusted broker adapter, not a guest API. Caller authenticates the logical
// schema manifest and artifact, owns a non-reentrant turn permit and invokes
// a bounded isolated worker. No guest callback runs under a SQLite lock.
// This initial lane has a checkpoint-valued reply; no mailbox/alarm effects.
class ScalarTurnV1 final {
    const ScalarCheckpointV1 codec_;
    const std::string actor_, schema_, initial_;
    static std::string label(std::string_view digest,std::uint64_t fingerprint) {
        if(digest.size()!=64)throw std::invalid_argument("invalid trusted schema manifest digest");
        for(char c:digest)if(!((c>='0'&&c<='9')||(c>='a'&&c<='f')))
            throw std::invalid_argument("invalid trusted schema manifest digest");
        std::string out="atto.scalar.v1.";out+=digest;out+='.';
        for(int i=60;i>=0;i-=4)out+="0123456789abcdef"[(fingerprint>>i)&15];
        return out;
    }
    void validate(std::string_view bytes) const { (void)codec_.decode(bytes); }
public:
    ScalarTurnV1(std::string actor,std::vector<ScalarCheckpointV1::Field> fields,
                 std::string_view manifest_digest,std::string initial)
        :codec_(actor,std::move(fields)),actor_(std::move(actor)),
         schema_(label(manifest_digest,codec_.fingerprint())),initial_(std::move(initial)) {
        validate(initial_);
    }
    std::string_view schema() const noexcept {return schema_;}
    template<class Store,class Key,class Guest>
    auto run(Store& store,const Key& key,std::int64_t epoch,std::string_view request_id,
             std::string_view command,Guest&& guest,bool inject_precommit_failure=false) const {
        if(key.actor_type!=actor_||epoch<=0||request_id.empty()||request_id.size()>256||
           request_id.find('\0')!=std::string_view::npos||request_id.substr(0,1)=="@"||
           command.empty()||command.size()>4096)
            throw std::invalid_argument("invalid scalar turn envelope");
        const std::string id(request_id),owned_command(command);
        const auto before=store.snapshot(key);
        if(before.epoch!=epoch||before.revision<0)throw std::logic_error("stale scalar turn");
        if(before.revision==0) {
            if(!before.schema.empty()||!before.bytes.empty())throw std::logic_error("invalid initial checkpoint");
        }else{
            if(before.schema!=schema_)throw std::logic_error("scalar schema migration required");
            validate(before.bytes);
        }
        if(auto prior=store.lookup_receipt(key,epoch,id,owned_command,schema_,nullptr,&before)) {
            if(!prior->duplicate||prior->revision<=0||prior->revision>before.revision)
                throw std::logic_error("invalid scalar receipt revision");
            validate(prior->reply);
            if(prior->revision==before.revision&&prior->reply!=before.bytes)
                throw std::logic_error("scalar receipt conflicts with checkpoint");
            return *prior;
        }
        if(before.revision==std::numeric_limits<std::int64_t>::max())throw std::overflow_error("scalar revision exhausted");
        // Copies form the only guest-facing inputs. A guest may propose a
        // valid state but cannot select its identity, schema or commit policy.
        const std::string proposed=guest(std::string(before.revision==0?initial_:before.bytes),std::string(owned_command));
        validate(proposed);
        const auto receipt=store.commit(key,epoch,before.revision,id,owned_command,schema_,
            proposed,proposed,inject_precommit_failure,nullptr,0,nullptr,nullptr,&before);
        if(receipt.duplicate||receipt.revision!=before.revision+1||receipt.reply!=proposed)
            throw std::logic_error("unexpected scalar commit receipt");
        return receipt;
    }
};
} // namespace ores::durable
