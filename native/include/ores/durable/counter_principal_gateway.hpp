#pragma once
// Trusted ingress adapter. The authentication boundary supplies a stable
// principal ID; this class does not authenticate passwords or network tokens.
#include "ores/durable/counter_signed_deployment.hpp"

namespace ores::durable {
class CounterPrincipalGateway final {
public:
    using Clock=CounterDirectory::Clock;
    using Policy=std::function<bool(std::string_view,const SyncKey&,std::int64_t)>;
private:
    struct Authority {
        const Policy policy;
        explicit Authority(Policy p):policy(std::move(p)) {
            if(!policy) throw std::invalid_argument("missing principal policy");
        }
    };
    const std::shared_ptr<const Authority> authority_;
    const std::unique_ptr<CounterDirectory> directory_;
    static void label(std::string_view value) {
        if(value.empty() || value.size()>256 || value.find('\0')!=std::string_view::npos)
            throw std::invalid_argument("invalid principal or request identity");
    }
    static void append(std::string& out,std::string_view value) {
        out+=std::to_string(value.size());out+=':';out.append(value.data(),value.size());
    }
    static std::string operation_id(std::string_view principal,std::string_view request) {
        label(principal);label(request);
        std::string framed="atto.counter.principal-request.v1:";
        append(framed,principal);append(framed,request);
        std::array<unsigned char,32> digest{};
        unsigned int count=0;
        if(EVP_Digest(framed.data(),framed.size(),digest.data(),&count,EVP_sha256(),nullptr)!=1 ||
           count!=digest.size()) throw std::runtime_error("request identity hash failed");
        std::string encoded="@principal:";
        const char* hex="0123456789abcdef";
        for(auto byte:digest){encoded+=hex[byte>>4];encoded+=hex[byte&15];}
        return encoded;
    }
public:
    // Opaque in-process capability, never deserialized from guest or network.
    // Copying grants the same bounded authority; issuance is trusted host only.
    class Capability final {
        friend class CounterPrincipalGateway;
        std::weak_ptr<const Authority> issuer_;
        std::string principal_;
        SyncKey key_;
        std::int64_t op_;
        Clock::time_point expires_;
        Capability(const std::shared_ptr<const Authority>& issuer,std::string principal,
                   SyncKey key,std::int64_t op,Clock::time_point expires)
            :issuer_(issuer),principal_(std::move(principal)),key_(std::move(key)),
             op_(op),expires_(expires){}
    public:
        Capability(const Capability&)=default;
        Capability& operator=(const Capability&)=default;
    };
    CounterPrincipalGateway(const SignedCounterDeployment& deployment,const std::string& db,
                            CounterDirectory::Authorize scope,Policy policy,
                            std::size_t capacity=128,std::size_t active=128,std::size_t workers=1)
        :authority_(std::make_shared<const Authority>(std::move(policy))),
         directory_(deployment.open(db,std::move(scope),capacity,active,workers,
             Clock::duration::zero(),Clock::duration::zero(),Clock::duration::zero(),true)) {}
    // Caller MUST authenticate the principal upstream. Exact object and
    // operation are fixed at issue time; no ambient tenant supplied on send.
    Capability issue(std::string principal,SyncKey key,std::int64_t op,
                     std::chrono::milliseconds lifetime) const {
        label(principal);label(key.tenant);label(key.name_space);label(key.actor_type);label(key.object_id);
        if((op!=1 && op!=2) || lifetime.count()<=0 || lifetime>std::chrono::minutes(5))
            throw std::invalid_argument("invalid capability grant");
        if(!authority_->policy(principal,key,op)) throw std::logic_error("principal not authorized");
        return Capability(authority_,std::move(principal),std::move(key),op,Clock::now()+lifetime);
    }
    std::future<CounterDirectory::Reply> send(const Capability& capability,std::string request,
                                               std::int64_t delta) {
        const auto issuer=capability.issuer_.lock();
        if(!issuer || issuer!=authority_) throw std::logic_error("foreign capability");
        const auto auth=[issuer,capability]{
            if(Clock::now()>=capability.expires_ ||
               !issuer->policy(capability.principal_,capability.key_,capability.op_))
                throw std::logic_error("principal capability expired or revoked");
        };
        auth();
        // Stable request namespace across gateway restart. A fresh grant for
        // the same principal can resolve its prior committed operation.
        const auto id=operation_id(capability.principal_,request);
        return directory_->send_authorized(capability.key_,id,capability.op_,delta,auth);
    }
};
} // namespace ores::durable
