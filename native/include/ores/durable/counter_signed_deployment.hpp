#pragma once
// Host-only Ed25519 deployment verification; never expose key selection,
// manifest creation, or OpenSSL to the guest. Trust roots are provisioned
// by the operator, not supplied by an untrusted network request.
#include "ores/durable/counter_artifact_registry.hpp"
#include <array>
#include <memory>
#include <openssl/evp.h>

namespace ores::durable {
class SignedCounterDeployment final {
public:
    using PublicKey=std::array<unsigned char,32>;
    using Signature=std::array<unsigned char,64>;
private:
    const CounterArtifactRegistry registry_;
    static void verify(std::string_view manifest,const PublicKey& key,const Signature& sig) {
        using Key=std::unique_ptr<EVP_PKEY,decltype(&EVP_PKEY_free)>;
        using Context=std::unique_ptr<EVP_MD_CTX,decltype(&EVP_MD_CTX_free)>;
        Key trusted(EVP_PKEY_new_raw_public_key(EVP_PKEY_ED25519,nullptr,key.data(),key.size()),EVP_PKEY_free);
        Context ctx(EVP_MD_CTX_new(),EVP_MD_CTX_free);
        if(!trusted || !ctx || EVP_DigestVerifyInit(ctx.get(),nullptr,nullptr,nullptr,trusted.get())!=1 ||
           EVP_DigestVerify(ctx.get(),sig.data(),sig.size(),
                reinterpret_cast<const unsigned char*>(manifest.data()),manifest.size())!=1)
            throw std::logic_error("deployment signature verification failed");
    }
public:
    SignedCounterDeployment(CounterArtifactRegistry registry,const PublicKey& trusted_key,
                            const Signature& signature)
        :registry_(std::move(registry)) {
        verify(registry_.manifest(),trusted_key,signature);
    }
    // The trusted ingress authorizer must authenticate principals and check
    // operation authority independently. A signed deployment is not a login.
    std::unique_ptr<CounterDirectory> open(const std::string& db,CounterDirectory::Authorize authorize,
            std::size_t capacity=128,std::size_t active_limit=128,std::size_t workers=1,
            CounterDirectory::Clock::duration idle=CounterDirectory::Clock::duration::zero(),
            CounterDirectory::Clock::duration alarms=CounterDirectory::Clock::duration::zero(),
            CounterDirectory::Clock::duration mail=CounterDirectory::Clock::duration::zero(),
            bool principal_mode=false) const {
        if(!authorize) throw std::invalid_argument("missing trusted principal authorizer");
        auto admission=[registry=registry_,authorize=std::move(authorize)](const SyncKey& key) {
            return registry.contains(key) && authorize(key);
        };
        return std::make_unique<CounterDirectory>(db,CounterDirectory::KeyedGuest(registry_),
            std::move(admission),capacity,active_limit,workers,idle,alarms,mail,registry_.manifest(),principal_mode);
    }
};
} // namespace ores::durable
