#pragma once
// Host-only Ed25519 deployment verification; never expose key selection,
// manifest creation, or OpenSSL to the guest. Trust roots are provisioned
// by the operator, not supplied by an untrusted network request.
#include "ores/durable/counter_artifact_registry.hpp"
#include <array>
#include <memory>
#include <openssl/evp.h>

namespace ores::durable {
// Aggregate reservation envelope for the fixed trusted Linux fuel-worker
// profile. This is concurrent worst-case capacity, not actual measured RSS,
// CPU time per window, or a cgroup. Each turn reserves one full slot even
// if it ends before guest invocation. Never accepted from guest input.
struct CounterFuelTenantBudget {
    std::size_t queued=32;
    std::uint64_t address_space_bytes=512ull*1024*1024;
    std::uint64_t in_flight_fuel=100000;
    std::uint64_t in_flight_cpu_seconds=4;
    CounterTenantLimits limits() const {
        constexpr std::uint64_t worker_as=256ull*1024*1024,worker_fuel=50000,worker_cpu=2;
        const auto slots=std::min({address_space_bytes/worker_as,in_flight_fuel/worker_fuel,
                                   in_flight_cpu_seconds/worker_cpu,std::uint64_t{64}});
        if(!queued || !slots) throw std::invalid_argument("tenant budget cannot reserve one fuel worker");
        return {queued,static_cast<std::size_t>(slots)};
    }
};
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
            bool principal_mode=false,CounterDirectory::PrincipalPolicy mail_policy={},bool transactional_policy=false,
            CounterFuelTenantBudget tenant_budget={}) const {
        if(!authorize) throw std::invalid_argument("missing trusted principal authorizer");
        auto admission=[registry=registry_,authorize=std::move(authorize)](const SyncKey& key) {
            return registry.contains(key) && authorize(key);
        };
        return std::make_unique<CounterDirectory>(db,CounterDirectory::KeyedGuest(registry_),
            std::move(admission),capacity,active_limit,workers,idle,alarms,mail,registry_.manifest(),principal_mode,std::move(mail_policy),transactional_policy,tenant_budget.limits());
    }
};
} // namespace ores::durable
