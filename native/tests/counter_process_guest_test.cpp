// Real compiled guest integration, including an intentionally looping turn.
#include "ores/durable/counter_principal_gateway.hpp"
#include <cassert>
#include <atomic>
#include <filesystem>
#include <cstdlib>
#include <fstream>
#include <sys/prctl.h>
using namespace ores::durable;
using namespace std::chrono_literals;
int main(int argc,char** argv) {
    // Trusted synthetic worker intentionally ignores parent death. It keeps
    // fd 3 open until it exits, allowing a deterministic orphan regression.
    if(argc==7) {
        assert(::fcntl(3,F_GETFD)>=0);
        const std::string marker=argv[1];
        {std::ofstream ready(marker+".ready");ready<<::getpid();}
        const auto limit=CounterDirectory::Clock::now()+10s;
        while(!std::filesystem::exists(marker+".release") &&
              CounterDirectory::Clock::now()<limit) std::this_thread::sleep_for(5ms);
        return std::filesystem::exists(marker+".release")?0:49;
    }
    assert(argc==5 || argc==4);
    const bool fuel=argc==4;
    auto make_runner=[&](std::string digest){
        if(fuel) return CounterProcessGuest::fuel_worker(argv[1],argv[2],std::move(digest),5s);
        return CounterProcessGuest(argv[1],argv[2],argv[3],std::move(digest),1500ms);
    };
    auto runner=make_runner(argv[argc-1]);
    assert(runner(0,1,2)==2);
    bool stopped=false;
    const auto start=CounterDirectory::Clock::now();
    try {(void)runner(0,1,7);}
    catch(const CounterDirectory::RetryableGuestFailure& e) {
        stopped=true;
        assert(e.reason==(fuel?CounterDirectory::FailureReason::fuel:CounterDirectory::FailureReason::deadline));
    }
    assert(stopped);
    if(!fuel) assert(CounterDirectory::Clock::now()-start>=1500ms);
    auto wrong=make_runner(std::string(64,'0'));
    bool invalid=false;
    try {(void)wrong(0,1,2);}
    catch(const CounterDirectory::RetryableGuestFailure&) {assert(false);}
    catch(const std::runtime_error&) {invalid=true;}
    assert(invalid);
    int status=0;
    assert(::waitpid(-1,&status,WNOHANG)==-1 && errno==ECHILD);
    char tmp[]="/tmp/atto-process-mail-XXXXXX";
    assert(mkdtemp(tmp));
    const auto db=std::string(tmp)+"/mail.db";
    const SyncKey a{"tenant","counters","Counter","poison"}, b{"tenant","counters","Counter","healthy"};
    const auto auth=[](const SyncKey& k){return k.tenant=="tenant";};
    std::atomic<int> timeouts{0};
    {
        CounterDirectory directory(db,runner,auth);
        assert(directory.admit(a,"hang",1,7).get().sequence>0);
        assert(directory.admit(a,"after",1,2).get().sequence>0);
        assert(directory.admit(b,"healthy",1,5).get().sequence>0);
    }
    {
        CounterDirectory directory(db,[&](auto n,auto op,auto d){
            try {return runner(n,op,d);}
            catch(const CounterDirectory::RetryableGuestFailure& e) {
                assert(e.reason==(fuel?CounterDirectory::FailureReason::fuel:CounterDirectory::FailureReason::deadline));
                ++timeouts;throw;
            }
        },auth,16,4,2,10ms,0ms,10ms);
        SqliteActorSync observer(db);
        const auto deadline=CounterDirectory::Clock::now()+25s;
        while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<deadline)
            std::this_thread::sleep_for(10ms);
        assert(observer.pending_mail().empty());
        assert(timeouts==3 && !directory.mail_fault());
        assert(observer.snapshot(a).bytes=="2" && observer.snapshot(a).revision==1);
        assert(observer.snapshot(b).bytes=="5" && observer.snapshot(b).revision==1);
        const auto dead=directory.admit(a,"hang",1,7).get();
        assert(dead.dead_letter && !dead.receipt);
        assert(directory.admit(a,"after",1,2).get().receipt->reply=="2");
    }
    assert(::waitpid(-1,&status,WNOHANG)==-1 && errno==ECHILD);
    // Real compiled fuel worker with host-selected, tenant-scoped artifacts.
    if(fuel) {
        using Binding=CounterArtifactRegistry::Binding;
        const SyncKey other{"other","counters","Counter","same"};
        const SyncKey missing{"tenant","missing","Counter","same"};
        const SyncKey invalid{"tenant","bad-digest","Counter","same"};
        const std::vector<Binding> bindings{
            {"tenant","counters","Counter",argv[2],argv[3]},
            {"other","counters","Counter",argv[2],argv[3]},
            {"tenant","bad-digest","Counter",argv[2],std::string(64,'0')}
        };
        const CounterArtifactRegistry registry(argv[1],5s,bindings);
        bool denied=false;
        try { (void)registry(a,0,1,1); }
        catch(const std::logic_error&) {denied=true;}
        assert(denied);
        denied=false;
        try { CounterArtifactRegistry duplicate(argv[1],5s,{bindings[0],bindings[0]}); }
        catch(const std::invalid_argument&) {denied=true;}
        assert(denied);
        const auto registry_db=std::string(tmp)+"/registry.db";
        const auto authorize=[&](const SyncKey& k){return registry.contains(k);};
        {
            CounterDirectory directory(registry_db,CounterDirectory::KeyedGuest(registry),
                authorize,128,128,1,0ms,0ms,0ms,registry.manifest());
            assert(directory.send(a,"same",1,2).get()->reply=="2");
            assert(directory.send(other,"same",1,5).get()->reply=="5");
            denied=false;
            try {(void)directory.send(missing,"same",1,1);}
            catch(const std::logic_error&) {denied=true;}
            assert(denied);
            denied=false;
            try {(void)directory.send(invalid,"same",1,1).get();}
            catch(const CounterDirectory::RetryableGuestFailure&) {assert(false);}
            catch(const std::runtime_error&) {denied=true;}
            assert(denied);
            SqliteActorSync observer(registry_db);
            assert(observer.snapshot(invalid).revision==0);
            assert(!observer.lookup_receipt(invalid,observer.snapshot(invalid).epoch,
                                            "same","1:1",CounterTurn::schema));
            assert(observer.snapshot(a).bytes=="2" && observer.snapshot(other).bytes=="5");
            assert(directory.admit(a,"pending",1,3).get().sequence>0);
        }
        // Reordering bindings must not change the identity; changing even
        // one digest or the wall budget must reject startup before replay.
        auto reversed=bindings;
        std::reverse(reversed.begin(),reversed.end());
        const CounterArtifactRegistry same(argv[1],5s,reversed);
        assert(same.manifest()==registry.manifest());
        auto changed=bindings;
        changed[0].sha256=std::string(64,'1');
        const CounterArtifactRegistry different(argv[1],5s,changed);
        assert(different.manifest()!=registry.manifest());
        const CounterArtifactRegistry different_budget(argv[1],6s,bindings);
        for(const auto* candidate:{&different,&different_budget}) {
            denied=false;
            try {
                CounterDirectory replacement(registry_db,CounterDirectory::KeyedGuest(*candidate),
                    authorize,128,128,1,0ms,0ms,0ms,candidate->manifest());
            } catch(const std::logic_error&) {denied=true;}
            assert(denied);
        }
        denied=false;
        try {CounterDirectory legacy(registry_db,runner,auth);}
        catch(const std::logic_error&) {denied=true;}
        assert(denied);
        {
            CounterDirectory replacement(registry_db,CounterDirectory::KeyedGuest(same),
                authorize,128,128,1,0ms,0ms,10ms,same.manifest());
            auto replay=replacement.send(a,"same",1,2).get();
            assert(replay && replay->duplicate && replay->reply=="2");
            SqliteActorSync observer(registry_db);
            const auto until=CounterDirectory::Clock::now()+5s;
            while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<until)
                std::this_thread::sleep_for(5ms);
            assert(observer.pending_mail().empty() && !replacement.mail_fault());
            assert(observer.snapshot(a).bytes=="5");
        }
        // Ephemeral test signing key only; never persisted or logged.
        using Key=std::unique_ptr<EVP_PKEY,decltype(&EVP_PKEY_free)>;
        using Keygen=std::unique_ptr<EVP_PKEY_CTX,decltype(&EVP_PKEY_CTX_free)>;
        using Sign=std::unique_ptr<EVP_MD_CTX,decltype(&EVP_MD_CTX_free)>;
        Keygen keygen(EVP_PKEY_CTX_new_id(EVP_PKEY_ED25519,nullptr),EVP_PKEY_CTX_free);
        assert(keygen && EVP_PKEY_keygen_init(keygen.get())==1);
        EVP_PKEY* raw=nullptr;
        assert(EVP_PKEY_keygen(keygen.get(),&raw)==1);
        Key key(raw,EVP_PKEY_free);
        SignedCounterDeployment::PublicKey public_key{};
        std::size_t pubsize=public_key.size();
        assert(EVP_PKEY_get_raw_public_key(key.get(),public_key.data(),&pubsize)==1 &&
               pubsize==public_key.size());
        SignedCounterDeployment::Signature signature{};
        auto sigsize=signature.size();
        Sign sign(EVP_MD_CTX_new(),EVP_MD_CTX_free);
        assert(sign && EVP_DigestSignInit(sign.get(),nullptr,nullptr,nullptr,key.get())==1);
        assert(EVP_DigestSign(sign.get(),signature.data(),&sigsize,
            reinterpret_cast<const unsigned char*>(registry.manifest().data()),
            registry.manifest().size())==1 && sigsize==signature.size());
        const SignedCounterDeployment signed_deployment(same,public_key,signature);
        auto corrupted=signature;corrupted[0]^=1;
        denied=false;
        try {SignedCounterDeployment rejected(same,public_key,corrupted);}
        catch(const std::logic_error&) {denied=true;}
        assert(denied);
        auto wrong_key=public_key;wrong_key[0]^=1;
        denied=false;
        try {SignedCounterDeployment rejected(same,wrong_key,signature);}
        catch(const std::logic_error&) {denied=true;}
        assert(denied);
        denied=false;
        try {SignedCounterDeployment rejected(different,public_key,signature);}
        catch(const std::logic_error&) {denied=true;}
        assert(denied);
        denied=false;
        try {SignedCounterDeployment rejected(different_budget,public_key,signature);}
        catch(const std::logic_error&) {denied=true;}
        assert(denied);
        const auto signed_db=std::string(tmp)+"/signed.db";
        {
            auto directory=signed_deployment.open(signed_db,
                [&](const SyncKey& k){return k.tenant=="tenant";});
            assert(directory->send(a,"signed",1,3).get()->reply=="3");
            denied=false;
            try {(void)directory->send(other,"signed",1,1);}
            catch(const std::logic_error&) {denied=true;}
            assert(denied);
        }
        {
            auto directory=signed_deployment.open(signed_db,authorize);
            auto replay=directory->send(a,"signed",1,3).get();
            assert(replay && replay->duplicate && replay->reply=="3");
        }
        // Opaque, operation-bound in-process grants; no network credential
        // handling in this fixture. Scope and principal policy are distinct.
        const auto gateway_db=std::string(tmp)+"/gateway.db";
        std::atomic<bool> alice_allowed{true};
        const auto policy=[&](std::string_view principal,const SyncKey& k,std::int64_t op){
            return k.tenant=="tenant" && k.object_id=="poison" &&
                ((principal=="alice" && alice_allowed) || principal=="bob") &&
                (op==1 || op==2);
        };
        std::optional<CounterPrincipalGateway::Capability> old;
        {
            CounterPrincipalGateway gateway(signed_deployment,gateway_db,authorize,policy);
            const auto alice=gateway.issue("alice",a,1,30s);
            const auto bob=gateway.issue("bob",a,1,30s);
            old=alice;
            assert(gateway.send(alice,"same-id",2).get()->reply=="2");
            assert(gateway.send(bob,"same-id",3).get()->reply=="5");
            auto again=gateway.send(alice,"same-id",2).get();
            assert(again && again->duplicate && again->reply=="2");
            denied=false;
            try {(void)gateway.send(alice,"same-id",3).get();}
            catch(const std::logic_error&) {denied=true;}
            assert(denied);
            denied=false;
            try {(void)gateway.issue("alice",other,1,30s);}
            catch(const std::logic_error&) {denied=true;}
            assert(denied);
            const auto read=gateway.issue("bob",a,2,30s);
            denied=false;
            try {(void)gateway.send(read,"write",1);}
            catch(const std::invalid_argument&) {denied=true;}
            assert(denied);
            const auto expires=gateway.issue("alice",a,1,1ms);
            std::this_thread::sleep_for(5ms);
            denied=false;
            try {(void)gateway.send(expires,"expired",1);}
            catch(const std::logic_error&) {denied=true;}
            assert(denied);
            alice_allowed=false;
            denied=false;
            try {(void)gateway.send(alice,"same-id",2);}
            catch(const std::logic_error&) {denied=true;}
            assert(denied);
            alice_allowed=true;
        }
        {
            CounterPrincipalGateway gateway(signed_deployment,gateway_db,authorize,policy);
            denied=false;
            try {(void)gateway.send(*old,"same-id",2);}
            catch(const std::logic_error&) {denied=true;}
            assert(denied);
            auto fresh=gateway.issue("alice",a,1,30s);
            auto replay=gateway.send(fresh,"same-id",2).get();
            assert(replay && replay->duplicate && replay->reply=="2");
            SqliteActorSync observer(gateway_db);
            assert(observer.snapshot(a).bytes=="5" && observer.snapshot(a).revision==2);
        }
        denied=false;
        try {auto raw=signed_deployment.open(gateway_db,authorize);}
        catch(const std::logic_error&) {denied=true;}
        assert(denied);
        // Durable admission survives capability expiry and gateway restart,
        // but never bypasses current principal policy at dispatch.
        const auto durable_gateway_db=std::string(tmp)+"/durable-gateway.db";
        {
            CounterPrincipalGateway gateway(signed_deployment,durable_gateway_db,authorize,policy);
            auto alice=gateway.issue("alice",a,1,30s);
            auto bob=gateway.issue("bob",a,1,30s);
            auto first=gateway.admit(alice,"durable",2).get();
            auto second=gateway.admit(bob,"durable",3).get();
            assert(first.sequence>0 && second.sequence>first.sequence);
            assert(gateway.admit(alice,"durable",2).get().sequence==first.sequence);
            SqliteActorSync observer(durable_gateway_db);
            auto heads=observer.pending_mail();
            assert(heads.size()==1 && heads[0].principal=="alice");
            assert(observer.snapshot(a).revision==0);
        }
        alice_allowed=false;
        {
            CounterPrincipalGateway gateway(signed_deployment,durable_gateway_db,authorize,policy,128,128,2,10ms);
            SqliteActorSync observer(durable_gateway_db);
            const auto until=CounterDirectory::Clock::now()+5s;
            while(!observer.pending_mail().empty() && CounterDirectory::Clock::now()<until)
                std::this_thread::sleep_for(5ms);
            assert(observer.pending_mail().empty() && !gateway.mail_fault());
            assert(observer.snapshot(a).bytes=="3" && observer.snapshot(a).revision==1);
            auto bob=gateway.issue("bob",a,1,30s);
            auto receipt=gateway.admit(bob,"durable",3).get();
            assert(receipt.receipt && receipt.receipt->duplicate && receipt.receipt->reply=="3");
            alice_allowed=true;
            auto alice=gateway.issue("alice",a,1,30s);
            auto rejected=gateway.admit(alice,"durable",2).get();
            assert(rejected.dead_letter && rejected.authorization_denied && !rejected.receipt);
            assert(observer.snapshot(a).revision==1);
        }
        // Strict mode additionally requires persisted operator grants.
        // The external policy still allows both principals; only the
        // transactional policy revokes Alice before restart dispatch.
        const auto strict_db=std::string(tmp)+"/strict-gateway.db";
        {
            CounterPrincipalGateway gateway(signed_deployment,strict_db,authorize,policy,128,128,2,0ms,true);
            SqliteActorSync admin(strict_db);
            assert(admin.set_principal_policy(a,"alice",1,0,true)==1);
            assert(admin.set_principal_policy(a,"bob",1,0,true)==1);
            auto alice=gateway.issue("alice",a,1,30s);
            auto bob=gateway.issue("bob",a,1,30s);
            assert(gateway.admit(alice,"strict",2).get().sequence>0);
            assert(gateway.admit(bob,"strict",3).get().sequence>0);
            assert(admin.set_principal_policy(a,"alice",1,1,false)==2);
        }
        {
            CounterPrincipalGateway gateway(signed_deployment,strict_db,authorize,policy,128,128,2,10ms,true);
            SqliteActorSync admin(strict_db);
            const auto until=CounterDirectory::Clock::now()+5s;
            while(!admin.pending_mail().empty() && CounterDirectory::Clock::now()<until)
                std::this_thread::sleep_for(5ms);
            assert(admin.pending_mail().empty() && !gateway.mail_fault());
            assert(admin.snapshot(a).bytes=="3" && admin.snapshot(a).revision==1);
            auto bob=gateway.issue("bob",a,1,30s);
            assert(gateway.admit(bob,"strict",3).get().receipt->reply=="3");
            assert(admin.set_principal_policy(a,"alice",1,2,true)==3);
            auto alice=gateway.issue("alice",a,1,30s);
            assert(gateway.admit(alice,"strict",2).get().authorization_denied);
        }
        // A populated legacy store must not be implicitly adopted.
        const auto legacy_db=std::string(tmp)+"/legacy-deployment.db";
        {CounterDirectory legacy(legacy_db,runner,auth);
         assert(legacy.send(a,"old",1,1).get()->reply=="1");}
        denied=false;
        try {
            CounterDirectory adopt(legacy_db,CounterDirectory::KeyedGuest(registry),
                authorize,128,128,1,0ms,0ms,0ms,registry.manifest());
        } catch(const std::logic_error&) {denied=true;}
        assert(denied);
    }
    // Host-only orphan test: kill the controller while its trusted worker
    // retains the inherited lock. An independent controller must fail closed
    // until the worker actually exits (no PID-based guesses).
    assert(::prctl(PR_SET_CHILD_SUBREAPER,1)==0);
    const auto orphan_db=std::string(tmp)+"/orphan.db";
    const auto marker=std::string(tmp)+"/orphan";
    const auto self=std::filesystem::canonical("/proc/self/exe").string();
    const pid_t controller=fork();
    assert(controller>=0);
    if(controller==0) {
        try {
            auto fake=CounterProcessGuest::fuel_worker(self,marker,std::string(64,'0'),15s);
            CounterDirectory directory(orphan_db,fake,auth);
            (void)directory.send(a,"orphan",1,1).get();
            _exit(50);
        } catch(...) {_exit(51);}
    }
    const auto ready_deadline=CounterDirectory::Clock::now()+5s;
    while(!std::filesystem::exists(marker+".ready") &&
          CounterDirectory::Clock::now()<ready_deadline) std::this_thread::sleep_for(5ms);
    assert(std::filesystem::exists(marker+".ready"));
    assert(::kill(controller,SIGKILL)==0);
    assert(::waitpid(controller,&status,0)==controller && WIFSIGNALED(status));
    bool blocked=false;
    try {CounterDirectory replacement(orphan_db,runner,auth);}
    catch(const std::logic_error&) {blocked=true;}
    assert(blocked);
    {std::ofstream release(marker+".release");release<<"release";}
    // Subreaper adopts the former controller's still-running worker.
    const auto orphan_deadline=CounterDirectory::Clock::now()+5s;
    pid_t retired=0;
    do {
        retired=::waitpid(-1,&status,WNOHANG);
        if(retired==0) std::this_thread::sleep_for(5ms);
    } while(retired==0 && CounterDirectory::Clock::now()<orphan_deadline);
    assert(retired>0 && WIFEXITED(status) && WEXITSTATUS(status)==0);
    {
        CounterDirectory replacement(orphan_db,runner,auth);
        assert(replacement.send(a,"orphan",1,1).get()->reply=="1");
    }
    assert(::waitpid(-1,&status,WNOHANG)==-1 && errno==ECHILD);
    assert(::prctl(PR_SET_CHILD_SUBREAPER,0)==0);
    std::filesystem::remove_all(tmp);
    return 0;
}
