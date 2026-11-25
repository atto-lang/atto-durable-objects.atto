#pragma once
#include "ores/durable/counter_turn.hpp"
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <filesystem>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <future>
#include <map>
#include <mutex>
#include <optional>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace ores::durable {
// Trusted single-host controller prototype. Cooperative local ownership
// uses a persistent advisory lock file in a trusted directory. Raw store
// users must honor this policy. No distributed lease election.
// Guest and authorization callbacks must be thread-safe and terminate.
// Never invoke directory methods recursively from either callback.
class CounterDirectory final {
public:
    using Guest = std::function<std::int64_t(std::int64_t,std::int64_t,std::int64_t)>;
    // Trusted host callback receives the full authenticated identity.
    using KeyedGuest = std::function<std::int64_t(const SyncKey&,std::int64_t,std::int64_t,std::int64_t)>;
    using Reply = std::optional<SyncCommit>;
    using Authorize = std::function<bool(const SyncKey&)>;
    using PrincipalPolicy=std::function<bool(std::string_view,const SyncKey&,std::int64_t)>;
    using Clock = std::chrono::steady_clock;
    // Trusted runner signals a classified failure only after guest execution
    // has physically stopped. Never use for database/ownership/unknown errors.
    enum class FailureReason { classified, deadline, trap, fuel };
    class RetryableGuestFailure final : public std::runtime_error {
    public:
        const FailureReason reason;
        explicit RetryableGuestFailure(FailureReason why=FailureReason::classified)
            :std::runtime_error("classified guest failure"),reason(why){}
    };
private:
    using Key = std::tuple<std::string,std::string,std::string,std::string>;
    struct Lane {
        std::deque<std::packaged_task<void()>> queue;
        bool running=false;
        std::int64_t epoch=0; // only its executing turn can install an epoch
        Clock::time_point idle_since=Clock::now();
    };
    // Lock lifetime spans all workers and the SQLite store. Never unlink the
    // lock file: unlink/recreate can give two controllers different inodes.
    // Trusted local directory only; not safe against hostile path replacement.
    class ControllerLock {
        std::string db_;
        int fd_=-1;
    public:
        explicit ControllerLock(const std::string& db) {
            if(db.empty() || db.find('\0')!=std::string::npos)
                throw std::invalid_argument("invalid controller database path");
            db_=std::filesystem::weakly_canonical(std::filesystem::absolute(db)).string();
            struct stat st{};
            if(::stat(db_.c_str(),&st)==0 && (!S_ISREG(st.st_mode) || st.st_nlink!=1))
                throw std::logic_error("database must be a regular unaliased file");
            const auto lock=db_+".controller.lock";
            fd_=::open(lock.c_str(),O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK,0600);
            if(fd_<0) throw std::runtime_error("controller lock open failed");
            if(::fstat(fd_,&st)!=0 || !S_ISREG(st.st_mode) || st.st_nlink!=1 ||
               st.st_uid!=::geteuid() || (st.st_mode & 0077)!=0 ||
               ::flock(fd_,LOCK_EX|LOCK_NB)!=0) {
                ::close(fd_);fd_=-1;
                throw std::logic_error("controller lock denied");
            }
        }
        ControllerLock(const ControllerLock&)=delete;
        ControllerLock& operator=(const ControllerLock&)=delete;
        ~ControllerLock(){if(fd_>=0)::close(fd_);}
        const std::string& path() const noexcept {return db_;}
        int fd() const noexcept {return fd_;}
    };
    ControllerLock owner_;
    SqliteActorSync store_;
    const KeyedGuest guest_;
    const std::string deployment_manifest_;
    // Borrowed only while invoking a synchronous trusted guest runner.
    // A spawned worker duplicates this open file description into its own
    // descriptor table, retaining flock until physical process termination.
    inline static thread_local int guest_guard_fd_=-1;
    inline static thread_local const std::string* guest_deployment_=nullptr;
    struct GuestGuard {
        int previous;
        const std::string* previous_deployment;
        GuestGuard(int fd,const std::string& manifest)
            :previous(guest_guard_fd_),previous_deployment(guest_deployment_) {
            guest_guard_fd_=fd;guest_deployment_=&manifest;
        }
        ~GuestGuard(){guest_guard_fd_=previous;guest_deployment_=previous_deployment;}
    };
    auto guarded_guest(const SyncKey& k) {
        return [this,&k](std::int64_t n,std::int64_t op,std::int64_t d){
            GuestGuard guard(owner_.fd(),deployment_manifest_);
            return guest_(k,n,op,d);
        };
    }
    static KeyedGuest adapt(Guest guest) {
        if(!guest) throw std::invalid_argument("missing guest runner");
        return [guest=std::move(guest)](const SyncKey&,auto n,auto op,auto d){return guest(n,op,d);};
    }
    const Authorize authorize_;
    const bool principal_mode_;
    const PrincipalPolicy mail_policy_;
    struct PolicyDenied {};
    const std::size_t capacity_,active_limit_;
    const Clock::duration idle_timeout_;
    std::map<Key,Lane> lanes_; // protected by mu_; running lanes cannot be erased
    std::size_t queued_=0,active_=0;
    std::optional<Key> last_scheduled_;
    std::mutex mu_;
    std::condition_variable wake_;
    bool closed_=false;
    std::vector<std::thread> workers_;
    std::mutex alarm_mu_;
    std::condition_variable alarm_wake_;
    bool alarm_stop_=false;
    std::exception_ptr alarm_fault_;
    std::thread alarm_worker_;
    static std::int64_t unix_ms() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
    std::future<Reply> enqueue_alarm(SyncAlarm alarm) {
        validate(alarm.key);
        const auto id=identity(alarm.key);
        return submit(id,[this,id,alarm=std::move(alarm)]{
            validate(alarm.key);
            const auto epoch=activate(id,alarm.key);
            try {
                auto r=CounterTurn::run_alarm(store_,alarm,epoch,unix_ms(),guarded_guest(alarm.key));
                validate(alarm.key);
                if(store_.snapshot(alarm.key).epoch!=epoch) throw std::logic_error("stale alarm publication epoch");
                return r;
            } catch(...) {
                retire(id);throw;
            }
        });
    }
    // One supervisor poller and at most one outstanding alarm turn. On
    // unexpected fault stop dispatch and retain the pending durable alarm.
    // Supervisor must inspect alarm_fault() and repair/reopen the controller.
    void alarm_loop(Clock::duration interval) noexcept {
        try {
            for(;;) {
                {
                    std::unique_lock<std::mutex> lock(alarm_mu_);
                    if(alarm_wake_.wait_for(lock,interval,[&]{return alarm_stop_;})) return;
                }
                for(const auto& alarm:store_.due_alarms(unix_ms(),64)) {
                    {
                        std::lock_guard<std::mutex> lock(alarm_mu_);
                        if(alarm_stop_) return;
                    }
                    // Saturation and activation quota are backpressure:
                    // retain deadline and retry after the next interval.
                    try { (void)enqueue_alarm(alarm).get(); }
                    catch(const std::length_error&) { break; }
                }
            }
        } catch(...) {
            std::lock_guard<std::mutex> lock(alarm_mu_);
            alarm_fault_=std::current_exception();
        }
    }

    std::mutex mail_mu_;
    std::condition_variable mail_wake_;
    bool mail_stop_=false;
    std::exception_ptr mail_fault_;
    std::thread mail_worker_;
    bool mail_authorized(const SyncMail& mail) const {
        check(mail.key.tenant);check(mail.key.name_space);check(mail.key.actor_type);check(mail.key.object_id);
        if(mail.key.actor_type!="Counter") throw std::logic_error("unexpected mail actor");
        if(!principal_mode_) {
            if(!mail.principal.empty()) throw std::logic_error("principal gateway required");
            validate(mail.key);return true;
        }
        if(mail.principal.empty() || !mail_policy_ || mail.operation_id.compare(0,11,"@principal:")!=0 ||
           mail.command.size()<3 || mail.command[1]!=':' || (mail.command[0]!='1' && mail.command[0]!='2'))
            throw std::logic_error("invalid persisted principal mail");
        return authorize_(mail.key) && mail_policy_(mail.principal,mail.key,mail.command[0]-'0');
    }
    std::future<Reply> enqueue_mail(SyncMail mail) {
        // Structural validation only here; a revoked principal is a
        // terminal outcome for its exact head, not a global poller fault.
        check(mail.key.tenant);check(mail.key.name_space);check(mail.key.actor_type);check(mail.key.object_id);
        const auto id=identity(mail.key);
        return submit(id,[this,id,mail=std::move(mail)]{
            const auto epoch=activate(id,mail.key);
            try {
                if(unix_ms()<mail.retry_at_ms || !store_.mail_pending(mail,epoch))
                    return Reply{};
                if(!mail_authorized(mail)) {
                    store_.deny_mail(mail,epoch);return Reply{};
                }
                // A surviving dispatch marker belongs to a previous activation.
                // The controller lock excludes former hosts and cooperating
                // process runners that inherit its open file description.
                // General callbacks must still establish physical retirement.
                if(mail.attempt_epoch!=0) {
                    if(mail.attempt_epoch>=epoch)
                        throw std::logic_error("unresolved current mailbox attempt");
                    (void)store_.fail_mail(mail,epoch,unix_ms());
                    return Reply{};
                }
                const auto started=store_.begin_mail(mail,epoch,unix_ms());
                Reply r;
                try {
                    auto invoke=guarded_guest(mail.key);
                    auto checked=[&](auto n,auto op,auto d) {
                        if(!mail_authorized(started)) throw PolicyDenied{};
                        auto result=invoke(n,op,d);
                        if(!mail_authorized(started)) throw PolicyDenied{};
                        return result;
                    };
                    r=CounterTurn::run_mail(store_,started,epoch,checked);
                } catch(const PolicyDenied&) {
                    store_.deny_mail(started,epoch);return Reply{};
                } catch(const RetryableGuestFailure&) {
                    if(!mail_authorized(started)) {
                        store_.deny_mail(started,epoch);return Reply{};
                    }
                    // Only after the runner physically stops may this owner
                    // resolve its marker as a classified failure.
                    (void)store_.fail_mail(started,epoch,unix_ms());
                    return Reply{};
                }
                if(!r) throw std::logic_error("marked mailbox turn did not commit");
                if(!mail_authorized(started)) return Reply{};
                if(store_.snapshot(mail.key).epoch!=epoch) throw std::logic_error("stale mail publication epoch");
                return r;
            } catch(...) {retire(id);throw;}
        });
    }
    // At most one outstanding durable mail turn per object and at most
    // worker_count total. Reap ready futures without waiting behind an
    // unrelated slow guest. Stop admitting on unexpected faults, but drain
    // already submitted turns before leaving the supervisor.
    void mail_loop(Clock::duration interval,std::size_t worker_count) noexcept {
        std::map<Key,std::future<Reply>> in_flight;
        std::string last_tenant;
        std::exception_ptr fault;
        auto collect=[&](bool drain) {
            for(auto it=in_flight.begin();it!=in_flight.end();) {
                if(!drain && it->second.wait_for(Clock::duration::zero())!=std::future_status::ready) {
                    ++it;continue;
                }
                try {(void)it->second.get();}
                catch(const std::length_error&) {/* durable backpressure */}
                catch(...) {if(!fault) fault=std::current_exception();}
                it=in_flight.erase(it);
            }
        };
        try {
            for(;;) {
                {
                    std::unique_lock<std::mutex> lock(mail_mu_);
                    if(mail_wake_.wait_for(lock,interval,[&]{return mail_stop_;})) break;
                }
                collect(false);
                if(fault) break;
                if(in_flight.size()>=worker_count) continue;
                for(const auto& mail:store_.fair_pending_mail(256,unix_ms(),last_tenant)) {
                    {
                        std::lock_guard<std::mutex> lock(mail_mu_);
                        if(mail_stop_) break;
                    }
                    if(in_flight.size()>=worker_count) break;
                    const auto id=identity(mail.key);
                    if(in_flight.find(id)!=in_flight.end()) continue;
                    // Rotate even on queue/activation saturation; do not
                    // let one tenant monopolize subsequent bounded scans.
                    last_tenant=mail.key.tenant;
                    // Allocate tracking before enqueue, so allocation failure
                    // cannot leave an untracked accepted future.
                    auto inserted=in_flight.try_emplace(id);
                    try {inserted.first->second=enqueue_mail(mail);}
                    catch(const std::length_error&) {in_flight.erase(inserted.first);}
                    catch(...) {in_flight.erase(inserted.first);throw;}
                }
            }
        } catch(...) {if(!fault) fault=std::current_exception();}
        // Publish a detected fatal fault before waiting for already accepted
        // turns; supervisor observability must not depend on their duration.
        if(fault) {
            std::lock_guard<std::mutex> lock(mail_mu_);
            mail_fault_=fault;
        }
        collect(true);
        if(fault) {
            std::lock_guard<std::mutex> lock(mail_mu_);
            mail_fault_=fault;
        }
    }

    static Key identity(const SyncKey& k) {
        return {k.tenant,k.name_space,k.actor_type,k.object_id};
    }
    static void check(const std::string& s) {
        if(s.empty() || s.size()>256 || s.find('\0')!=std::string::npos)
            throw std::invalid_argument("invalid identity component");
    }
    void validate(const SyncKey& k) const {
        check(k.tenant);check(k.name_space);check(k.actor_type);check(k.object_id);
        if(k.actor_type!="Counter" || !authorize_(k)) throw std::logic_error("unauthorized actor");
    }
    // mu_ held. Only fully quiescent lanes may be retired.
    void sweep(Clock::time_point now) {
        for(auto it=lanes_.begin();it!=lanes_.end();) {
            auto& l=it->second;
            if(!l.running && l.queue.empty() &&
               (!l.epoch || (idle_timeout_!=Clock::duration::zero() &&
                            now-l.idle_since>=idle_timeout_))) {
                if(l.epoch) --active_;
                it=lanes_.erase(it);
            } else ++it;
        }
    }
    auto runnable() {
        const auto start=last_scheduled_ ? lanes_.upper_bound(*last_scheduled_) : lanes_.begin();
        const auto ready=[](const auto& p){return !p.second.running && !p.second.queue.empty();};
        auto it=std::find_if(start,lanes_.end(),ready);
        if(it!=lanes_.end()) return it;
        it=std::find_if(lanes_.begin(),start,ready);
        return it==start ? lanes_.end() : it;
    }
    void loop() {
        for(;;) {
            std::packaged_task<void()> job;
            Key id;
            {
                std::unique_lock<std::mutex> lock(mu_);
                for(;;) {
                    sweep(Clock::now());
                    auto it=runnable();
                    if(it!=lanes_.end()) {
                        id=it->first;
                        last_scheduled_=id;
                        auto& l=it->second;
                        l.running=true;
                        job=std::move(l.queue.front());l.queue.pop_front();--queued_;
                        break;
                    }
                    if(closed_ && queued_==0) return;
                    if(idle_timeout_==Clock::duration::zero()) wake_.wait(lock);
                    else wake_.wait_for(lock,idle_timeout_);
                }
            }
            job();
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto& l=lanes_.at(id);
                l.running=false;l.idle_since=Clock::now();
                sweep(l.idle_since);
            }
            wake_.notify_all();
        }
    }
    template<class F> auto submit(const Key& id,F f) -> std::future<decltype(f())> {
        using R=decltype(f());
        std::packaged_task<R()> inner(std::move(f));
        auto result=inner.get_future();
        {
            std::lock_guard<std::mutex> lock(mu_);
            if(closed_) throw std::logic_error("directory closed");
            if(queued_>=capacity_) throw std::length_error("directory queue full");
            sweep(Clock::now());
            auto [it,inserted]=lanes_.try_emplace(id);
            try {
                it->second.queue.emplace_back([job=std::move(inner)]() mutable {job();});
            } catch(...) {
                if(inserted) lanes_.erase(it);
                throw;
            }
            ++queued_;
        }
        wake_.notify_all();
        return result;
    }
    std::int64_t activate(const Key& id,const SyncKey& k) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto& l=lanes_.at(id);
            if(l.epoch) return l.epoch;
            if(active_>=active_limit_) throw std::length_error("activation quota exhausted");
            ++active_; // reserve before releasing scheduler lock for I/O
        }
        try {
            const auto epoch=store_.activate_next(k);
            std::lock_guard<std::mutex> lock(mu_);
            lanes_.at(id).epoch=epoch;
            return epoch;
        } catch(...) {
            std::lock_guard<std::mutex> lock(mu_);
            --active_;
            throw;
        }
    }
    bool retire(const Key& id) {
        std::lock_guard<std::mutex> lock(mu_);
        auto& l=lanes_.at(id);
        if(!l.epoch) return false;
        l.epoch=0;--active_;
        return true;
    }
public:
    // Host-only borrowed descriptor; never expose to compiled guest imports.
    static int current_guest_guard_fd() noexcept {return guest_guard_fd_;}
    static bool current_guest_deployment_matches(std::string_view manifest) noexcept {
        return guest_deployment_ && !manifest.empty() && *guest_deployment_==manifest;
    }
    CounterDirectory(const std::string& db, Guest guest, Authorize authorize,
                     std::size_t capacity=128,std::size_t active_limit=128,
                     std::size_t workers=1,
                     Clock::duration idle_timeout=Clock::duration::zero(),
                     Clock::duration alarm_interval=Clock::duration::zero(),
                     Clock::duration mail_interval=Clock::duration::zero())
        : CounterDirectory(db,adapt(std::move(guest)),std::move(authorize),capacity,
                           active_limit,workers,idle_timeout,alarm_interval,mail_interval) {}
    CounterDirectory(const std::string& db, KeyedGuest guest, Authorize authorize,
                     std::size_t capacity=128,std::size_t active_limit=128,
                     std::size_t workers=1,
                     Clock::duration idle_timeout=Clock::duration::zero(),
                     Clock::duration alarm_interval=Clock::duration::zero(),
                     Clock::duration mail_interval=Clock::duration::zero(),
                     std::string deployment_manifest={},bool principal_mode=false,PrincipalPolicy mail_policy={})
        : owner_(db),store_(owner_.path()),guest_(std::move(guest)),
          deployment_manifest_(std::move(deployment_manifest)),authorize_(std::move(authorize)),
          principal_mode_(principal_mode),mail_policy_(std::move(mail_policy)),
          capacity_(capacity),active_limit_(active_limit),idle_timeout_(idle_timeout) {
        if(!guest_ || !authorize_ || !capacity_ || !active_limit_ ||
           !workers || workers>64 || idle_timeout_<Clock::duration::zero() ||
           alarm_interval<Clock::duration::zero() || mail_interval<Clock::duration::zero() ||
           (principal_mode_ && (alarm_interval!=Clock::duration::zero() || (mail_interval!=Clock::duration::zero() && !mail_policy_))) ||
           (!principal_mode_ && mail_policy_))
            throw std::invalid_argument("invalid directory configuration");
        // Reject changed or missing deployment before any activation or replay.
        store_.bind_deployment(deployment_manifest_);
        store_.bind_principal_namespace(principal_mode);
        workers_.reserve(workers);
        try {
            for(std::size_t i=0;i<workers;++i) workers_.emplace_back([this]{loop();});
            if(alarm_interval!=Clock::duration::zero())
                alarm_worker_=std::thread([this,alarm_interval]{alarm_loop(alarm_interval);});
            if(mail_interval!=Clock::duration::zero())
                mail_worker_=std::thread([this,mail_interval,workers]{mail_loop(mail_interval,workers);});
        } catch(...) {
            {std::lock_guard<std::mutex> lock(alarm_mu_);alarm_stop_=true;}
            alarm_wake_.notify_all();
            if(alarm_worker_.joinable()) alarm_worker_.join();
            {std::lock_guard<std::mutex> lock(mu_);closed_=true;}
            wake_.notify_all();
            for(auto& t:workers_) t.join();
            throw;
        }
    }
    CounterDirectory(const CounterDirectory&)=delete;
    CounterDirectory& operator=(const CounterDirectory&)=delete;
    ~CounterDirectory() {
        {std::lock_guard<std::mutex> lock(mail_mu_);mail_stop_=true;}
        mail_wake_.notify_all();
        {std::lock_guard<std::mutex> lock(alarm_mu_);alarm_stop_=true;}
        if(mail_worker_.joinable()) mail_worker_.join();
        alarm_wake_.notify_all();
        if(alarm_worker_.joinable()) alarm_worker_.join();
        {std::lock_guard<std::mutex> lock(mu_);closed_=true;}
        wake_.notify_all();
        for(auto& t:workers_) t.join(); // drain accepted work
    }
    std::future<Reply> send(SyncKey k,std::string request,std::int64_t op,std::int64_t delta) {
        if(request.compare(0,11,"@principal:")==0)
            throw std::invalid_argument("reserved principal request identity");
        return send_authorized(std::move(k),std::move(request),op,delta,[]{});
    }
    // Trusted ingress only. The callback is bound to an authenticated
    // principal, exact object and operation by the gateway; it is never
    // guest code. Recheck on admission, worker entry, after guest execution
    // and before handing a receipt to the host. A policy change racing a
    // commit cannot undo it; this is not transactional revocation.
    std::future<Reply> send_authorized(SyncKey k,std::string request,std::int64_t op,
                                      std::int64_t delta,std::function<void()> authorize_request) {
        if(!authorize_request) throw std::invalid_argument("missing request authorization");
        validate(k);check(request);
        if(request.compare(0,7,"@alarm:")==0) throw std::invalid_argument("reserved alarm operation identity");
        if((op!=1 && op!=2) || (op==1 && (delta<=0 || delta>CounterTurn::maximum)) ||
           (op==2 && delta!=0)) throw std::invalid_argument("invalid command");
        authorize_request();
        const auto id=identity(k);
        return submit(id,[this,id,k=std::move(k),request=std::move(request),op,delta,
                          authorize_request=std::move(authorize_request)]{
            validate(k);authorize_request();
            const auto epoch=activate(id,k);
            try {
                auto invoke=guarded_guest(k);
                auto checked=[&](auto n,auto operation,auto d){
                    authorize_request();
                    auto result=invoke(n,operation,d);
                    authorize_request();
                    return result;
                };
                auto r=CounterTurn::run(store_,k,epoch,request,op,delta,checked);
                validate(k);authorize_request();
                if(store_.snapshot(k).epoch!=epoch) throw std::logic_error("stale publication epoch");
                return r;
            } catch(...) {
                retire(id);throw;
            }
        });
    }
    // An ACK from this separate API is a persisted admission, not completion.
    // Retry the exact identity/command to retrieve a pending sequence or
    // completed receipt. Ordinary send() remains a transient request path.
    std::future<SyncAdmission> admit(SyncKey k,std::string request,std::int64_t op,std::int64_t delta) {
        if(principal_mode_) throw std::logic_error("principal admission required");
        return admit_bound(std::move(k),std::move(request),op,delta,"",[]{});
    }
    // Trusted gateway supplies the authenticated principal and a live
    // capability check. The durable policy is separate from capability TTL.
    std::future<SyncAdmission> admit_bound(SyncKey k,std::string request,std::int64_t op,std::int64_t delta,
                                            std::string principal,std::function<void()> authorize_request) {
        validate(k);check(request);
        if(!authorize_request || principal_mode_!=!principal.empty() ||
           (!principal.empty() && (!mail_policy_ || request.compare(0,11,"@principal:")!=0)))
            throw std::logic_error("invalid principal admission");
        if(!principal.empty()) check(principal);
        if(request.compare(0,7,"@alarm:")==0 || (principal.empty() && request.compare(0,11,"@principal:")==0) || (op!=1 && op!=2) ||
           (op==1 && (delta<=0 || delta>CounterTurn::maximum)) || (op==2 && delta!=0))
            throw std::invalid_argument("invalid mailbox command");
        auto check_policy=[this,&principal,&k,op] {
            if(!principal.empty() && !mail_policy_(principal,k,op)) throw std::logic_error("principal policy denied");
        };
        authorize_request();check_policy();
        const auto id=identity(k);
        return submit(id,[this,id,k=std::move(k),request=std::move(request),op,delta,
                          principal=std::move(principal),authorize_request=std::move(authorize_request)]{
            auto authorized=[&]{
                validate(k);authorize_request();
                if(!principal.empty() && !mail_policy_(principal,k,op)) throw std::logic_error("principal policy denied");
            };
            authorized();
            const auto epoch=activate(id,k);
            try {
                auto r=store_.admit_mail(k,epoch,request,std::to_string(op)+":"+std::to_string(delta),CounterTurn::schema,principal);
                authorized();
                if(store_.snapshot(k).epoch!=epoch) throw std::logic_error("stale admission epoch");
                return r;
            } catch(...) {retire(id);throw;}
        });
    }
    std::exception_ptr mail_fault() {
        std::lock_guard<std::mutex> lock(mail_mu_);
        return mail_fault_;
    }
    // Trusted controls join the same FIFO as normal requests. A generation
    // CAS rejects stale retries, including retries following a lost reply.
    std::future<std::int64_t> set_alarm(SyncKey k,std::int64_t expected_generation,
                                        std::optional<std::int64_t> due_ms) {
        validate(k);
        if(expected_generation<0 || (due_ms && *due_ms<0)) throw std::invalid_argument("invalid alarm control");
        const auto id=identity(k);
        return submit(id,[this,id,k=std::move(k),expected_generation,due_ms]{
            validate(k);
            const auto epoch=activate(id,k);
            try {
                auto g=store_.set_alarm(k,epoch,expected_generation,due_ms);
                validate(k);
                if(store_.snapshot(k).epoch!=epoch) throw std::logic_error("stale alarm publication epoch");
                return g;
            } catch(...) {retire(id);throw;}
        });
    }
    std::future<std::int64_t> alarm_generation(SyncKey k) {
        validate(k);
        const auto id=identity(k);
        return submit(id,[this,id,k=std::move(k)]{
            validate(k);return store_.alarm_generation(k,activate(id,k));
        });
    }
    // Diagnostic for trusted supervisor; empty means no terminal fault seen.
    std::exception_ptr alarm_fault() {
        std::lock_guard<std::mutex> lock(alarm_mu_);
        return alarm_fault_;
    }
    // Trusted, authorized diagnostic only. Does not reserve a turn or
    // grant an ownership capability; the value may change immediately.
    bool is_active(const SyncKey& k) {
        validate(k);
        std::lock_guard<std::mutex> lock(mu_);
        sweep(Clock::now());
        const auto it=lanes_.find(identity(k));
        return it!=lanes_.end() && it->second.epoch!=0;
    }
    std::future<bool> passivate(SyncKey k) {
        validate(k);
        const auto id=identity(k);
        return submit(id,[this,id,k=std::move(k)]{validate(k);return retire(id);});
    }
};
} // namespace ores::durable
