#pragma once
// Linux/glibc trusted runner prototype: one isolated OS child per turn.
// No shell, no ambient environment, only an optional controller guard fd 3.
// The child script and artifact directory MUST be trusted and immutable.
// This is a wall-clock watchdog, not instruction fuel or a hostile OS sandbox.
#include "ores/durable/counter_directory.hpp"
#include <cerrno>
#include <charconv>
#include <csignal>
#include <poll.h>
#include <spawn.h>
#include <sys/wait.h>

namespace ores::durable {
class CounterProcessGuest final {
    std::string node_,script_,artifact_,digest_;
    std::chrono::milliseconds budget_;
    struct Fd {
        int value=-1;
        explicit Fd(int fd):value(fd){}
        Fd(const Fd&)=delete;
        ~Fd(){close();}
        void close(){if(value>=0){::close(value);value=-1;}}
    };
    struct Actions {
        posix_spawn_file_actions_t value;
        Actions(){check(posix_spawn_file_actions_init(&value));}
        ~Actions(){posix_spawn_file_actions_destroy(&value);}
        static void check(int rc){if(rc) throw std::runtime_error("guest spawn setup failed");}
    };
    struct Child {
        pid_t pid=-1;
        Child()=default;
        Child(const Child&)=delete;
        ~Child(){if(pid>0) stop();}
        // Never return control to the scheduler while the child is alive.
        // A lost waitpid owner is a fatal host invariant, not a retry.
        void stop() noexcept {
            if(::kill(pid,SIGKILL)!=0 && errno!=ESRCH) std::terminate();
            int status=0;
            while(::waitpid(pid,&status,0)<0) {
                if(errno!=EINTR) std::terminate();
            }
            pid=-1;
        }
        bool exited(int& status) {
            for(;;) {
                const auto r=::waitpid(pid,&status,WNOHANG);
                if(r==pid){pid=-1;return true;}
                if(r==0)return false;
                if(errno!=EINTR) std::terminate();
            }
        }
    };
    static void path(const std::string& p) {
        if(p.empty() || p.front()!='/' || p.find('\0')!=std::string::npos)
            throw std::invalid_argument("runner requires trusted absolute paths");
    }
public:
    CounterProcessGuest(std::string node,std::string script,std::string artifact,
                        std::string digest,std::chrono::milliseconds budget)
        :node_(std::move(node)),script_(std::move(script)),artifact_(std::move(artifact)),
         digest_(std::move(digest)),budget_(budget) {
        path(node_);if(!script_.empty()) path(script_);path(artifact_);
        if(digest_.size()!=64 || digest_.find_first_not_of("0123456789abcdef")!=std::string::npos ||
           budget_.count()<=0 || budget_.count()>60000)
            throw std::invalid_argument("invalid guest execution budget or digest");
    }
    // Native fuel worker uses the same bounded IPC and reaping protocol.
    static CounterProcessGuest fuel_worker(std::string executable,std::string artifact,
                                           std::string digest,std::chrono::milliseconds budget) {
        return CounterProcessGuest(std::move(executable),"",std::move(artifact),std::move(digest),budget);
    }
    std::int64_t operator()(std::int64_t n,std::int64_t op,std::int64_t d) const {
        if(n<0 || n>CounterTurn::maximum || (op!=1 && op!=2) ||
           d<0 || d>CounterTurn::maximum) throw std::invalid_argument("invalid guest input");
        int pipes[2];
        if(::pipe2(pipes,O_CLOEXEC)!=0) throw std::runtime_error("guest pipe failed");
        Fd read(pipes[0]),write(pipes[1]);
        if(read.value<3 || write.value<3)
            throw std::runtime_error("guest runner requires open standard descriptors");
        if(::fcntl(read.value,F_SETFL,O_NONBLOCK)<0) throw std::runtime_error("guest nonblocking pipe failed");
        Actions a;
        Actions::check(posix_spawn_file_actions_addopen(&a.value,0,"/dev/null",O_RDONLY,0));
        Actions::check(posix_spawn_file_actions_adddup2(&a.value,write.value,1));
        Actions::check(posix_spawn_file_actions_addopen(&a.value,2,"/dev/null",O_WRONLY,0));
        const int guard=CounterDirectory::current_guest_guard_fd();
        // Dup the *same open file description*, not a new open of the path.
        // The kernel keeps its flock alive even if the controller is killed.
        // fd 3 is reserved; the trusted worker must never unlock or close it.
        if(guard>=0) {
            if(guard<3) throw std::logic_error("invalid controller guard descriptor");
            Actions::check(posix_spawn_file_actions_adddup2(&a.value,guard,3));
        }
        Actions::check(posix_spawn_file_actions_addclosefrom_np(&a.value,guard>=0?4:3));
        std::vector<std::string> args{node_};
        if(!script_.empty()) args.push_back(script_);
        args.insert(args.end(),{artifact_,digest_,std::to_string(n),std::to_string(op),std::to_string(d)});
        if(script_.empty()) args.push_back(std::to_string(::getpid()));
        std::vector<char*> argv;
        for(auto& arg:args) argv.push_back(arg.data());
        argv.push_back(nullptr);
        char* env[]={nullptr};
        Child child;
        const auto deadline=CounterDirectory::Clock::now()+budget_;
        pid_t pid=-1;
        if(posix_spawn(&pid,node_.c_str(),&a.value,nullptr,argv.data(),env)!=0)
            throw std::runtime_error("guest spawn failed");
        child.pid=pid;write.close();
        std::string output;
        int status=0;bool done=false;
        for(;;) {
            if(CounterDirectory::Clock::now()>=deadline) {
                if(child.pid>0) child.stop();
                throw CounterDirectory::RetryableGuestFailure(CounterDirectory::FailureReason::deadline);
            }
            char buf[32];
            const auto count=::read(read.value,buf,sizeof(buf));
            if(count>0) {
                if(output.size()+static_cast<std::size_t>(count)>32)
                    throw std::runtime_error("oversized guest output");
                output.append(buf,static_cast<std::size_t>(count));
                continue;
            }
            if(count<0 && errno==EINTR)continue;
            if(count<0 && errno!=EAGAIN && errno!=EWOULDBLOCK)
                throw std::runtime_error("guest read failed");
            if(!done) done=child.exited(status);
            if(done && count==0)break;
            if(count==0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
            else {
                pollfd p{read.value,POLLIN,0};
                if(::poll(&p,1,5)<0 && errno!=EINTR)
                    throw std::runtime_error("guest poll failed");
            }
        }
        if(WIFEXITED(status) && (WEXITSTATUS(status)==73 || (script_.empty() && WEXITSTATUS(status)==75)))
            throw CounterDirectory::RetryableGuestFailure(
                WEXITSTATUS(status)==75 ? CounterDirectory::FailureReason::fuel : CounterDirectory::FailureReason::trap);
        if(!WIFEXITED(status) || WEXITSTATUS(status)!=0)
            throw std::runtime_error("guest worker failed");
        if(output.empty() || output.back()!='\n') throw std::runtime_error("guest output framing");
        output.pop_back();
        std::int64_t result=0;
        const auto p=std::from_chars(output.data(),output.data()+output.size(),result);
        if(p.ec!=std::errc{} || p.ptr!=output.data()+output.size() ||
           result<-1 || result>CounterTurn::maximum || std::to_string(result)!=output)
            throw std::runtime_error("guest output invalid");
        return result;
    }
};
} // namespace ores::durable
