#pragma once
// Test-only SQLite VFS that injects genuine xWrite/xSync failures for one
// tenant's SQLite WAL file. NEVER register this VFS in a production host.
#include <sqlite3.h>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>

namespace ores::durable::test {
class FaultVfs final {
public:
    enum class Point : int { None, WalWrite, WalSync };
    explicit FaultVfs(std::string wal_filename) : target_(std::move(wal_filename)) {
        original_ = sqlite3_vfs_find(nullptr);
        if (!original_) throw std::runtime_error("SQLite VFS not found");
        proxy_ = *original_;
        proxy_.zName = "ores-test-fault-vfs";
        // Preserve underlying VFS pAppData for all forwarded callbacks.
        // This isolated test process admits one active fault VFS.
        FaultVfs* expected = nullptr;
        if (!active_.compare_exchange_strong(expected, this))
            throw std::logic_error("another fault VFS is already active");
        proxy_.pNext = nullptr;
        proxy_.szOsFile = static_cast<int>(offsetof(File, storage) + original_->szOsFile);
        proxy_.xOpen = &open;
        if (sqlite3_vfs_register(&proxy_, 1) != SQLITE_OK) {
            active_.store(nullptr, std::memory_order_release);
            throw std::runtime_error("SQLite fault VFS registration failed");
        }
        registered_ = true;
    }
    FaultVfs(const FaultVfs&) = delete;
    FaultVfs& operator=(const FaultVfs&) = delete;
    ~FaultVfs() {
        if (registered_) {
            (void)sqlite3_vfs_register(original_, 1);
            (void)sqlite3_vfs_unregister(&proxy_);
            active_.store(nullptr, std::memory_order_release);
        }
    }
    void arm(Point p) noexcept { point_.store(static_cast<int>(p), std::memory_order_release); }
    unsigned faults() const noexcept { return hits_.load(std::memory_order_acquire); }
private:
    struct File {
        sqlite3_file base{};
        sqlite3_file* real = nullptr;
        const sqlite3_io_methods* original = nullptr;
        FaultVfs* owner = nullptr;
        bool target = false;
        alignas(sqlite3_file) unsigned char storage[1];
    };
    static File* cast(sqlite3_file* p) noexcept { return reinterpret_cast<File*>(p); }
    static bool trigger(File* f, Point requested) noexcept {
        if (!f->target) return false;
        int expected = static_cast<int>(requested);
        if (!f->owner->point_.compare_exchange_strong(expected, static_cast<int>(Point::None),
                                                        std::memory_order_acq_rel)) return false;
        f->owner->hits_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    static int open(sqlite3_vfs*, const char* name, sqlite3_file* out,
                    int flags, int* out_flags) noexcept {
        auto* self = active_.load(std::memory_order_acquire);
        if (!self) return SQLITE_IOERR;
        auto* w = cast(out);
        w->real = reinterpret_cast<sqlite3_file*>(w->storage);
        w->owner = self;
        w->target = name && (flags & SQLITE_OPEN_WAL) != 0 &&
            std::strstr(name, self->target_.c_str()) != nullptr;
        const int result = self->original_->xOpen(self->original_, name, w->real, flags, out_flags);
        if (result != SQLITE_OK) return result;
        w->original = w->real->pMethods;
        w->base.pMethods = &methods();
        return SQLITE_OK;
    }
    static int close(sqlite3_file* p) noexcept { auto* f = cast(p); return f->original->xClose(f->real); }
    static int read(sqlite3_file* p, void* dst, int n, sqlite3_int64 offset) noexcept {
        auto* f = cast(p); return f->original->xRead(f->real,dst,n,offset);
    }
    static int write(sqlite3_file* p, const void* src, int n, sqlite3_int64 offset) noexcept {
        auto* f = cast(p);
        if (trigger(f,Point::WalWrite)) return SQLITE_IOERR_WRITE;
        return f->original->xWrite(f->real,src,n,offset);
    }
    static int truncate(sqlite3_file* p, sqlite3_int64 sz) noexcept {
        auto* f=cast(p);return f->original->xTruncate(f->real,sz);
    }
    static int sync(sqlite3_file* p, int flags) noexcept {
        auto* f=cast(p);
        if (trigger(f,Point::WalSync)) return SQLITE_IOERR_FSYNC;
        return f->original->xSync(f->real,flags);
    }
    static int file_size(sqlite3_file* p, sqlite3_int64* sz) noexcept {
        auto* f=cast(p);return f->original->xFileSize(f->real,sz);
    }
    static int lock(sqlite3_file* p,int v) noexcept {
        auto* f=cast(p);return f->original->xLock(f->real,v);
    }
    static int unlock(sqlite3_file* p,int v) noexcept {
        auto* f=cast(p);return f->original->xUnlock(f->real,v);
    }
    static int reserved(sqlite3_file* p,int* v) noexcept {
        auto* f=cast(p);return f->original->xCheckReservedLock(f->real,v);
    }
    static int control(sqlite3_file* p,int op,void* arg) noexcept {
        auto* f=cast(p);return f->original->xFileControl(f->real,op,arg);
    }
    static int sector(sqlite3_file* p) noexcept {
        auto* f=cast(p);return f->original->xSectorSize(f->real);
    }
    static int characteristics(sqlite3_file* p) noexcept {
        auto* f=cast(p);return f->original->xDeviceCharacteristics(f->real);
    }
    static int shm_map(sqlite3_file* p,int i,int n,int extend,void volatile** out) noexcept {
        auto* f=cast(p);return f->original->xShmMap(f->real,i,n,extend,out);
    }
    static int shm_lock(sqlite3_file* p,int offset,int n,int flags) noexcept {
        auto* f=cast(p);return f->original->xShmLock(f->real,offset,n,flags);
    }
    static void shm_barrier(sqlite3_file* p) noexcept {
        auto* f=cast(p);f->original->xShmBarrier(f->real);
    }
    static int shm_unmap(sqlite3_file* p,int v) noexcept {
        auto* f=cast(p);return f->original->xShmUnmap(f->real,v);
    }
    static int fetch(sqlite3_file* p,sqlite3_int64 offset,int n,void** out) noexcept {
        auto* f=cast(p);
        if(f->original->iVersion<3 || !f->original->xFetch) { *out=nullptr;return SQLITE_OK; }
        return f->original->xFetch(f->real,offset,n,out);
    }
    static int unfetch(sqlite3_file* p,sqlite3_int64 offset,void* ptr) noexcept {
        auto* f=cast(p);
        if(f->original->iVersion<3 || !f->original->xUnfetch) return SQLITE_OK;
        return f->original->xUnfetch(f->real,offset,ptr);
    }
    static const sqlite3_io_methods& methods() noexcept {
        static const sqlite3_io_methods io = {
            3, &close, &read, &write, &truncate, &sync, &file_size, &lock,
            &unlock, &reserved, &control, &sector, &characteristics,
            &shm_map, &shm_lock, &shm_barrier, &shm_unmap, &fetch, &unfetch
        };
        return io;
    }
    static inline std::atomic<FaultVfs*> active_{nullptr};
    sqlite3_vfs* original_ = nullptr;
    sqlite3_vfs proxy_{};
    std::string target_;
    std::atomic<int> point_{static_cast<int>(Point::None)};
    std::atomic<unsigned> hits_{0};
    bool registered_ = false;
};
} // namespace ores::durable::test
