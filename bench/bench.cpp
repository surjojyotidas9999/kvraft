#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <random>
#include <string>
#include "../src/storage/db.h"

using Clock = std::chrono::steady_clock;

static double Seconds(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double>(b - a).count();
}

// sync_every = 1: fsync after every write
// sync_every = N: fsync after every N writes
// sync_every = 0: fsync only once at the very end
static double RunWrites(int n, int sync_every) {
    std::string dir = "bench_data";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);
    {
        DB db(dir, 64 * 1024);
        std::string value(100, 'v');
        auto t0 = Clock::now();
        for (int i = 0; i < n; i++) {
            bool sync = (sync_every == 1) || (sync_every > 1 && (i + 1) % sync_every == 0);
            db.Put("user" + std::to_string(i), value, sync);
        }
        if (sync_every != 1) db.Sync();  // make sure everything is on disk
        auto t1 = Clock::now();
        std::filesystem::remove_all(dir);
        return n / Seconds(t0, t1);
    }
}

int main(int argc, char** argv) {
    int n = argc > 1 ? std::atoi(argv[1]) : 20000;

    std::cout << "--- writes ---\n";
    std::cout << "fsync every write:    " << (long)RunWrites(2000, 1) << " ops/sec   (2000 keys)\n";
    std::cout << "fsync every 100:      " << (long)RunWrites(n, 100) << " ops/sec   (" << n << " keys)\n";
    std::cout << "fsync once at end:    " << (long)RunWrites(n, 0) << " ops/sec   (" << n << " keys)\n";

    // Reads: fill a database, then read from it.
    std::string dir = "bench_data";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directory(dir);
    {
        DB db(dir, 64 * 1024);
        std::string value(100, 'v');
        for (int i = 0; i < n; i++) db.Put("user" + std::to_string(i), value, false);
        db.Sync();

        std::string out;
        std::mt19937 rng(42);
        std::uniform_int_distribution<int> pick(0, n - 1);

        auto t0 = Clock::now();
        int found = 0;
        for (int i = 0; i < n; i++)
            if (db.Get("user" + std::to_string(pick(rng)), &out)) found++;
        auto t1 = Clock::now();
        int missing = 0;
        for (int i = 0; i < n; i++)
            if (!db.Get("nokey" + std::to_string(i), &out)) missing++;
        auto t2 = Clock::now();

        std::cout << "--- reads ---\n";
        std::cout << "sstable files at end: " << db.TableCount() << "\n";
        std::cout << "reads (hit):  " << (long)(n / Seconds(t0, t1)) << " ops/sec   (found " << found << ")\n";
        std::cout << "reads (miss): " << (long)(n / Seconds(t1, t2)) << " ops/sec   (missing " << missing << ")\n";
    }
    std::filesystem::remove_all(dir);
}