#pragma once
#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include "memtable.h"
#include "sstable.h"
#include "wal.h"

class DB {
public:
    explicit DB(const std::string& dir, size_t flush_threshold = 4 * 1024 * 1024)
        : dir_(dir), flush_threshold_(flush_threshold), wal_(dir + "/data.wal") {
        // 1. Load the SSTable files that already exist, oldest first.
        std::vector<std::pair<int, std::string>> found;
        for (const auto& entry : std::filesystem::directory_iterator(dir_)) {
            std::string name = entry.path().filename().string();
            if (name.size() > 8 && name.rfind("sst_", 0) == 0 &&
                name.substr(name.size() - 4) == ".sst") {
                int id = std::stoi(name.substr(4, name.size() - 8));
                found.push_back({id, entry.path().string()});
            }
        }
        std::sort(found.begin(), found.end());
        for (const auto& f : found) {
            tables_.push_back(std::make_unique<SSTable>(f.second));
            next_id_ = f.first + 1;
        }

        // 2. Replay the log to rebuild whatever was only in memory.
        wal_.Replay([&](RecordType t, const std::string& k, const std::string& v) {
            if (t == RecordType::PUT) mem_.Put(k, v);
            else mem_.Delete(k);
        });
    }

    // sync = true: safe against power loss, but slow (one fsync per write).
    // sync = false: much faster; call Sync() later to make everything safe.
    bool Put(const std::string& key, const std::string& value, bool sync = true) {
        if (!wal_.Append(RecordType::PUT, key, value, sync)) return false;
        mem_.Put(key, value);
        MaybeFlush();
        return true;
    }

    bool Delete(const std::string& key, bool sync = true) {
        if (!wal_.Append(RecordType::DELETE, key, "", sync)) return false;
        mem_.Delete(key);
        MaybeFlush();
        return true;
    }

    // Forces all earlier writes onto the disk.
    bool Sync() { return wal_.Sync(); }

    // Look in memory first, then SSTables from newest to oldest.
    bool Get(const std::string& key, std::string* out) {
        Memtable::Result r = mem_.Get(key, out);
        if (r == Memtable::Result::FOUND) return true;
        if (r == Memtable::Result::DELETED) return false;

        for (auto it = tables_.rbegin(); it != tables_.rend(); ++it) {
            r = (*it)->Get(key, out);
            if (r == Memtable::Result::FOUND) return true;
            if (r == Memtable::Result::DELETED) return false;
        }
        return false;
    }

    // Writes the memtable to a new SSTable, then empties memory and the log.
    bool Flush() {
        if (mem_.Entries().empty()) return true;

        char name[32];
        snprintf(name, sizeof(name), "/sst_%06d.sst", next_id_);
        std::string path = dir_ + name;

        if (!SSTable::Write(path, mem_)) return false;
        tables_.push_back(std::make_unique<SSTable>(path));
        next_id_++;
        mem_ = Memtable();
        wal_.Reset();

        // Too many files makes reads slow, so merge them.
        if (tables_.size() >= 4) Compact();
        return true;
    }

    // Merges all SSTables into one. Newer values overwrite older ones.
    // Tombstones are kept on purpose: this makes a crash in the middle of
    // compaction safe (a deleted key can never come back to life).
    bool Compact() {
        if (tables_.size() < 2) return true;

        Memtable merged;
        for (auto& t : tables_) {  // oldest to newest
            t->ForEach([&](const std::string& k, const std::string& v, bool deleted) {
                if (deleted) merged.Delete(k);
                else merged.Put(k, v);
            });
        }

        std::string newest_path = tables_.back()->path();
        std::vector<std::string> old_paths;
        for (size_t i = 0; i + 1 < tables_.size(); i++)
            old_paths.push_back(tables_[i]->path());

        // Replace the newest file with the merged result (all-or-nothing rename),
        // and only then delete the older files.
        if (!SSTable::Write(newest_path, merged)) return false;
        tables_.clear();
        for (const auto& p : old_paths) std::remove(p.c_str());
        tables_.push_back(std::make_unique<SSTable>(newest_path));
        return true;
    }

    size_t TableCount() const { return tables_.size(); }

private:
    void MaybeFlush() {
        if (mem_.ApproximateSize() >= flush_threshold_) Flush();
    }

    std::string dir_;
    size_t flush_threshold_;
    int next_id_ = 1;
    WriteAheadLog wal_;
    Memtable mem_;
    std::vector<std::unique_ptr<SSTable>> tables_;
};