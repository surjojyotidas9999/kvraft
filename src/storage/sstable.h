#pragma once
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <string>
#include "memtable.h"

class SSTable {
public:
    // Writes a memtable to a new sorted file on disk. Returns true on success.
    static bool Write(const std::string& path, const Memtable& mem);

    explicit SSTable(const std::string& path);
    ~SSTable();
    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;

    Memtable::Result Get(const std::string& key, std::string* out);

    // Calls fn(key, value, is_deleted) for every entry, in sorted key order.
    void ForEach(const std::function<void(const std::string&, const std::string&, bool)>& fn);

    const std::string& path() const { return path_; }

private:
    struct IndexEntry {
        long value_offset;
        uint32_t value_len;
        bool deleted;
    };
    std::string path_;
    FILE* file_;
    std::map<std::string, IndexEntry> index_;  // key -> where its value sits in the file
};