#pragma once
#include <map>
#include <string>

struct MemEntry {
    std::string value;
    bool deleted;  // true = tombstone (a marker meaning "this key was deleted")
};

class Memtable {
public:
    enum class Result { NOT_PRESENT, FOUND, DELETED };

    void Put(const std::string& key, const std::string& value) {
        map_[key] = {value, false};
        size_ += key.size() + value.size();
    }

    void Delete(const std::string& key) {
        map_[key] = {"", true};
        size_ += key.size();
    }

    Result Get(const std::string& key, std::string* out) const {
        auto it = map_.find(key);
        if (it == map_.end()) return Result::NOT_PRESENT;
        if (it->second.deleted) return Result::DELETED;
        *out = it->second.value;
        return Result::FOUND;
    }

    size_t ApproximateSize() const { return size_; }
    const std::map<std::string, MemEntry>& Entries() const { return map_; }

private:
    std::map<std::string, MemEntry> map_;  // std::map keeps keys sorted
    size_t size_ = 0;
};