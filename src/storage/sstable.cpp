#include "sstable.h"
#include <cstdio>
#include <unistd.h>
#include <vector>

// File format, one record after another, sorted by key:
// [key_len 4][key][deleted 1][value_len 4][value]

bool SSTable::Write(const std::string& path, const Memtable& mem) {
    std::string tmp = path + ".tmp";
    FILE* f = fopen(tmp.c_str(), "wb");
    if (!f) return false;

    for (const auto& kv : mem.Entries()) {
        const std::string& key = kv.first;
        const MemEntry& e = kv.second;
        uint32_t klen = key.size();
        uint32_t vlen = e.value.size();
        uint8_t del = e.deleted ? 1 : 0;
        if (fwrite(&klen, 4, 1, f) != 1 ||
            fwrite(key.data(), 1, klen, f) != klen ||
            fwrite(&del, 1, 1, f) != 1 ||
            fwrite(&vlen, 4, 1, f) != 1 ||
            fwrite(e.value.data(), 1, vlen, f) != vlen) {
            fclose(f);
            return false;
        }
    }

    fflush(f);
    fsync(fileno(f));
    fclose(f);
    // Rename is all-or-nothing, so we never end up with a half-written SSTable.
    return rename(tmp.c_str(), path.c_str()) == 0;
}

SSTable::SSTable(const std::string& path) : path_(path) {
    file_ = fopen(path.c_str(), "rb");
    if (!file_) return;

    // Scan the file once and remember where each key's value is.
    while (true) {
        uint32_t klen;
        if (fread(&klen, 4, 1, file_) != 1) break;
        if (klen > 64u * 1024 * 1024) break;
        std::vector<char> kbuf(klen);
        if (fread(kbuf.data(), 1, klen, file_) != klen) break;
        uint8_t del;
        if (fread(&del, 1, 1, file_) != 1) break;
        uint32_t vlen;
        if (fread(&vlen, 4, 1, file_) != 1) break;

        long offset = ftell(file_);
        if (fseek(file_, vlen, SEEK_CUR) != 0) break;
        index_[std::string(kbuf.data(), klen)] = {offset, vlen, del == 1};
    }
}

SSTable::~SSTable() {
    if (file_) fclose(file_);
}

Memtable::Result SSTable::Get(const std::string& key, std::string* out) {
    auto it = index_.find(key);
    if (it == index_.end()) return Memtable::Result::NOT_PRESENT;
    if (it->second.deleted) return Memtable::Result::DELETED;

    out->assign(it->second.value_len, '\0');
    fseek(file_, it->second.value_offset, SEEK_SET);
    if (fread(&(*out)[0], 1, it->second.value_len, file_) != it->second.value_len)
        return Memtable::Result::NOT_PRESENT;
    return Memtable::Result::FOUND;
}

void SSTable::ForEach(
    const std::function<void(const std::string&, const std::string&, bool)>& fn) {
    for (const auto& kv : index_) {
        std::string value;
        if (!kv.second.deleted) Get(kv.first, &value);
        fn(kv.first, value, kv.second.deleted);
    }
}