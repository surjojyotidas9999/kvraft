#include "wal.h"
#include <cstring>
#include <unistd.h>
#include <vector>

// Checksum: a number computed from the data, used to detect corruption.
static uint32_t Crc32(const uint8_t* data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++)
            crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
    }
    return ~crc;
}

WriteAheadLog::WriteAheadLog(const std::string& path) : path_(path) {
    file_ = fopen(path.c_str(), "ab+");
}

WriteAheadLog::~WriteAheadLog() {
    if (file_) fclose(file_);
}

bool WriteAheadLog::Append(RecordType type, const std::string& key, const std::string& value,
                           bool sync) {
    if (!file_) return false;

    // Payload layout: [type 1][key_len 4][key][value_len 4][value]
    std::string payload;
    payload.push_back(static_cast<char>(type));
    uint32_t klen = key.size();
    uint32_t vlen = value.size();
    payload.append(reinterpret_cast<const char*>(&klen), 4);
    payload.append(key);
    payload.append(reinterpret_cast<const char*>(&vlen), 4);
    payload.append(value);

    // On disk: [crc 4][payload_len 4][payload]
    uint32_t len = payload.size();
    uint32_t crc = Crc32(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());

    if (fwrite(&crc, 4, 1, file_) != 1) return false;
    if (fwrite(&len, 4, 1, file_) != 1) return false;
    if (fwrite(payload.data(), 1, payload.size(), file_) != payload.size()) return false;
    if (fflush(file_) != 0) return false;                  // hand the data to the OS
    if (sync && fsync(fileno(file_)) != 0) return false;   // force it onto the disk
    return true;
}

bool WriteAheadLog::Sync() {
    if (!file_) return false;
    if (fflush(file_) != 0) return false;
    return fsync(fileno(file_)) == 0;
}

void WriteAheadLog::Replay(
    const std::function<void(RecordType, const std::string&, const std::string&)>& fn) {
    if (!file_) return;
    fseek(file_, 0, SEEK_SET);
    long valid_end = 0;

    while (true) {
        uint32_t crc, len;
        if (fread(&crc, 4, 1, file_) != 1) break;
        if (fread(&len, 4, 1, file_) != 1) break;
        if (len < 9 || len > 64u * 1024 * 1024) break;

        std::vector<char> buf(len);
        if (fread(buf.data(), 1, len, file_) != len) break;
        if (Crc32(reinterpret_cast<const uint8_t*>(buf.data()), len) != crc) break;

        RecordType type = static_cast<RecordType>(buf[0]);
        uint32_t klen;
        memcpy(&klen, buf.data() + 1, 4);
        if (5ull + klen + 4 > len) break;
        std::string key(buf.data() + 5, klen);
        uint32_t vlen;
        memcpy(&vlen, buf.data() + 5 + klen, 4);
        if (9ull + klen + vlen != len) break;
        std::string value(buf.data() + 9 + klen, vlen);

        fn(type, key, value);
        valid_end = ftell(file_);
    }

    // Cut off any broken tail so new records are not written after garbage.
    fflush(file_);
    if (ftruncate(fileno(file_), valid_end) != 0) {}
    fseek(file_, 0, SEEK_END);
}

bool WriteAheadLog::Reset() {
    if (file_) fclose(file_);
    FILE* f = fopen(path_.c_str(), "wb");  // this empties the file
    if (!f) { file_ = nullptr; return false; }
    fclose(f);
    file_ = fopen(path_.c_str(), "ab+");
    return file_ != nullptr;
}