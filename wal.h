#pragma once
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>

enum class RecordType : uint8_t { PUT = 1, DELETE = 2 };

class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::string& path);
    ~WriteAheadLog();

    // Writes one record to disk and forces it to be saved. Returns true on success.
    bool Append(RecordType type, const std::string& key, const std::string& value);

    // Reads all valid records from the start of the file.
    // Stops at the first broken or half-written record and cuts it off.
    void Replay(const std::function<void(RecordType, const std::string&, const std::string&)>& fn);

private:
    std::string path_;
    FILE* file_;
};