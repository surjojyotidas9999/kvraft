#include <cstdlib>
#include <iostream>
#include <sstream>
#include "storage/db.h"

int main() {
    DB db(".", 4096);  // flush to disk every ~4 KB (small, so we can watch it happen)
    std::cout << "kvraft shell. Commands: put <key> <value> | get <key> | del <key> | flush | compact | stats | fill <n> | quit\n";

    std::string line;
    while (std::cout << "> " && std::getline(std::cin, line)) {
        std::istringstream in(line);
        std::string cmd, key, value;
        in >> cmd >> key;

        if (cmd == "quit" || cmd == "exit") {
            break;
        } else if (cmd == "put") {
            std::getline(in >> std::ws, value);
            if (key.empty() || value.empty()) {
                std::cout << "usage: put <key> <value>\n";
            } else {
                std::cout << (db.Put(key, value) ? "OK" : "ERROR") << "\n";
            }
        } else if (cmd == "get") {
            std::string out;
            if (key.empty()) std::cout << "usage: get <key>\n";
            else if (db.Get(key, &out)) std::cout << out << "\n";
            else std::cout << "(not found)\n";
        } else if (cmd == "del") {
            if (key.empty()) std::cout << "usage: del <key>\n";
            else std::cout << (db.Delete(key) ? "OK" : "ERROR") << "\n";
        } else if (cmd == "flush") {
            std::cout << (db.Flush() ? "flushed" : "ERROR") << "\n";
        } else if (cmd == "compact") {
            std::cout << (db.Compact() ? "compacted" : "ERROR") << "\n";
        } else if (cmd == "stats") {
            std::cout << "sstable files: " << db.TableCount() << "\n";
        } else if (cmd == "fill") {
            int n = key.empty() ? 0 : std::atoi(key.c_str());
            for (int i = 0; i < n; i++)
                db.Put("key" + std::to_string(i), std::string(100, 'x') + std::to_string(i));
            std::cout << "wrote " << n << " keys\n";
        } else if (!cmd.empty()) {
            std::cout << "unknown command\n";
        }
    }
}