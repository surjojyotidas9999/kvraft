#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cctype>
#include <cstdio>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include "../storage/db.h"

// A simple multi-client TCP server. Each client connection gets its own thread.
// A mutex makes sure only one thread touches the database at a time.
class Server {
public:
    Server(DB& db, int port) : db_(db), port_(port) {}

    bool Run() {
        int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) { perror("socket"); return false; }

        int yes = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(port_);

        if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            perror("bind");
            return false;
        }
        if (listen(listen_fd, 64) < 0) {
            perror("listen");
            return false;
        }

        std::cout << "kvraft server listening on port " << port_ << std::endl;
        while (true) {
            int client = accept(listen_fd, nullptr, nullptr);
            if (client < 0) continue;
            std::thread(&Server::HandleClient, this, client).detach();
        }
    }

private:
    void HandleClient(int fd) {
        std::string buf;
        char tmp[4096];
        while (true) {
            ssize_t n = read(fd, tmp, sizeof(tmp));
            if (n <= 0) break;  // client disconnected
            buf.append(tmp, n);

            // A command is one line. Handle every complete line we have.
            size_t pos;
            while ((pos = buf.find('\n')) != std::string::npos) {
                std::string line = buf.substr(0, pos);
                buf.erase(0, pos + 1);
                if (!line.empty() && line.back() == '\r') line.pop_back();

                bool quit = false;
                std::string reply = Execute(line, &quit) + "\n";
                if (!WriteAll(fd, reply) || quit) {
                    close(fd);
                    return;
                }
            }
            if (buf.size() > (1u << 20)) break;  // a line over 1 MB: drop the client
        }
        close(fd);
    }

    static bool WriteAll(int fd, const std::string& data) {
        size_t sent = 0;
        while (sent < data.size()) {
            ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) return false;
            sent += n;
        }
        return true;
    }

    std::string Execute(const std::string& line, bool* quit) {
        std::istringstream in(line);
        std::string cmd, key, value;
        in >> cmd >> key;
        for (char& c : cmd) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

        if (cmd == "PING") return "PONG";
        if (cmd == "QUIT") { *quit = true; return "BYE"; }

        std::lock_guard<std::mutex> lock(mu_);
        if (cmd == "PUT") {
            std::getline(in >> std::ws, value);
            if (key.empty() || value.empty()) return "ERROR usage: PUT <key> <value>";
            if (db_.Put(key, value)) return "OK";
            return "ERROR write failed";
        }
        if (cmd == "GET") {
            if (key.empty()) return "ERROR usage: GET <key>";
            std::string out;
            if (db_.Get(key, &out)) return "VALUE " + out;
            return "NOT_FOUND";
        }
        if (cmd == "DEL") {
            if (key.empty()) return "ERROR usage: DEL <key>";
            if (db_.Delete(key)) return "OK";
            return "ERROR write failed";
        }
        if (cmd.empty()) return "ERROR empty command";
        return "ERROR unknown command";
    }

    DB& db_;
    int port_;
    std::mutex mu_;
};