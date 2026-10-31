#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include "net/server.h"

int main(int argc, char** argv) {
    int port = argc > 1 ? std::atoi(argv[1]) : 7000;
    std::string dir = argc > 2 ? argv[2] : "data";

    std::filesystem::create_directories(dir);
    DB db(dir);

    Server server(db, port);
    return server.Run() ? 0 : 1;
}