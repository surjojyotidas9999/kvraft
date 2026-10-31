#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
#include "raft/raft_node.h"

// Usage: ./raft_node <my_id> <addr1> <addr2> <addr3> ...
// Each addr is either "port" (on localhost) or "host:port".
// Example for a 3-node cluster on one machine:
//   ./raft_node 1 8001 8002 8003
int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "usage: " << argv[0] << " <my_id> <addr1> <addr2> [addr3 ...]\n";
        return 1;
    }
    int id = std::atoi(argv[1]);

    std::vector<Peer> cluster;
    for (int i = 2; i < argc; i++) {
        std::string spec = argv[i];
        Peer p;
        size_t colon = spec.rfind(':');
        if (colon == std::string::npos) {
            p.host = "127.0.0.1";
            p.port = std::atoi(spec.c_str());
        } else {
            p.host = spec.substr(0, colon);
            p.port = std::atoi(spec.substr(colon + 1).c_str());
        }
        cluster.push_back(p);
    }

    if (id < 1 || id > static_cast<int>(cluster.size())) {
        std::cerr << "my_id must be between 1 and " << cluster.size() << "\n";
        return 1;
    }

    std::string dir = "node" + std::to_string(id) + "_data";
    std::filesystem::create_directories(dir);

    RaftNode node(id, cluster, dir);
    std::cout << "node " << id << " started on port " << cluster[id - 1].port << std::endl;
    node.Run();
    return 0;
}