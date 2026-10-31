#pragma once
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

struct Peer {
    std::string host;
    int port;
};

using Clock = std::chrono::steady_clock;

// One node of a Raft cluster. This version does leader election and heartbeats.
// Messages are single text lines over TCP:
//   REQUEST_VOTE <term> <candidate_id>   ->  VOTE <term> <granted 0/1>
//   APPEND_ENTRIES <term> <leader_id>    ->  APPEND <term> <success 0/1>
//   STATUS                               ->  STATUS node=.. role=.. term=.. leader=..
class RaftNode {
public:
    enum class Role { FOLLOWER, CANDIDATE, LEADER };

    // id is 1..N. cluster[id-1] is this node's own address.
    RaftNode(int id, std::vector<Peer> cluster, const std::string& dir)
        : id_(id), cluster_(std::move(cluster)), state_path_(dir + "/raft_state"),
          rng_(std::random_device{}() + id) {
        LoadState();
        ResetElectionTimer();
    }

    // Starts the node. Never returns.
    void Run() {
        std::thread(&RaftNode::ListenLoop, this).detach();
        TickLoop();
    }

private:
    // ---------- timing and main loop ----------

    void TickLoop() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            bool start_election = false;
            bool send_heartbeat = false;
            {
                std::lock_guard<std::mutex> lock(mu_);
                auto now = Clock::now();
                if (role_ == Role::LEADER) {
                    if (now >= next_heartbeat_) {
                        send_heartbeat = true;
                        next_heartbeat_ = now + std::chrono::milliseconds(100);
                    }
                } else if (now >= election_deadline_) {
                    start_election = true;
                }
            }
            if (start_election) StartElection();
            if (send_heartbeat) SendHeartbeats();
        }
    }

    // Must be called with mu_ held.
    void ResetElectionTimer() {
        std::uniform_int_distribution<int> dist(500, 1000);
        election_deadline_ = Clock::now() + std::chrono::milliseconds(dist(rng_));
    }

    // ---------- elections ----------

    void StartElection() {
        long long election_term;
        {
            std::lock_guard<std::mutex> lock(mu_);
            term_++;
            role_ = Role::CANDIDATE;
            voted_for_ = id_;  // vote for myself
            leader_ = 0;
            votes_ = 1;
            PersistState();
            ResetElectionTimer();
            election_term = term_;
            Log("election timeout, starting election");
            if (votes_ * 2 > static_cast<int>(cluster_.size())) {  // single-node cluster
                BecomeLeader();
                return;
            }
        }

        for (size_t p = 0; p < cluster_.size(); p++) {
            if (static_cast<int>(p) + 1 == id_) continue;
            std::thread([this, p, election_term]() {
                std::string reply;
                std::string req = "REQUEST_VOTE " + std::to_string(election_term) + " " +
                                  std::to_string(id_);
                if (!Rpc(cluster_[p], req, &reply)) return;

                std::istringstream in(reply);
                std::string tag;
                long long reply_term;
                int granted;
                if (!(in >> tag >> reply_term >> granted) || tag != "VOTE") return;

                std::lock_guard<std::mutex> lock(mu_);
                if (reply_term > term_) {
                    BecomeFollower(reply_term);
                    return;
                }
                // Ignore votes that arrive after the election is already over.
                if (role_ != Role::CANDIDATE || term_ != election_term) return;
                if (granted) {
                    votes_++;
                    if (votes_ * 2 > static_cast<int>(cluster_.size())) BecomeLeader();
                }
            }).detach();
        }
    }

    // Must be called with mu_ held.
    void BecomeLeader() {
        role_ = Role::LEADER;
        leader_ = id_;
        next_heartbeat_ = Clock::now();  // send the first heartbeat right away
        Log("became LEADER");
    }

    // Must be called with mu_ held.
    void BecomeFollower(long long new_term) {
        if (role_ == Role::LEADER)
            Log("saw higher term " + std::to_string(new_term) + ", stepping down");
        term_ = new_term;
        voted_for_ = 0;
        role_ = Role::FOLLOWER;
        PersistState();
    }

    void SendHeartbeats() {
        long long t;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (role_ != Role::LEADER) return;
            t = term_;
        }
        for (size_t p = 0; p < cluster_.size(); p++) {
            if (static_cast<int>(p) + 1 == id_) continue;
            std::thread([this, p, t]() {
                std::string reply;
                std::string req = "APPEND_ENTRIES " + std::to_string(t) + " " + std::to_string(id_);
                if (!Rpc(cluster_[p], req, &reply)) return;

                std::istringstream in(reply);
                std::string tag;
                long long reply_term;
                int success;
                if (!(in >> tag >> reply_term >> success) || tag != "APPEND") return;

                std::lock_guard<std::mutex> lock(mu_);
                if (reply_term > term_) BecomeFollower(reply_term);
            }).detach();
        }
    }

    // ---------- handling messages from other nodes ----------

    std::string OnRequestVote(long long term, int candidate) {
        std::lock_guard<std::mutex> lock(mu_);
        if (term > term_) BecomeFollower(term);

        bool grant = false;
        if (term == term_ && (voted_for_ == 0 || voted_for_ == candidate)) {
            voted_for_ = candidate;
            PersistState();  // the vote must be on disk before we tell anyone about it
            ResetElectionTimer();
            grant = true;
        }
        return "VOTE " + std::to_string(term_) + " " + (grant ? "1" : "0");
    }

    std::string OnAppendEntries(long long term, int leader) {
        std::lock_guard<std::mutex> lock(mu_);
        if (term < term_) return "APPEND " + std::to_string(term_) + " 0";  // stale leader
        if (term > term_) BecomeFollower(term);

        role_ = Role::FOLLOWER;  // a candidate gives up when it hears from a real leader
        if (leader_ != leader) {
            leader_ = leader;
            Log("following leader node " + std::to_string(leader));
        }
        ResetElectionTimer();
        return "APPEND " + std::to_string(term_) + " 1";
    }

    std::string HandleLine(const std::string& line) {
        std::istringstream in(line);
        std::string cmd;
        in >> cmd;
        if (cmd == "REQUEST_VOTE") {
            long long term;
            int candidate;
            if (!(in >> term >> candidate)) return "ERROR bad request";
            return OnRequestVote(term, candidate);
        }
        if (cmd == "APPEND_ENTRIES") {
            long long term;
            int leader;
            if (!(in >> term >> leader)) return "ERROR bad request";
            return OnAppendEntries(term, leader);
        }
        if (cmd == "STATUS") {
            std::lock_guard<std::mutex> lock(mu_);
            return "STATUS node=" + std::to_string(id_) + " role=" + RoleName(role_) +
                   " term=" + std::to_string(term_) + " leader=" + std::to_string(leader_);
        }
        return "ERROR unknown command";
    }

    // ---------- networking ----------

    void ListenLoop() {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        int yes = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(cluster_[id_ - 1].port);
        if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            perror("bind (is another node already using this port?)");
            std::exit(1);
        }
        listen(fd, 64);
        while (true) {
            int client = accept(fd, nullptr, nullptr);
            if (client < 0) continue;
            std::thread(&RaftNode::HandleConn, this, client).detach();
        }
    }

    void HandleConn(int fd) {
        std::string buf;
        char tmp[1024];
        while (true) {
            ssize_t n = read(fd, tmp, sizeof(tmp));
            if (n <= 0) break;
            buf.append(tmp, n);
            size_t pos;
            while ((pos = buf.find('\n')) != std::string::npos) {
                std::string line = buf.substr(0, pos);
                buf.erase(0, pos + 1);
                if (!line.empty() && line.back() == '\r') line.pop_back();
                std::string reply = HandleLine(line) + "\n";
                send(fd, reply.data(), reply.size(), MSG_NOSIGNAL);
            }
            if (buf.size() > 65536) break;
        }
        close(fd);
    }

    // Sends one line to a peer and waits for one line back. Gives up after ~150ms.
    static bool Rpc(const Peer& peer, const std::string& request, std::string* reply) {
        addrinfo hints{};
        addrinfo* res = nullptr;
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;
        if (getaddrinfo(peer.host.c_str(), std::to_string(peer.port).c_str(), &hints, &res) != 0)
            return false;

        int fd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (fd < 0) {
            freeaddrinfo(res);
            return false;
        }
        timeval tv{0, 150000};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        bool connected = connect(fd, res->ai_addr, res->ai_addrlen) == 0;
        freeaddrinfo(res);
        if (!connected) {
            close(fd);
            return false;
        }

        std::string line = request + "\n";
        if (send(fd, line.data(), line.size(), MSG_NOSIGNAL) != static_cast<ssize_t>(line.size())) {
            close(fd);
            return false;
        }
        std::string buf;
        char c;
        while (true) {
            ssize_t n = read(fd, &c, 1);
            if (n <= 0) {
                close(fd);
                return false;
            }
            if (c == '\n') break;
            buf.push_back(c);
        }
        close(fd);
        *reply = buf;
        return true;
    }

    // ---------- saving state to disk ----------
    // Raft requires the current term and the vote to survive a crash.
    // Otherwise a restarted node could vote twice in one term.

    void PersistState() {
        std::string tmp = state_path_ + ".tmp";
        FILE* f = fopen(tmp.c_str(), "w");
        if (!f) {
            perror("persist");
            return;
        }
        fprintf(f, "%lld %d\n", term_, voted_for_);
        fflush(f);
        fsync(fileno(f));
        fclose(f);
        rename(tmp.c_str(), state_path_.c_str());  // all-or-nothing replace
    }

    void LoadState() {
        FILE* f = fopen(state_path_.c_str(), "r");
        if (!f) return;
        long long t;
        int v;
        if (fscanf(f, "%lld %d", &t, &v) == 2) {
            term_ = t;
            voted_for_ = v;
        }
        fclose(f);
    }

    // ---------- helpers ----------

    static const char* RoleName(Role r) {
        switch (r) {
            case Role::FOLLOWER: return "FOLLOWER";
            case Role::CANDIDATE: return "CANDIDATE";
            default: return "LEADER";
        }
    }

    // Call with mu_ held, so that log lines don't get mixed together.
    void Log(const std::string& msg) {
        double secs = std::chrono::duration<double>(Clock::now() - start_).count();
        std::printf("[%7.2fs] node %d (term %lld): %s\n", secs, id_, term_, msg.c_str());
        std::fflush(stdout);
    }

    int id_;
    std::vector<Peer> cluster_;
    std::string state_path_;
    std::mt19937 rng_;
    std::mutex mu_;
    Clock::time_point start_ = Clock::now();

    long long term_ = 0;
    int voted_for_ = 0;  // 0 means "nobody"
    int leader_ = 0;
    int votes_ = 0;
    Role role_ = Role::FOLLOWER;
    Clock::time_point election_deadline_;
    Clock::time_point next_heartbeat_;
};