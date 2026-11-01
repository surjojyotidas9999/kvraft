#pragma once
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include "../storage/db.h"

struct Peer {
    std::string host;
    int port;
};

struct LogEntry {
    long long term;
    std::string cmd;  // "PUT key value", "DEL key" or "NOP"
};

using Clock = std::chrono::steady_clock;

// Commands travel between nodes as hex text, so spaces in values are safe.
inline std::string HexEncode(const std::string& s) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    out.reserve(s.size() * 2);
    for (unsigned char c : s) {
        out.push_back(digits[c >> 4]);
        out.push_back(digits[c & 15]);
    }
    return out;
}

inline bool HexDecode(const std::string& h, std::string* out) {
    if (h.size() % 2 != 0) return false;
    auto val = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        return -1;
    };
    out->clear();
    for (size_t i = 0; i < h.size(); i += 2) {
        int a = val(h[i]), b = val(h[i + 1]);
        if (a < 0 || b < 0) return false;
        out->push_back(static_cast<char>(a * 16 + b));
    }
    return true;
}

// One node of a Raft cluster: leader election, log replication, and a key-value
// state machine on top. One TCP port serves both other nodes and clients.
//
// Messages (one text line each):
//   REQUEST_VOTE <term> <candidate> <last_log_index> <last_log_term>
//   APPEND_ENTRIES <term> <leader> <prev_index> <prev_term> <commit> <n> [<term> <hexcmd>]...
//   PUT <key> <value> | GET <key> | DEL <key>     (clients; leader only)
//   STATUS | PING | LOCALGET <key>                (debugging)
class RaftNode {
public:
    enum class Role { FOLLOWER, CANDIDATE, LEADER };

    RaftNode(int id, std::vector<Peer> cluster, const std::string& dir)
        : id_(id), cluster_(std::move(cluster)),
          state_path_(dir + "/raft_state"), log_path_(dir + "/raft_log"),
          rng_(std::random_device{}() + id) {
        LoadState();
        LoadLog();

        // The key-value data is rebuilt from the log, so start with an empty database.
        std::string db_dir = dir + "/db";
        std::filesystem::remove_all(db_dir);
        std::filesystem::create_directories(db_dir);
        db_ = std::make_unique<DB>(db_dir);

        size_t n = cluster_.size();
        next_index_.assign(n, 1);
        match_index_.assign(n, 0);
        next_send_.assign(n, Clock::now());
        inflight_.assign(n, 0);
        ResetElectionTimer();
    }

    // Starts the node. Never returns.
    void Run() {
        std::thread(&RaftNode::ListenLoop, this).detach();
        std::thread(&RaftNode::ApplyLoop, this).detach();
        TickLoop();
    }

private:
    // ---------- log helpers (call with mu_ held) ----------

    long long LastIndex() const { return static_cast<long long>(log_.size()); }

    long long TermAt(long long i) const {
        if (i <= 0 || i > LastIndex()) return 0;
        return log_[i - 1].term;
    }

    // On disk: [term 8][len 4][cmd]
    static bool WriteRecord(FILE* f, const LogEntry& e) {
        uint64_t term = static_cast<uint64_t>(e.term);
        uint32_t len = static_cast<uint32_t>(e.cmd.size());
        return fwrite(&term, 8, 1, f) == 1 && fwrite(&len, 4, 1, f) == 1 &&
               fwrite(e.cmd.data(), 1, len, f) == len;
    }

    void SyncLogFile() {
        fflush(log_file_);
        fsync(fileno(log_file_));
    }

    void AppendToLog(const LogEntry& e) {
        log_.push_back(e);
        WriteRecord(log_file_, e);
        SyncLogFile();
    }

    // Rewrites the whole log file (used after the log was cut short).
    void RewriteLogFile() {
        if (log_file_) fclose(log_file_);
        std::string tmp = log_path_ + ".tmp";
        FILE* f = fopen(tmp.c_str(), "wb");
        if (f) {
            for (const auto& e : log_) WriteRecord(f, e);
            fflush(f);
            fsync(fileno(f));
            fclose(f);
            rename(tmp.c_str(), log_path_.c_str());  // all-or-nothing replace
        }
        log_file_ = fopen(log_path_.c_str(), "ab");
    }

    void LoadLog() {
        FILE* f = fopen(log_path_.c_str(), "rb");
        if (f) {
            while (true) {
                uint64_t term;
                uint32_t len;
                if (fread(&term, 8, 1, f) != 1) break;
                if (fread(&len, 4, 1, f) != 1) break;
                if (len == 0 || len > (64u << 20)) break;
                std::string cmd(len, '\0');
                if (fread(&cmd[0], 1, len, f) != len) break;
                log_.push_back(LogEntry{static_cast<long long>(term), cmd});
            }
            fclose(f);
        }
        RewriteLogFile();  // also drops a half-written record at the end
    }

    // ---------- timing and main loop ----------

    void TickLoop() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            bool start_election = false;
            {
                std::lock_guard<std::mutex> lock(mu_);
                if (role_ == Role::LEADER) {
                    ReplicateToPeers();
                } else if (Clock::now() >= election_deadline_) {
                    start_election = true;
                }
            }
            if (start_election) StartElection();
        }
    }

    void ResetElectionTimer() {
        std::uniform_int_distribution<int> dist(500, 1000);
        election_deadline_ = Clock::now() + std::chrono::milliseconds(dist(rng_));
    }

    // ---------- elections ----------

    void StartElection() {
        long long election_term, last_idx, last_term;
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
            last_idx = LastIndex();
            last_term = TermAt(last_idx);
            Log("election timeout, starting election");
            if (votes_ * 2 > static_cast<int>(cluster_.size())) {  // single-node cluster
                BecomeLeader();
                return;
            }
        }

        for (size_t p = 0; p < cluster_.size(); p++) {
            if (static_cast<int>(p) + 1 == id_) continue;
            std::thread([this, p, election_term, last_idx, last_term]() {
                std::string reply;
                std::string req = "REQUEST_VOTE " + std::to_string(election_term) + " " +
                                  std::to_string(id_) + " " + std::to_string(last_idx) + " " +
                                  std::to_string(last_term);
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
                if (role_ != Role::CANDIDATE || term_ != election_term) return;
                if (granted) {
                    votes_++;
                    if (votes_ * 2 > static_cast<int>(cluster_.size())) BecomeLeader();
                }
            }).detach();
        }
    }

    void BecomeLeader() {
        role_ = Role::LEADER;
        leader_ = id_;
        auto now = Clock::now();
        for (size_t p = 0; p < cluster_.size(); p++) {
            next_index_[p] = LastIndex() + 1;
            match_index_[p] = 0;
            next_send_[p] = now;
        }
        // A leader may only commit entries from its own term by counting replicas.
        // Committing this empty "no-op" entry commits everything before it as well.
        AppendToLog(LogEntry{term_, "NOP"});
        AdvanceCommit();
        Log("became LEADER");
    }

    void BecomeFollower(long long new_term) {
        if (role_ == Role::LEADER)
            Log("saw higher term " + std::to_string(new_term) + ", stepping down");
        term_ = new_term;
        voted_for_ = 0;
        leader_ = 0;
        role_ = Role::FOLLOWER;
        PersistState();
        applied_cv_.notify_all();  // wake clients that were waiting on this leader
    }

    // ---------- log replication (leader side) ----------

    void ReplicateToPeers() {
        auto now = Clock::now();
        for (size_t p = 0; p < cluster_.size(); p++) {
            if (static_cast<int>(p) + 1 == id_) continue;
            if (inflight_[p] || now < next_send_[p]) continue;

            long long next = next_index_[p];
            long long prev_idx = next - 1;
            long long prev_term = TermAt(prev_idx);
            long long last = std::min<long long>(LastIndex(), next + 99);  // up to 100 per message
            long long count = last >= next ? last - next + 1 : 0;

            std::ostringstream req;
            req << "APPEND_ENTRIES " << term_ << " " << id_ << " " << prev_idx << " " << prev_term
                << " " << commit_index_ << " " << count;
            for (long long i = next; i <= last; i++)
                req << " " << log_[i - 1].term << " " << HexEncode(log_[i - 1].cmd);

            inflight_[p] = 1;
            next_send_[p] = now + std::chrono::milliseconds(50);  // heartbeat interval
            long long sent_term = term_;
            std::string request = req.str();
            std::thread([this, p, sent_term, request]() {
                std::string reply;
                bool ok = Rpc(cluster_[p], request, &reply);
                OnAppendReply(p, sent_term, ok, reply);
            }).detach();
        }
    }

    void OnAppendReply(size_t p, long long sent_term, bool ok, const std::string& reply) {
        long long reply_term = 0, value = 0;
        int success = 0;
        bool parsed = false;
        if (ok) {
            std::istringstream in(reply);
            std::string tag;
            parsed = (in >> tag >> reply_term >> success >> value) && tag == "APPEND";
        }

        std::lock_guard<std::mutex> lock(mu_);
        inflight_[p] = 0;
        if (!parsed) return;  // network problem: try again on the next tick
        if (reply_term > term_) {
            BecomeFollower(reply_term);
            return;
        }
        if (role_ != Role::LEADER || term_ != sent_term) return;

        if (success) {
            if (value > match_index_[p]) match_index_[p] = value;
            next_index_[p] = match_index_[p] + 1;
            AdvanceCommit();
            if (next_index_[p] <= LastIndex()) next_send_[p] = Clock::now();  // more to send
        } else {
            // Follower's log doesn't match ours here: step back and retry right away.
            next_index_[p] = std::max<long long>(1, std::min(next_index_[p] - 1, value + 1));
            next_send_[p] = Clock::now();
        }
    }

    // An entry is committed when a majority of nodes have it.
    void AdvanceCommit() {
        int majority = static_cast<int>(cluster_.size()) / 2 + 1;
        for (long long n = LastIndex(); n > commit_index_; n--) {
            if (TermAt(n) != term_) break;
            int count = 1;  // the leader itself
            for (size_t p = 0; p < cluster_.size(); p++)
                if (static_cast<int>(p) + 1 != id_ && match_index_[p] >= n) count++;
            if (count >= majority) {
                commit_index_ = n;
                apply_cv_.notify_all();
                break;
            }
        }
    }

    // ---------- applying committed entries to the database ----------

    void ApplyLoop() {
        std::unique_lock<std::mutex> lock(mu_);
        while (true) {
            apply_cv_.wait(lock, [&] { return last_applied_ < commit_index_; });
            while (last_applied_ < commit_index_) {
                long long idx = last_applied_ + 1;
                std::string cmd = log_[idx - 1].cmd;
                lock.unlock();
                ApplyToDb(cmd);
                lock.lock();
                last_applied_ = idx;
                applied_cv_.notify_all();
            }
        }
    }

    // The raft log is the durable record, so the database itself doesn't need fsync.
    void ApplyToDb(const std::string& cmd) {
        std::istringstream in(cmd);
        std::string op, key, value;
        in >> op >> key;
        std::lock_guard<std::mutex> g(db_mu_);
        if (op == "PUT") {
            std::getline(in >> std::ws, value);
            db_->Put(key, value, false);
        } else if (op == "DEL") {
            db_->Delete(key, false);
        }
    }

    // ---------- client commands ----------

    std::string HandleClient(const std::string& cmd, const std::string& key,
                             const std::string& value) {
        std::string entry_cmd;
        if (cmd == "PUT") {
            if (key.empty() || value.empty()) return "ERROR usage: PUT <key> <value>";
            entry_cmd = "PUT " + key + " " + value;
        } else if (cmd == "DEL") {
            if (key.empty()) return "ERROR usage: DEL <key>";
            entry_cmd = "DEL " + key;
        } else {  // GET goes through the log too, which proves we are still the leader
            if (key.empty()) return "ERROR usage: GET <key>";
            entry_cmd = "NOP";
        }

        std::unique_lock<std::mutex> lock(mu_);
        if (role_ != Role::LEADER) {
            if (leader_ == 0) return "NOT_LEADER unknown";
            return "NOT_LEADER " + std::to_string(leader_);
        }

        long long my_term = term_;
        AppendToLog(LogEntry{my_term, entry_cmd});
        long long idx = LastIndex();
        for (size_t p = 0; p < cluster_.size(); p++) next_send_[p] = Clock::now();  // send now
        AdvanceCommit();

        bool done = applied_cv_.wait_for(lock, std::chrono::seconds(3), [&] {
            return last_applied_ >= idx || term_ != my_term || role_ != Role::LEADER;
        });
        if (!done || last_applied_ < idx || TermAt(idx) != my_term)
            return "ERROR not committed, retry";
        lock.unlock();

        if (cmd == "GET") {
            std::lock_guard<std::mutex> g(db_mu_);
            std::string out;
            if (db_->Get(key, &out)) return "VALUE " + out;
            return "NOT_FOUND";
        }
        return "OK";
    }

    // ---------- handling messages from other nodes ----------

    std::string OnRequestVote(long long term, int candidate, long long last_idx,
                              long long last_term) {
        std::lock_guard<std::mutex> lock(mu_);
        if (term > term_) BecomeFollower(term);

        // Only vote for a candidate whose log is at least as up to date as ours.
        long long my_last_term = TermAt(LastIndex());
        bool up_to_date = last_term > my_last_term ||
                          (last_term == my_last_term && last_idx >= LastIndex());

        bool grant = false;
        if (term == term_ && up_to_date && (voted_for_ == 0 || voted_for_ == candidate)) {
            voted_for_ = candidate;
            PersistState();  // the vote must be on disk before we tell anyone about it
            ResetElectionTimer();
            grant = true;
        }
        return "VOTE " + std::to_string(term_) + " " + (grant ? "1" : "0");
    }

    std::string OnAppendEntries(long long term, int leader, long long prev_idx,
                                long long prev_term, long long commit,
                                const std::vector<LogEntry>& entries) {
        std::lock_guard<std::mutex> lock(mu_);
        if (term < term_)  // message from an old leader
            return "APPEND " + std::to_string(term_) + " 0 " + std::to_string(LastIndex());
        if (term > term_) BecomeFollower(term);

        role_ = Role::FOLLOWER;
        if (leader_ != leader) {
            leader_ = leader;
            Log("following leader node " + std::to_string(leader));
        }
        ResetElectionTimer();

        // Our log must contain the entry just before the new ones, with the same term.
        if (prev_idx > LastIndex() || (prev_idx > 0 && TermAt(prev_idx) != prev_term)) {
            long long hint = std::min(LastIndex(), prev_idx - 1);
            return "APPEND " + std::to_string(term_) + " 0 " + std::to_string(hint);
        }

        size_t old_size = log_.size();
        bool truncated = false;
        long long idx = prev_idx;
        for (const auto& e : entries) {
            idx++;
            if (idx <= LastIndex()) {
                if (TermAt(idx) == e.term) continue;  // already have this entry
                Log("log conflict at index " + std::to_string(idx) + ", truncating");
                log_.resize(idx - 1);  // delete the conflicting entry and everything after
                truncated = true;
            }
            log_.push_back(e);
        }

        if (truncated) {
            RewriteLogFile();
        } else if (log_.size() > old_size) {
            for (size_t i = old_size; i < log_.size(); i++) WriteRecord(log_file_, log_[i]);
            SyncLogFile();
        }
        if (log_.size() != old_size || truncated)
            Log("log now has " + std::to_string(LastIndex()) + " entries");

        long long last_new = prev_idx + static_cast<long long>(entries.size());
        long long new_commit = std::min(commit, last_new);
        if (new_commit > commit_index_) {
            commit_index_ = new_commit;
            apply_cv_.notify_all();
        }
        return "APPEND " + std::to_string(term_) + " 1 " + std::to_string(last_new);
    }

    std::string HandleLine(const std::string& line) {
        std::istringstream in(line);
        std::string cmd;
        in >> cmd;
        for (char& c : cmd) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

        if (cmd == "REQUEST_VOTE") {
            long long term, last_idx, last_term;
            int candidate;
            if (!(in >> term >> candidate >> last_idx >> last_term)) return "ERROR bad request";
            return OnRequestVote(term, candidate, last_idx, last_term);
        }
        if (cmd == "APPEND_ENTRIES") {
            long long term, prev_idx, prev_term, commit, n;
            int leader;
            if (!(in >> term >> leader >> prev_idx >> prev_term >> commit >> n))
                return "ERROR bad request";
            if (n < 0 || n > 1000) return "ERROR bad request";
            std::vector<LogEntry> entries;
            for (long long i = 0; i < n; i++) {
                long long t;
                std::string hex, text;
                if (!(in >> t >> hex) || !HexDecode(hex, &text)) return "ERROR bad request";
                entries.push_back(LogEntry{t, text});
            }
            return OnAppendEntries(term, leader, prev_idx, prev_term, commit, entries);
        }
        if (cmd == "PING") return "PONG";
        if (cmd == "STATUS") {
            std::lock_guard<std::mutex> lock(mu_);
            return "STATUS node=" + std::to_string(id_) + " role=" + RoleName(role_) +
                   " term=" + std::to_string(term_) + " leader=" + std::to_string(leader_) +
                   " log=" + std::to_string(LastIndex()) +
                   " commit=" + std::to_string(commit_index_) +
                   " applied=" + std::to_string(last_applied_);
        }
        if (cmd == "PUT" || cmd == "GET" || cmd == "DEL") {
            std::string key, value;
            in >> key;
            std::getline(in >> std::ws, value);
            return HandleClient(cmd, key, value);
        }
        if (cmd == "LOCALGET") {  // debugging: read this node's own copy, on any node
            std::string key, out;
            in >> key;
            std::lock_guard<std::mutex> g(db_mu_);
            if (db_->Get(key, &out)) return "VALUE " + out;
            return "NOT_FOUND";
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
        char tmp[4096];
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
            if (buf.size() > (8u << 20)) break;
        }
        close(fd);
    }

    // Sends one line to a peer and waits for one line back. Gives up after ~300ms.
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
        timeval tv{0, 300000};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        bool connected = connect(fd, res->ai_addr, res->ai_addrlen) == 0;
        freeaddrinfo(res);
        if (!connected) {
            close(fd);
            return false;
        }

        std::string line = request + "\n";
        size_t sent = 0;
        while (sent < line.size()) {
            ssize_t n = send(fd, line.data() + sent, line.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) {
                close(fd);
                return false;
            }
            sent += n;
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

    // ---------- saving term and vote to disk ----------
    // Without this, a restarted node could vote twice in the same term.

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
    std::string log_path_;
    std::mt19937 rng_;
    std::mutex mu_;  // protects everything below except db_
    Clock::time_point start_ = Clock::now();

    // persistent state
    long long term_ = 0;
    int voted_for_ = 0;  // 0 means "nobody"
    std::vector<LogEntry> log_;  // log_[0] is entry number 1
    FILE* log_file_ = nullptr;

    // volatile state
    int leader_ = 0;
    int votes_ = 0;
    Role role_ = Role::FOLLOWER;
    long long commit_index_ = 0;
    long long last_applied_ = 0;
    Clock::time_point election_deadline_;

    // leader-only state, one slot per node
    std::vector<long long> next_index_;
    std::vector<long long> match_index_;
    std::vector<Clock::time_point> next_send_;
    std::vector<char> inflight_;

    std::condition_variable apply_cv_;    // wakes the applier thread
    std::condition_variable applied_cv_;  // wakes clients waiting for their command

    std::mutex db_mu_;
    std::unique_ptr<DB> db_;
};