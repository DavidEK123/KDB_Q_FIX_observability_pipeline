// receiver.cpp — FIX receiver, appends quotes to CSV for kdb+ ingest
// Build: cmake -B build && cmake --build build
// Run:   ./build/receiver   (listens 0.0.0.0:9009)
//
// Uses file-ingest fallback (OPTION 2): writes ~/market-pipeline/inbox.csv
// which receiver.q tails every second.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <ctime>
#include <cmath>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <deque>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

// ---------------------------------------------------------------------------
// Minimal FIX decoder
// ---------------------------------------------------------------------------
static constexpr char SOH = '\x01';

struct FixFields {
    std::string symbol;
    char side = 0;
    double price = 0.0;
    int qty = 0;
    int64_t send_ts_ns = 0;
    int64_t seq = 0;
    bool valid = false;
    std::string error;
};

static uint8_t fix_checksum(const char* data, size_t len) {
    uint32_t sum = 0;
    for (size_t i = 0; i < len; ++i) sum += static_cast<uint8_t>(data[i]);
    return static_cast<uint8_t>(sum & 0xFF);
}

static FixFields fix_decode(const char* data, size_t len) {
    FixFields f;

    // Parse tag=value pairs separated by SOH
    std::unordered_map<int, std::string> tags;
    size_t i = 0;
    while (i < len) {
        // find '='
        size_t eq = i;
        while (eq < len && data[eq] != '=') ++eq;
        if (eq >= len) break;

        int tag = 0;
        for (size_t j = i; j < eq; ++j) {
            if (data[j] < '0' || data[j] > '9') { tag = -1; break; }
            tag = tag * 10 + (data[j] - '0');
        }

        // find SOH (or end)
        size_t val_start = eq + 1;
        size_t val_end = val_start;
        while (val_end < len && data[val_end] != SOH) ++val_end;

        if (tag > 0) {
            tags[tag] = std::string(data + val_start, val_end - val_start);
        }

        i = (val_end < len) ? val_end + 1 : val_end;
    }

    // Validate checksum: CheckSum(10) covers everything before "10=..."
    auto it10 = tags.find(10);
    if (it10 != tags.end()) {
        // Find "10=" in raw data to know where body ends
        const char* cs_pos = nullptr;
        for (size_t j = 0; j + 3 <= len; ++j) {
            if (data[j] == '1' && data[j+1] == '0' && data[j+2] == '=' &&
                (j == 0 || data[j-1] == SOH)) {
                cs_pos = data + j;
                break;
            }
        }
        if (cs_pos) {
            size_t body_len = cs_pos - data;
            uint8_t expected = fix_checksum(data, body_len);
            int received = atoi(it10->second.c_str());
            if (static_cast<int>(expected) != received) {
                f.error = "checksum mismatch: expected=" + std::to_string(expected) +
                          " received=" + std::to_string(received);
                return f;
            }
        }
    }

    // Extract fields
    auto get = [&](int t) -> const std::string* {
        auto it = tags.find(t);
        return (it != tags.end()) ? &it->second : nullptr;
    };

    const std::string* s;
    if ((s = get(55))) f.symbol = *s;
    if ((s = get(54))) f.side = (*s)[0];
    if ((s = get(44))) f.price = atof(s->c_str());
    if ((s = get(38))) f.qty = atoi(s->c_str());
    if ((s = get(5001))) f.send_ts_ns = atoll(s->c_str());
    if ((s = get(34)))  f.seq = atoll(s->c_str());

    f.valid = true;
    return f;
}

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------
static volatile sig_atomic_t g_running = 1;
static void on_signal(int) { g_running = 0; }

static int64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + ts.tv_nsec;
}

// ---------------------------------------------------------------------------
// Latency tracker (ring of recent latencies, last 5 seconds)
// ---------------------------------------------------------------------------
struct LatencyTracker {
    struct Sample { int64_t ts_ns; int64_t latency_ns; };
    std::deque<Sample> samples;

    void add(int64_t recv_ts, int64_t latency) {
        samples.push_back({recv_ts, latency});
    }

    void prune(int64_t cutoff_ns) {
        while (!samples.empty() && samples.front().ts_ns < cutoff_ns)
            samples.pop_front();
    }

    void report(int64_t now) {
        prune(now - 5'000'000'000LL);
        if (samples.empty()) { printf("  latency: no samples\n"); return; }

        std::vector<int64_t> v;
        v.reserve(samples.size());
        for (auto& s : samples) v.push_back(s.latency_ns);
        std::sort(v.begin(), v.end());

        auto pct = [&](double p) -> int64_t {
            size_t idx = static_cast<size_t>(p * (v.size() - 1));
            return v[idx];
        };

        printf("  latency (5s window, n=%zu): p50=%ldus p95=%ldus p99=%ldus max=%ldus\n",
               v.size(),
               pct(0.50) / 1000, pct(0.95) / 1000,
               pct(0.99) / 1000, v.back() / 1000);
    }
};

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    (void)argc; (void)argv;

    signal(SIGINT,  on_signal);
    signal(SIGTERM, on_signal);

    const std::unordered_set<std::string> VALID_SYMS = {"PLTR","TSLA","AMD","NVDA","COIN"};
    const int PORT = 9009;

    // Open CSV output
    const char* csv_path = "inbox.csv";
    FILE* csv_fp = fopen(csv_path, "a");
    if (!csv_fp) { perror("fopen inbox.csv"); return 1; }
    // Write header if file is empty
    fseek(csv_fp, 0, SEEK_END);
    if (ftell(csv_fp) == 0) {
        fprintf(csv_fp, "recv_ts_ns,send_ts_ns,sym,side,px,qty,seq\n");
    }
    setlinebuf(csv_fp);  // line-buffered

    printf("[receiver] CSV output: %s\n", csv_path);

    // Create listening socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);
    if (bind(listen_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(listen_fd); return 1;
    }
    if (listen(listen_fd, 1) < 0) {
        perror("listen"); close(listen_fd); return 1;
    }
    printf("[receiver] listening on 0.0.0.0:%d\n", PORT);

    while (g_running) {
        // Accept (with poll so we can check g_running)
        struct pollfd pfd = {listen_fd, POLLIN, 0};
        int pr = poll(&pfd, 1, 500);
        if (pr <= 0) continue;

        struct sockaddr_in client_addr{};
        socklen_t clen = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr*)&client_addr, &clen);
        if (client_fd < 0) { perror("accept"); continue; }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));
        printf("[receiver] client connected: %s\n", client_ip);

        int flag = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        // Per-connection state
        char buf[65536];
        std::string leftover;
        int64_t total_msgs = 0;
        int64_t seq_gaps = 0;
        int64_t last_seq = -1;
        int64_t last_report_ns = now_ns();
        LatencyTracker lat;

        while (g_running) {
            // Poll client_fd with 100ms timeout for timer checks
            struct pollfd cpfd = {client_fd, POLLIN, 0};
            int cpr = poll(&cpfd, 1, 100);

            if (cpr > 0 && (cpfd.revents & POLLIN)) {
                ssize_t n = recv(client_fd, buf, sizeof(buf), 0);
                if (n <= 0) {
                    printf("[receiver] client disconnected (n=%zd)\n", n);
                    break;
                }

                leftover.append(buf, n);

                // Extract newline-delimited frames
                size_t pos = 0;
                while (true) {
                    size_t nl = leftover.find('\n', pos);
                    if (nl == std::string::npos) break;

                    size_t frame_len = nl - pos;
                    if (frame_len == 0) { pos = nl + 1; continue; }

                    const char* frame = leftover.data() + pos;
                    int64_t recv_ts = now_ns();

                    FixFields f = fix_decode(frame, frame_len);

                    if (!f.valid) {
                        fprintf(stderr, "[receiver] DECODE ERROR: %s\n", f.error.c_str());
                        pos = nl + 1;
                        continue;
                    }

                    // Validate
                    bool ok = true;
                    if (VALID_SYMS.find(f.symbol) == VALID_SYMS.end()) {
                        fprintf(stderr, "[receiver] bad symbol: %s\n", f.symbol.c_str());
                        ok = false;
                    }
                    if (f.side != 'B' && f.side != 'S') {
                        fprintf(stderr, "[receiver] bad side: %c\n", f.side);
                        ok = false;
                    }
                    if (f.price <= 0) {
                        fprintf(stderr, "[receiver] bad price: %.4f\n", f.price);
                        ok = false;
                    }
                    if (f.qty <= 0) {
                        fprintf(stderr, "[receiver] bad qty: %d\n", f.qty);
                        ok = false;
                    }

                    if (!ok) { pos = nl + 1; continue; }

                    // Sequence monitoring
                    if (last_seq < 0) {
                        last_seq = f.seq;
                    } else if (f.seq != last_seq + 1) {
                        ++seq_gaps;
                        fprintf(stderr, "[receiver] SEQ GAP: expected=%ld actual=%ld\n",
                                (long)(last_seq + 1), (long)f.seq);
                        last_seq = f.seq;
                    } else {
                        last_seq = f.seq;
                    }

                    // Latency
                    int64_t latency = recv_ts - f.send_ts_ns;
                    lat.add(recv_ts, latency);

                    // Write to CSV
                    fprintf(csv_fp, "%ld,%ld,%s,%c,%.4f,%d,%ld\n",
                            (long)recv_ts, (long)f.send_ts_ns,
                            f.symbol.c_str(), f.side, f.price, f.qty, (long)f.seq);

                    ++total_msgs;
                    pos = nl + 1;
                }

                // Keep remainder
                if (pos > 0) leftover.erase(0, pos);
            }

            // 1-second status report
            int64_t now = now_ns();
            if (now - last_report_ns >= 1'000'000'000LL) {
                last_report_ns = now;
                printf("[receiver] total=%ld seq_gaps=%ld\n", (long)total_msgs, (long)seq_gaps);
                lat.report(now);
                fflush(csv_fp);
            }
        }

        close(client_fd);
    }

    fclose(csv_fp);
    close(listen_fd);
    printf("[receiver] stopped. total messages written to %s\n", csv_path);
    return 0;
}
