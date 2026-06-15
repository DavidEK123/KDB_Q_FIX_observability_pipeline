// sender.cpp — Synthetic market data generator, streams FIX over TCP
// Build: cmake -B build && cmake --build build
// Run:   ./build/sender --host 10.10.10.2 --port 9009 --rate 100

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <csignal>
#include <string>
#include <random>
#include <chrono>
#include <thread>
#include <unordered_map>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

// ---------------------------------------------------------------------------
// Minimal FIX encoder
// ---------------------------------------------------------------------------
// Custom tag numbers for payload fields:
//   Tag 55  = Symbol       (standard FIX)
//   Tag 54  = Side         (standard FIX: '1'=Buy, '2'=Sell; we use 'B'/'S')
//   Tag 44  = Price        (standard FIX)
//   Tag 38  = OrderQty     (standard FIX)
//   Tag 5001 = SendTimestampNs (custom)
//
// Wire format: tag=value\x01  (SOH delimiter)
// Framing: each message terminated by '\n' after the FIX CheckSum field.

static constexpr char SOH = '\x01';

static uint8_t fix_checksum(const char* data, size_t len) {
    uint32_t sum = 0;
    for (size_t i = 0; i < len; ++i) sum += static_cast<uint8_t>(data[i]);
    return static_cast<uint8_t>(sum & 0xFF);
}

// Builds a complete FIX message (without trailing '\n'; caller appends that).
static std::string fix_encode(int64_t seq, const char* symbol, char side,
                              double price, int qty, int64_t send_ts_ns) {
    // Body fields (everything between BeginString+BodyLength and CheckSum)
    char body[512];
    int blen = snprintf(body, sizeof(body),
        "35=W%c"        // MsgType = MarketDataSnapshot
        "49=SENDER%c"   // SenderCompID
        "56=RECEIVER%c" // TargetCompID
        "34=%ld%c"      // MsgSeqNum
        "52=%ld%c"      // SendingTime (ns epoch — non-standard but unambiguous)
        "55=%s%c"       // Symbol
        "54=%c%c"       // Side
        "44=%.4f%c"     // Price
        "38=%d%c"       // Qty
        "5001=%ld%c",   // SendTimestampNs
        SOH, SOH, SOH,
        (long)seq, SOH,
        (long)send_ts_ns, SOH,
        symbol, SOH,
        side, SOH,
        price, SOH,
        qty, SOH,
        (long)send_ts_ns, SOH);

    // Header: 8=FIX.4.4|9=<bodylen>|
    char header[64];
    int hlen = snprintf(header, sizeof(header), "8=FIX.4.4%c9=%d%c", SOH, blen, SOH);

    std::string msg;
    msg.reserve(hlen + blen + 16);
    msg.append(header, hlen);
    msg.append(body, blen);

    // Checksum over header+body
    uint8_t cs = fix_checksum(msg.data(), msg.size());
    char cstag[16];
    int cslen = snprintf(cstag, sizeof(cstag), "10=%03u%c", cs, SOH);
    msg.append(cstag, cslen);

    return msg;
}

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------
static volatile sig_atomic_t g_running = 1;
static void on_signal(int) { g_running = 0; }

// Send all bytes, handle partial writes
static bool send_all(int fd, const char* buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = ::send(fd, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static int64_t now_ns() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000LL + ts.tv_nsec;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char** argv) {
    // Defaults
    const char* host = "10.10.10.2";
    int port = 9009;
    int rate = 100;          // msgs/sec
    double buyBias = 0.70;
    uint64_t seed = 0;
    bool seed_set = false;

    for (int i = 1; i < argc; ++i) {
        if (!strcmp(argv[i], "--host") && i+1 < argc) host = argv[++i];
        else if (!strcmp(argv[i], "--port") && i+1 < argc) port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--rate") && i+1 < argc) rate = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--buyBias") && i+1 < argc) buyBias = atof(argv[++i]);
        else if (!strcmp(argv[i], "--seed") && i+1 < argc) { seed = strtoull(argv[++i], nullptr, 10); seed_set = true; }
    }
    if (rate <= 0) rate = 100;

    printf("[sender] host=%s port=%d rate=%d buyBias=%.2f\n", host, port, rate, buyBias);

    signal(SIGINT,  on_signal);
    signal(SIGTERM, on_signal);

    // Connect
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }
    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad address: %s\n", host);
        close(fd);
        return 1;
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }
    printf("[sender] connected to %s:%d\n", host, port);

    // Tickers and base prices
    struct Ticker { const char* sym; double px; };
    Ticker tickers[] = {
        {"PLTR", 25.0}, {"TSLA", 210.0}, {"AMD", 160.0},
        {"NVDA", 880.0}, {"COIN", 180.0}
    };
    constexpr int NTICKERS = 5;

    std::mt19937_64 rng(seed_set ? seed : static_cast<uint64_t>(now_ns()));
    std::uniform_int_distribution<int>    sym_dist(0, NTICKERS - 1);
    std::uniform_real_distribution<double> unit(0.0, 1.0);

    int64_t seq = 0;
    const int64_t interval_ns = 1'000'000'000LL / rate;

    while (g_running) {
        int64_t loop_start = now_ns();
        ++seq;

        // Pick symbol
        int ti = sym_dist(rng);
        Ticker& t = tickers[ti];

        // Random walk price (step proportional to price level)
        double step = t.px * 0.0005 * (unit(rng) * 2.0 - 1.0);
        t.px += step;
        if (t.px < 0.01) t.px = 0.01;

        // Side
        char side = (unit(rng) < buyBias) ? 'B' : 'S';

        // Qty
        int qty;
        if (side == 'B') {
            qty = 50 + static_cast<int>(unit(rng) * 451);  // [50..500]
        } else {
            qty = 10 + static_cast<int>(unit(rng) * 191);  // [10..200]
        }

        int64_t send_ts = now_ns();

        std::string msg = fix_encode(seq, t.sym, side, t.px, qty, send_ts);
        msg.push_back('\n');  // frame delimiter

        if (!send_all(fd, msg.data(), msg.size())) {
            fprintf(stderr, "[sender] send failed at seq=%ld, exiting\n", (long)seq);
            break;
        }

        if (seq % 200 == 0) {
            printf("[sender] seq=%ld sym=%s side=%c px=%.4f qty=%d\n",
                   (long)seq, t.sym, side, t.px, qty);
        }

        // Pace to target rate
        int64_t elapsed = now_ns() - loop_start;
        int64_t remaining = interval_ns - elapsed;
        if (remaining > 1000) {
            struct timespec sl;
            sl.tv_sec  = remaining / 1'000'000'000LL;
            sl.tv_nsec = remaining % 1'000'000'000LL;
            nanosleep(&sl, nullptr);
        }
    }

    close(fd);
    printf("[sender] stopped after seq=%ld\n", (long)seq);
    return 0;
}
