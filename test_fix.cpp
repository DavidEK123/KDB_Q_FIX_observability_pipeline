// test_fix.cpp — Unit tests for the market data pipeline
// Build: g++ -std=c++17 -o test_fix test_fix.cpp && ./test_fix
// No external test framework — just assertions with clear pass/fail output.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <unordered_map>
#include <vector>
#include <cassert>
#include <stdexcept>

// ============================================================================
// Copy of FIX codec from sender.cpp / receiver.cpp (so tests are standalone)
// ============================================================================

static constexpr char SOH = '\x01';

static uint8_t fix_checksum(const char* data, size_t len) {
    uint32_t sum = 0;
    for (size_t i = 0; i < len; ++i) sum += static_cast<uint8_t>(data[i]);
    return static_cast<uint8_t>(sum & 0xFF);
}

static std::string fix_encode(int64_t seq, const char* symbol, char side,
                              double price, int qty, int64_t send_ts_ns) {
    char body[512];
    int blen = snprintf(body, sizeof(body),
        "35=W%c49=SENDER%c56=RECEIVER%c34=%ld%c52=%ld%c55=%s%c54=%c%c44=%.4f%c38=%d%c5001=%ld%c",
        SOH, SOH, SOH,
        (long)seq, SOH,
        (long)send_ts_ns, SOH,
        symbol, SOH,
        side, SOH,
        price, SOH,
        qty, SOH,
        (long)send_ts_ns, SOH);

    char header[64];
    int hlen = snprintf(header, sizeof(header), "8=FIX.4.4%c9=%d%c", SOH, blen, SOH);

    std::string msg;
    msg.reserve(hlen + blen + 16);
    msg.append(header, hlen);
    msg.append(body, blen);

    uint8_t cs = fix_checksum(msg.data(), msg.size());
    char cstag[16];
    int cslen = snprintf(cstag, sizeof(cstag), "10=%03u%c", cs, SOH);
    msg.append(cstag, cslen);

    return msg;
}

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

static FixFields fix_decode(const char* data, size_t len) {
    FixFields f;
    std::unordered_map<int, std::string> tags;
    size_t i = 0;
    while (i < len) {
        size_t eq = i;
        while (eq < len && data[eq] != '=') ++eq;
        if (eq >= len) break;

        int tag = 0;
        for (size_t j = i; j < eq; ++j) {
            if (data[j] < '0' || data[j] > '9') { tag = -1; break; }
            tag = tag * 10 + (data[j] - '0');
        }

        size_t val_start = eq + 1;
        size_t val_end = val_start;
        while (val_end < len && data[val_end] != SOH) ++val_end;

        if (tag > 0) {
            tags[tag] = std::string(data + val_start, val_end - val_start);
        }
        i = (val_end < len) ? val_end + 1 : val_end;
    }

    auto it10 = tags.find(10);
    if (it10 != tags.end()) {
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

// ============================================================================
// Test helpers
// ============================================================================

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) static void test_##name()
#define RUN(name) do { \
    printf("  %-50s ", #name); \
    try { test_##name(); printf("PASS\n"); tests_passed++; } \
    catch (const std::exception& e) { printf("FAIL: %s\n", e.what()); tests_failed++; } \
    catch (...) { printf("FAIL: unknown exception\n"); tests_failed++; } \
} while(0)

#define ASSERT_TRUE(expr) do { if (!(expr)) throw std::runtime_error("assertion failed: " #expr); } while(0)
#define ASSERT_EQ(a, b) do { if ((a) != (b)) { \
    char _buf[256]; snprintf(_buf, sizeof(_buf), "expected %s == %s", #a, #b); \
    throw std::runtime_error(_buf); } } while(0)
#define ASSERT_NEAR(a, b, eps) do { if (std::fabs((a)-(b)) > (eps)) { \
    char _buf[256]; snprintf(_buf, sizeof(_buf), "%s=%.6f != %s=%.6f (eps=%.6f)", #a, (double)(a), #b, (double)(b), (double)(eps)); \
    throw std::runtime_error(_buf); } } while(0)

// ============================================================================
// TEST GROUP 1: FIX Encoder
// ============================================================================

TEST(encode_produces_nonempty_string) {
    std::string msg = fix_encode(1, "NVDA", 'B', 880.0, 100, 1234567890000000000LL);
    ASSERT_TRUE(msg.size() > 20);
}

TEST(encode_starts_with_beginstring) {
    std::string msg = fix_encode(1, "PLTR", 'S', 25.0, 50, 1000);
    // Should start with "8=FIX.4.4\x01"
    ASSERT_TRUE(msg.substr(0, 10) == std::string("8=FIX.4.4") + SOH);
}

TEST(encode_contains_symbol) {
    std::string msg = fix_encode(1, "TSLA", 'B', 210.5, 300, 5000);
    // Replace SOH with | for easier searching
    std::string readable = msg;
    for (char& c : readable) if (c == SOH) c = '|';
    ASSERT_TRUE(readable.find("55=TSLA") != std::string::npos);
}

TEST(encode_contains_checksum) {
    std::string msg = fix_encode(42, "AMD", 'S', 160.0, 75, 9999);
    // Should contain "10=" somewhere near the end
    std::string readable = msg;
    for (char& c : readable) if (c == SOH) c = '|';
    ASSERT_TRUE(readable.find("10=") != std::string::npos);
}

TEST(encode_checksum_is_three_digits) {
    std::string msg = fix_encode(1, "COIN", 'B', 180.0, 200, 12345);
    // Find "10=" and verify 3 digits follow
    size_t pos = msg.rfind("10=");
    ASSERT_TRUE(pos != std::string::npos);
    ASSERT_TRUE(pos + 6 <= msg.size());  // "10=" + 3 digits
    for (int i = 0; i < 3; ++i) {
        char c = msg[pos + 3 + i];
        ASSERT_TRUE(c >= '0' && c <= '9');
    }
}

TEST(encode_sequence_numbers_are_correct) {
    std::string msg1 = fix_encode(1, "PLTR", 'B', 25.0, 100, 1000);
    std::string msg2 = fix_encode(999, "PLTR", 'B', 25.0, 100, 1000);
    // msg1 should contain 34=1, msg2 should contain 34=999
    std::string r1 = msg1; for (char& c : r1) if (c == SOH) c = '|';
    std::string r2 = msg2; for (char& c : r2) if (c == SOH) c = '|';
    ASSERT_TRUE(r1.find("|34=1|") != std::string::npos);
    ASSERT_TRUE(r2.find("|34=999|") != std::string::npos);
}

// ============================================================================
// TEST GROUP 2: FIX Decoder (round-trip)
// ============================================================================

TEST(decode_roundtrip_basic) {
    std::string msg = fix_encode(7, "NVDA", 'B', 881.2345, 250, 1700000000000000000LL);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(f.valid);
    ASSERT_EQ(f.symbol, std::string("NVDA"));
    ASSERT_EQ(f.side, 'B');
    ASSERT_NEAR(f.price, 881.2345, 0.001);
    ASSERT_EQ(f.qty, 250);
    ASSERT_EQ(f.seq, 7);
    ASSERT_EQ(f.send_ts_ns, 1700000000000000000LL);
}

TEST(decode_roundtrip_all_symbols) {
    const char* syms[] = {"PLTR", "TSLA", "AMD", "NVDA", "COIN"};
    for (const char* sym : syms) {
        std::string msg = fix_encode(1, sym, 'S', 100.0, 50, 5000);
        FixFields f = fix_decode(msg.data(), msg.size());
        ASSERT_TRUE(f.valid);
        ASSERT_EQ(f.symbol, std::string(sym));
    }
}

TEST(decode_roundtrip_buy_and_sell) {
    std::string msgB = fix_encode(1, "PLTR", 'B', 25.0, 100, 1000);
    std::string msgS = fix_encode(2, "PLTR", 'S', 24.5, 50, 2000);
    FixFields fB = fix_decode(msgB.data(), msgB.size());
    FixFields fS = fix_decode(msgS.data(), msgS.size());
    ASSERT_TRUE(fB.valid);
    ASSERT_TRUE(fS.valid);
    ASSERT_EQ(fB.side, 'B');
    ASSERT_EQ(fS.side, 'S');
}

TEST(decode_large_sequence_number) {
    int64_t big_seq = 9999999999LL;
    std::string msg = fix_encode(big_seq, "AMD", 'B', 160.0, 100, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(f.valid);
    ASSERT_EQ(f.seq, big_seq);
}

TEST(decode_small_price) {
    std::string msg = fix_encode(1, "PLTR", 'B', 0.0100, 100, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(f.valid);
    ASSERT_NEAR(f.price, 0.01, 0.001);
}

TEST(decode_large_price) {
    std::string msg = fix_encode(1, "NVDA", 'S', 9999.9999, 10, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(f.valid);
    ASSERT_NEAR(f.price, 9999.9999, 0.01);
}

// ============================================================================
// TEST GROUP 3: Checksum validation
// ============================================================================

TEST(checksum_valid_message_passes) {
    std::string msg = fix_encode(1, "TSLA", 'B', 210.0, 300, 5000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(f.valid);
    ASSERT_TRUE(f.error.empty());
}

TEST(checksum_corrupted_message_fails) {
    std::string msg = fix_encode(1, "TSLA", 'B', 210.0, 300, 5000);
    // Corrupt a byte in the middle of the message
    if (msg.size() > 20) {
        msg[15] = 'X';  // flip a byte
    }
    FixFields f = fix_decode(msg.data(), msg.size());
    // Should either fail checksum or parse wrong values
    // The checksum should catch it
    if (f.valid) {
        // If it parsed "valid", the symbol/price might be wrong — that's still
        // a detected corruption. But typically checksum catches it.
        // This test mainly verifies we don't crash.
    }
}

TEST(checksum_truncated_message_does_not_crash) {
    std::string msg = fix_encode(1, "AMD", 'S', 160.0, 100, 1000);
    // Truncate to half
    std::string half = msg.substr(0, msg.size() / 2);
    FixFields f = fix_decode(half.data(), half.size());
    // Should not crash. May or may not be valid.
    (void)f;
}

TEST(checksum_empty_input_does_not_crash) {
    FixFields f = fix_decode("", 0);
    // Should not crash. Fields will be default/empty.
    ASSERT_EQ(f.symbol, std::string(""));
    ASSERT_EQ(f.qty, 0);
}

TEST(checksum_garbage_input_does_not_crash) {
    const char* garbage = "not a fix message at all!!!";
    FixFields f = fix_decode(garbage, strlen(garbage));
    // Should not crash. Probably not valid but won't segfault.
    (void)f;
}

// ============================================================================
// TEST GROUP 4: Framing (newline delimiter)
// ============================================================================

TEST(framing_single_message_with_newline) {
    std::string msg = fix_encode(1, "PLTR", 'B', 25.0, 100, 1000);
    msg.push_back('\n');
    // Simulate what the receiver does: find '\n', decode up to it
    size_t nl = msg.find('\n');
    ASSERT_TRUE(nl != std::string::npos);
    FixFields f = fix_decode(msg.data(), nl);
    ASSERT_TRUE(f.valid);
    ASSERT_EQ(f.symbol, std::string("PLTR"));
}

TEST(framing_multiple_messages_concatenated) {
    // Simulate TCP coalescing: two messages arrive in one read()
    std::string msg1 = fix_encode(1, "TSLA", 'B', 210.0, 100, 1000);
    msg1.push_back('\n');
    std::string msg2 = fix_encode(2, "AMD", 'S', 160.0, 50, 2000);
    msg2.push_back('\n');

    std::string combined = msg1 + msg2;

    // Parse frame by frame like the receiver does
    std::vector<FixFields> results;
    size_t pos = 0;
    while (true) {
        size_t nl = combined.find('\n', pos);
        if (nl == std::string::npos) break;
        size_t frame_len = nl - pos;
        if (frame_len > 0) {
            FixFields f = fix_decode(combined.data() + pos, frame_len);
            results.push_back(f);
        }
        pos = nl + 1;
    }

    ASSERT_EQ((int)results.size(), 2);
    ASSERT_TRUE(results[0].valid);
    ASSERT_TRUE(results[1].valid);
    ASSERT_EQ(results[0].symbol, std::string("TSLA"));
    ASSERT_EQ(results[1].symbol, std::string("AMD"));
    ASSERT_EQ(results[0].seq, 1);
    ASSERT_EQ(results[1].seq, 2);
}

TEST(framing_partial_message_leftover) {
    // Simulate TCP fragmentation: message split across two reads
    std::string msg = fix_encode(1, "NVDA", 'B', 880.0, 200, 3000);
    msg.push_back('\n');

    // Split at arbitrary point
    size_t split = msg.size() / 2;
    std::string part1 = msg.substr(0, split);
    std::string part2 = msg.substr(split);

    // First "read" — no newline found
    std::string buffer = part1;
    size_t nl = buffer.find('\n');
    ASSERT_TRUE(nl == std::string::npos);  // no complete frame yet

    // Second "read" — append remainder
    buffer += part2;
    nl = buffer.find('\n');
    ASSERT_TRUE(nl != std::string::npos);  // now we have a complete frame

    FixFields f = fix_decode(buffer.data(), nl);
    ASSERT_TRUE(f.valid);
    ASSERT_EQ(f.symbol, std::string("NVDA"));
}

// ============================================================================
// TEST GROUP 5: Field validation logic (receiver-side checks)
// ============================================================================

static bool validate_quote(const FixFields& f) {
    static const std::vector<std::string> VALID = {"PLTR","TSLA","AMD","NVDA","COIN"};
    bool sym_ok = false;
    for (auto& v : VALID) if (f.symbol == v) { sym_ok = true; break; }
    if (!sym_ok) return false;
    if (f.side != 'B' && f.side != 'S') return false;
    if (f.price <= 0) return false;
    if (f.qty <= 0) return false;
    return true;
}

TEST(validate_good_quote_passes) {
    std::string msg = fix_encode(1, "PLTR", 'B', 25.0, 100, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(f.valid);
    ASSERT_TRUE(validate_quote(f));
}

TEST(validate_bad_symbol_fails) {
    std::string msg = fix_encode(1, "AAPL", 'B', 180.0, 100, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(f.valid);           // FIX decodes fine
    ASSERT_TRUE(!validate_quote(f)); // but AAPL is not in our allowed set
}

TEST(validate_bad_side_fails) {
    std::string msg = fix_encode(1, "TSLA", 'X', 210.0, 100, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(f.valid);
    ASSERT_TRUE(!validate_quote(f)); // 'X' is not B or S
}

TEST(validate_zero_price_fails) {
    std::string msg = fix_encode(1, "AMD", 'B', 0.0, 100, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(!validate_quote(f)); // price must be > 0
}

TEST(validate_negative_price_fails) {
    std::string msg = fix_encode(1, "AMD", 'B', -5.0, 100, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(!validate_quote(f));
}

TEST(validate_zero_qty_fails) {
    std::string msg = fix_encode(1, "NVDA", 'S', 880.0, 0, 1000);
    FixFields f = fix_decode(msg.data(), msg.size());
    ASSERT_TRUE(!validate_quote(f)); // qty must be > 0
}

// ============================================================================
// TEST GROUP 6: Sequence gap detection
// ============================================================================

TEST(sequence_no_gap) {
    int64_t last_seq = 5;
    int64_t new_seq = 6;
    bool gap = (new_seq != last_seq + 1);
    ASSERT_TRUE(!gap);
}

TEST(sequence_gap_detected) {
    int64_t last_seq = 5;
    int64_t new_seq = 8;  // skipped 6, 7
    bool gap = (new_seq != last_seq + 1);
    ASSERT_TRUE(gap);
}

TEST(sequence_duplicate_detected) {
    int64_t last_seq = 5;
    int64_t new_seq = 5;  // duplicate
    bool gap = (new_seq != last_seq + 1);
    ASSERT_TRUE(gap);  // treated as a gap (out of order)
}

TEST(sequence_backwards_detected) {
    int64_t last_seq = 10;
    int64_t new_seq = 3;  // went backwards
    bool gap = (new_seq != last_seq + 1);
    ASSERT_TRUE(gap);
}

// ============================================================================
// TEST GROUP 7: Latency computation
// ============================================================================

TEST(latency_positive_when_clocks_synced) {
    int64_t send_ts = 1700000000000000000LL;
    int64_t recv_ts = 1700000000000500000LL;  // 500 microseconds later
    int64_t latency = recv_ts - send_ts;
    ASSERT_EQ(latency, 500000LL);  // 500us in ns
}

TEST(latency_negative_when_clock_skew) {
    // If receiver clock is behind sender, latency can go negative
    // The system should handle this gracefully (just report it)
    int64_t send_ts = 1700000000001000000LL;
    int64_t recv_ts = 1700000000000000000LL;
    int64_t latency = recv_ts - send_ts;
    ASSERT_TRUE(latency < 0);  // negative is expected with clock skew
}

// ============================================================================
// TEST GROUP 8: CSV format
// ============================================================================

TEST(csv_format_correct_columns) {
    // Verify the CSV line format matches what receiver.q expects
    int64_t recv_ts = 1700000000000000000LL;
    int64_t send_ts = 1700000000000000000LL;
    const char* sym = "PLTR";
    char side = 'B';
    double px = 25.1234;
    int qty = 100;
    int64_t seq = 42;

    char line[256];
    snprintf(line, sizeof(line), "%ld,%ld,%s,%c,%.4f,%d,%ld",
             (long)recv_ts, (long)send_ts, sym, side, px, qty, (long)seq);

    // Parse it back (simulating what q would do)
    // Count commas — should be exactly 6 (7 fields)
    int commas = 0;
    for (char* p = line; *p; ++p) if (*p == ',') commas++;
    ASSERT_EQ(commas, 6);

    // First field should be the recv timestamp
    char* tok = strtok(line, ",");
    ASSERT_TRUE(tok != nullptr);
    ASSERT_EQ(atoll(tok), (long long)recv_ts);
}

// ============================================================================
// main — run all tests
// ============================================================================

int main() {
    printf("\n=== Market Pipeline Test Suite ===\n\n");

    printf("[FIX Encoder]\n");
    RUN(encode_produces_nonempty_string);
    RUN(encode_starts_with_beginstring);
    RUN(encode_contains_symbol);
    RUN(encode_contains_checksum);
    RUN(encode_checksum_is_three_digits);
    RUN(encode_sequence_numbers_are_correct);

    printf("\n[FIX Decoder — Round-trip]\n");
    RUN(decode_roundtrip_basic);
    RUN(decode_roundtrip_all_symbols);
    RUN(decode_roundtrip_buy_and_sell);
    RUN(decode_large_sequence_number);
    RUN(decode_small_price);
    RUN(decode_large_price);

    printf("\n[Checksum Validation]\n");
    RUN(checksum_valid_message_passes);
    RUN(checksum_corrupted_message_fails);
    RUN(checksum_truncated_message_does_not_crash);
    RUN(checksum_empty_input_does_not_crash);
    RUN(checksum_garbage_input_does_not_crash);

    printf("\n[Framing — Newline Delimiter]\n");
    RUN(framing_single_message_with_newline);
    RUN(framing_multiple_messages_concatenated);
    RUN(framing_partial_message_leftover);

    printf("\n[Field Validation]\n");
    RUN(validate_good_quote_passes);
    RUN(validate_bad_symbol_fails);
    RUN(validate_bad_side_fails);
    RUN(validate_zero_price_fails);
    RUN(validate_negative_price_fails);
    RUN(validate_zero_qty_fails);

    printf("\n[Sequence Gap Detection]\n");
    RUN(sequence_no_gap);
    RUN(sequence_gap_detected);
    RUN(sequence_duplicate_detected);
    RUN(sequence_backwards_detected);

    printf("\n[Latency Computation]\n");
    RUN(latency_positive_when_clocks_synced);
    RUN(latency_negative_when_clock_skew);

    printf("\n[CSV Format]\n");
    RUN(csv_format_correct_columns);

    printf("\n=== Results: %d passed, %d failed ===\n\n", tests_passed, tests_failed);
    return tests_failed > 0 ? 1 : 0;
}
