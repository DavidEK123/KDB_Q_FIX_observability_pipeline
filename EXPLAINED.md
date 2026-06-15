# Market Data Pipeline — Explained Like You're 15

## What is this project?

Imagine you're watching a stock market. Every fraction of a second, people are saying things like:

> "I want to BUY 200 shares of NVIDIA at $880"
> "I want to SELL 50 shares of Tesla at $210"

These messages fly around **really** fast — thousands per second. This project simulates that. We built a mini version of what Wall Street trading systems actually do:

1. **Generate** fake stock quotes (like a stock market simulator)
2. **Send** them over the network using a real financial protocol called FIX
3. **Receive** them on another computer
4. **Analyze** them to answer: "What's the best price to buy/sell right now?"

---

## The Big Picture

```
YOUR PC                          JETSON (small computer)
┌─────────┐    ethernet cable    ┌──────────┐    file    ┌─────────┐
│  SENDER  │ ──── TCP:9009 ────> │ RECEIVER │ ── CSV ──> │  KDB+   │
│  (C++)   │   FIX messages      │  (C++)   │            │  (q)    │
└─────────┘                      └──────────┘            └─────────┘
 makes up                         catches them,           crunches the
 stock quotes                     checks them,            numbers every
 100/second                       logs to file             second
```

It's like a game of catch, but instead of a ball, you're throwing stock market data.

---

## File-by-File Breakdown

### 1. `sender.cpp` — The Fake Stock Market

**What it does:** Pretends to be a stock exchange. Every 10 milliseconds (at default 100 msgs/sec), it makes up a new stock quote and sends it over the network.

**How it works, step by step:**

1. **Pick a random stock** — We have 5: PLTR, TSLA, AMD, NVDA, COIN. It picks one randomly each time.

2. **Move the price a tiny bit** — This is called a "random walk." The price wiggles up and down by tiny amounts, like a real stock. NVDA starts at $880 and might drift to $881.23, then $880.95, etc. The key line:
   ```cpp
   double step = t.px * 0.0005 * (unit(rng) * 2.0 - 1.0);
   ```
   This says: take 0.05% of the current price, multiply by a random number between -1 and +1. So NVDA at $880 moves by at most ~$0.44 per tick. That's realistic.

3. **Choose buy or sell** — There's a `buyBias` of 0.70, meaning 70% of quotes are buys. This creates an "imbalance" — more people want to buy than sell — which is something traders actually watch.

4. **Choose quantity** — Buyers want 50-500 shares, sellers want 10-200. (Buyers are more aggressive in this simulation.)

5. **Encode it as a FIX message** — FIX is the actual protocol used on Wall Street (explained below).

6. **Send it over TCP** — TCP is like a reliable phone call between two computers. Every byte you send arrives in order, guaranteed.

7. **Sleep until it's time for the next message** — To hit exactly 100 messages/second, we measure how long the work took and sleep the remainder.

**The pacing trick:**
```cpp
int64_t elapsed = now_ns() - loop_start;
int64_t remaining = interval_ns - elapsed;
nanosleep(&sl, nullptr);  // sleep the leftover time
```
If we want 100 msgs/sec, that's one every 10,000,000 nanoseconds. If encoding+sending took 500,000ns, we sleep 9,500,000ns. This keeps the rate steady.

---

### 2. `receiver.cpp` — The Catcher

**What it does:** Sits on the Jetson computer, waits for the sender to connect, catches every message, validates it, and writes it to a CSV file for kdb+ to analyze.

**The tricky parts explained:**

**TCP Framing — why we need `\n`:**
TCP is a "stream" — it doesn't know where one message ends and the next begins. Imagine someone reading you a book over the phone with no pauses between sentences. You'd need some way to know where each sentence ends. We use `\n` (newline) as our sentence-ender. The receiver reads a big chunk of bytes, then scans for `\n` characters to split it into individual messages.

But here's the gotcha: TCP might give you 1.5 messages in one `recv()` call, or half a message. So we keep a `leftover` buffer:
```
Read: [full message 1\n][half of message 2...]
       ↑ process this     ↑ save this for later

Next read: [...rest of message 2\n]
            ↑ combine with leftover, then process
```

**Validation — trust nothing:**
Every message gets checked:
- Is the symbol one of our 5 stocks? (reject "AAPL" — not in our system)
- Is the side B or S? (reject "X")
- Is the price positive? (reject $0 or $-5)
- Is the quantity positive? (reject 0 shares)
- Does the checksum match? (reject corrupted data)

**Sequence gap detection:**
Messages are numbered 1, 2, 3, 4... If we get 1, 2, 5, we know 3 and 4 were lost. The receiver counts these gaps. On a good network, gaps = 0. On a bad network, this tells you how many messages you're losing.

**Latency tracking:**
Each message carries a timestamp from when the sender created it. The receiver stamps when it arrived. The difference = network latency. We track p50/p95/p99:
- **p50** = the "typical" latency (half are faster, half are slower)
- **p95** = the latency for the slowest 5% of messages
- **p99** = the latency for the slowest 1% (the "worst case" that happens 1 in 100 times)

These percentiles are what trading firms actually care about. If p50 is 100 microseconds but p99 is 50 milliseconds, you have a "tail latency" problem.

---

### 3. `receiver.q` — The Brain (kdb+)

**What is kdb+?**
kdb+ is a database built specifically for financial data. It's **insanely fast** at time-series queries — like asking "what was the highest NVIDIA bid in the last 5 seconds?" It uses a language called `q` that looks weird but is very powerful.

**What this script does every 1 second:**

1. **Reads new lines from inbox.csv** — The receiver.cpp is constantly appending new lines. The q script checks "how many lines are there now?" and only reads the new ones (using an offset). It also deduplicates by sequence number so it never counts the same quote twice.

2. **Computes "top of book"** — This is the core financial concept:

   - **Best bid** = the highest price someone is willing to BUY at
   - **Best ask** = the lowest price someone is willing to SELL at

   Think of it like eBay. If three people want to buy your PS5:
   - Person A offers $400
   - Person B offers $450
   - Person C offers $425

   The "best bid" is $450 (highest offer). That's the price you'd get if you sold right now.

   Similarly, if three people are selling PS5s:
   - Seller X wants $460
   - Seller Y wants $455
   - Seller Z wants $470

   The "best ask" is $455 (lowest price). That's what you'd pay to buy right now.

3. **Computes order-flow imbalance:**
   ```
   imbalance = (totalBuyQty - totalSellQty) / (totalBuyQty + totalSellQty)
   ```
   - If imbalance = +0.8, buyers massively outnumber sellers → price likely going UP
   - If imbalance = -0.5, sellers dominate → price likely going DOWN
   - If imbalance = 0, perfectly balanced

   This is a real signal that high-frequency trading firms use.

---

### 4. The FIX Protocol (built into sender.cpp and receiver.cpp)

**What is FIX?**
FIX = Financial Information eXchange. It's been used on Wall Street since the 1990s. Every stock exchange, every broker, every trading firm speaks FIX. It's like HTTP for finance.

**How our messages look on the wire:**
```
8=FIX.4.4 | 9=127 | 35=W | 49=SENDER | 56=RECEIVER | 34=42 | 52=1700000000 | 55=NVDA | 54=B | 44=881.2345 | 38=250 | 5001=1700000000 | 10=183 |
```
(The `|` is actually `\x01` — an invisible "Start of Heading" character called SOH)

Each piece is a "tag=value" pair:
| Tag | Name | Example | Meaning |
|-----|------|---------|---------|
| 8 | BeginString | FIX.4.4 | "I'm speaking FIX version 4.4" |
| 9 | BodyLength | 127 | "The message body is 127 bytes" |
| 35 | MsgType | W | "This is a Market Data Snapshot" |
| 34 | MsgSeqNum | 42 | "This is message number 42" |
| 55 | Symbol | NVDA | "This is about NVIDIA stock" |
| 54 | Side | B | "Someone wants to Buy" |
| 44 | Price | 881.23 | "At this price" |
| 38 | Qty | 250 | "This many shares" |
| 10 | CheckSum | 183 | "Error check: all bytes sum to 183 mod 256" |

**The checksum** is like a spell-check for data. You add up every byte in the message, take the remainder when dividing by 256, and that's your checksum. If even one byte gets corrupted in transit, the checksum won't match and the receiver rejects the message.

---

### 5. `test_fix.cpp` — The Tests (33 of them)

Tests are like a safety net. Before we run this on real hardware across a network, we make sure the basic building blocks work. Here's what each group tests:

**FIX Encoder (6 tests):**
- Does encoding produce something? (not empty)
- Does it start with the right FIX header?
- Does it include the stock symbol we gave it?
- Does it have a checksum?
- Is the checksum exactly 3 digits? (FIX spec requires this)
- Are sequence numbers correct?

**FIX Decoder Round-trip (6 tests):**
- Encode a message, then decode it. Do we get the same values back?
- Does this work for ALL 5 stock symbols?
- Does Buy and Sell both work?
- Do huge sequence numbers work? (9,999,999,999)
- Do very small prices work? ($0.01)
- Do very large prices work? ($9,999.99)

**Checksum Validation (5 tests):**
- Valid message passes checksum? (obviously should)
- **Corrupted message** — we flip a byte and check the checksum catches it
- **Truncated message** — we chop the message in half. Does it crash? (It must NOT crash, ever)
- **Empty input** — zero bytes. Crash? (NO)
- **Garbage input** — random English text. Crash? (NO)

**Framing (3 tests):**
- One message with `\n` — basic case
- **Two messages glued together** — simulates TCP coalescing (the network delivers both in one chunk). Can we split them apart correctly?
- **One message split in two** — simulates TCP fragmentation (network delivers half now, half later). Can we reassemble it?

**Field Validation (6 tests):**
- Good quote passes
- "AAPL" rejected (not in our 5 symbols)
- Side "X" rejected (must be B or S)
- Price $0 rejected
- Price -$5 rejected
- Quantity 0 rejected

**Sequence Gaps (4 tests):**
- 5→6 = no gap (good)
- 5→8 = gap detected (skipped 6,7)
- 5→5 = duplicate detected
- 10→3 = backwards detected

**Latency (2 tests):**
- Normal case: recv time > send time = positive latency
- Clock skew: recv time < send time = negative latency (this happens in real life when clocks aren't perfectly synced)

**CSV Format (1 test):**
- The output CSV has exactly 7 comma-separated fields (matching what kdb+ expects)

---

### 6. `CMakeLists.txt` — The Build Recipe

CMake is like a recipe that tells the compiler how to build your programs. Ours is simple:
- Use C++17
- Build two programs: `sender` and `receiver`
- No external libraries needed (everything uses standard C++ and Linux system calls)

---

## Why This Matters (the resume angle)

This project touches real concepts that trading firms interview on:

| Concept | Where it shows up |
|---------|-------------------|
| **Network programming** | TCP sockets, handling partial reads/writes |
| **Protocol design** | FIX encoding/decoding, checksums, framing |
| **Low-latency measurement** | Nanosecond timestamps, percentile latency stats |
| **Financial concepts** | Top-of-book, bid/ask, order-flow imbalance |
| **Data pipelines** | sender → network → receiver → CSV → kdb+ |
| **Defensive programming** | Input validation, never crashing on bad data |
| **Time-series databases** | kdb+ for real-time financial analytics |

---

## Glossary

| Term | Plain English |
|------|---------------|
| **TCP** | A reliable way for two computers to talk. Like a phone call — everything arrives in order. |
| **FIX** | The language Wall Street computers use to talk about trades. |
| **SOH** | An invisible character (byte value 1) used as a separator in FIX messages. |
| **Checksum** | A math trick to detect if data got corrupted. Add up all bytes, take mod 256. |
| **Latency** | How long it takes a message to travel from sender to receiver. Lower = better. |
| **p50/p95/p99** | Percentiles. p99 = "99% of messages were faster than this." |
| **Top of book** | The best available buy price and sell price for a stock right now. |
| **Bid** | The price someone is willing to BUY at. |
| **Ask** | The price someone is willing to SELL at. |
| **Spread** | Ask minus Bid. The "gap" between buyers and sellers. Smaller = more liquid market. |
| **Imbalance** | Are there more buyers or sellers? Predicts short-term price direction. |
| **Random walk** | Price moves by small random steps each tick. Models real stock behavior. |
| **kdb+** | A super-fast database for time-series data. Used by every major bank. |
| **Nanosecond** | One billionth of a second. Light travels ~1 foot in one nanosecond. |
| **TCP coalescing** | When the network bundles multiple small messages into one big delivery. |
| **TCP fragmentation** | When the network splits one message across multiple deliveries. |
| **Sequence number** | A counter on each message so you can detect if any went missing. |
