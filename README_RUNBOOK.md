# Market Data Pipeline — Runbook

## Architecture

```
[Sender (workstation)]  --TCP:9009-->  [Receiver (Jetson)]  --inbox.csv-->  [kdb+ q]
  synthetic FIX quotes                   decode + validate                   top-of-book
  david-B650-AORUS-ELITE-AX             david-jetson (10.10.10.2)           localhost:5000
```

## FIX Tag Reference

| Tag  | Field            | Standard? |
|------|------------------|-----------|
| 8    | BeginString      | Yes       |
| 9    | BodyLength       | Yes       |
| 10   | CheckSum         | Yes       |
| 34   | MsgSeqNum        | Yes       |
| 35   | MsgType (W)      | Yes       |
| 38   | OrderQty         | Yes       |
| 44   | Price            | Yes       |
| 49   | SenderCompID     | Yes       |
| 52   | SendingTime (ns) | Modified  |
| 54   | Side (B/S)       | Modified  |
| 55   | Symbol           | Yes       |
| 56   | TargetCompID     | Yes       |
| 5001 | SendTimestampNs  | Custom    |

Timestamps use `CLOCK_REALTIME` (nanoseconds since epoch) on both sender and receiver so latency = recv_ts - send_ts is meaningful across hosts with NTP-synchronized clocks.

## Build (both machines)

```bash
cd ~/market-pipeline
cmake -B build
cmake --build build
```

## Deploy to Jetson

```bash
# From workstation
ssh david@10.10.10.2 "mkdir -p ~/market-pipeline"
scp -r CMakeLists.txt sender.cpp receiver.cpp receiver.q \
    david@10.10.10.2:~/market-pipeline/

# Build on Jetson
ssh david@10.10.10.2 "cd ~/market-pipeline && cmake -B build && cmake --build build"
```

## Run

### 1. Sanity test (netcat)

```bash
# Jetson terminal 1
nc -l 9009

# Workstation
echo "hello" | nc 10.10.10.2 9009
```

### 2. Start kdb+ on Jetson

```bash
cd ~/market-pipeline
q receiver.q -p 5000
```

### 3. Start receiver on Jetson

```bash
cd ~/market-pipeline
./build/receiver
```

### 4. Start sender on workstation

```bash
cd ~/market-pipeline
./build/sender --host 10.10.10.2 --port 9009 --rate 100
```

Higher rate:
```bash
./build/sender --rate 1000 --buyBias 0.65
```

## kdb+ Queries (from q console or IPC)

```q
count quotes              / total ingested
book                      / current top-of-book (updated every 1s)
select avg px, sum qty by sym, side from quotes
select from book where imbalance > 0.3
```

## Cleanup

```bash
# Jetson: reset CSV
rm ~/market-pipeline/inbox.csv
```

## Troubleshooting

- **Connection refused**: Ensure receiver is running before sender.
- **Clock skew / negative latency**: Run `chronyc tracking` or `ntpq -p` on both hosts. Latency metric requires NTP sync.
- **No data in kdb+**: Check `inbox.csv` is being written. The q timer tails it every 1 second.
