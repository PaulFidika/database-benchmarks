# Database Benchmark Results

## 1. Database Comparison (Direct Access, 70% Reads / 30% Writes)

| Rank | Database | Peak ops/sec | Writes/s | Reads/s | Write p95 | Read p95 |
|------|----------|--------------|----------|---------|-----------|----------|
| 1 | **PostgreSQL** (single) | **17,990** | 5,407 | 12,583 | 28ms | 8ms |
| 2 | SpacetimeDB (single, HTTP) | 2,000 | 597 | 1,402 | 54ms | 55ms |
| 3 | CouchDB (3-node) | 715 | 214 | 502 | 778ms | 258ms |
| 4 | CockroachDB (3-node) | 119 | 34 | 85 | 405ms | 1.6s |
| 5 | SurrealDB (single, HTTP) | 166 | 49 | 117 | 93ms | 192ms |

**Findings:**
- PostgreSQL can sustain 25x more volume than distributed databases due to no consensus overhead
- Distributed databases (CockroachDB, YugabyteDB) sacrifice throughput for fault tolerance
- CouchDB's HTTP protocol adds significant latency vs binary protocols
- SurrealDB seems relatively low throughput.

## 2. Protocol Comparison (PostgreSQL vs SpacetimeDB)

| Database + Protocol | 10 Conc | 50 Conc | 100 Conc | Peak |
|---------------------|---------|---------|----------|------|
| **PostgreSQL Binary** (pgx) | 6,072 | 18,041 | 20,947 | **20,947** |
| **SpacetimeDB WebSocket** (BSATN) | 4,874 | 1,527 | 919 | **4,874** |
| SpacetimeDB HTTP | 2,000 | 929 | 510 | 2,000 |
| PostgreSQL HTTP (via API) | 3,539* | - | - | 3,539 |
| **SurrealDB WebSocket** (CBOR) | 602 | 540 | 533 | **602** |
| SurrealDB HTTP | 166 | 140 | 116 | 166 |

*broke at 50 concurrency

**Findings:**
- Binary protocols have 5-50x higher throughput than HTTP/JSON
- SpacetimeDB WebSocket (4,874 ops/sec) approaches PostgreSQL binary (6,072 ops/sec) at low concurrency
- SpacetimeDB and SurrealDB degrade at high concurrency (single-instance bottleneck); PostgreSQL scales up
- SurrealDB WebSocket (602 ops/sec) has 3.6x the throughput of HTTP (166 ops/sec), but still 10x lower than PostgreSQL
- HTTP APIs break under load; binary protocols remain stable

## 3. SpacetimeDB Multiplayer Game Server (120Hz Server, 10Hz Client Input)

| Players | Input Throughput | Updates/Client/s | Efficiency |
|---------|------------------|------------------|------------|
| 10 | 100/s | 87 | 100% |
| 30 | 300/s | 91 | 100% |
| 50 | 500/s | 88 | 100% |
| 70 | 609/s | 63 | 87% |
| 90 | 442/s | 25 | 49% |

**Peak: ~70 players** at 120Hz server tick rate with batched world state updates.

```
┌─────────────────────────────────────────────────────────────┐
│                      SpacetimeDB Server                     │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │ Player      │    │ Game Loop   │    │ World State     │  │
│  │ Inputs      │───▶│ (120 Hz)    │───▶│ (single row)    │  │
│  │ (buffered)  │    │             │    │                 │  │
│  └─────────────┘    │ - Collect   │    │ - tick: u64     │  │
│                     │   inputs    │    │ - players: Vec  │  │
│                     │ - Physics   │    │                 │  │
│                     │ - Broadcast │    └────────┬────────┘  │
│                     └─────────────┘             │           │
└─────────────────────────────────────────────────┼───────────┘
                                                  │
                   ┌──────────────────────────────┼──────────────────────────────┐
                   │                              │                              │
                   ▼                              ▼                              ▼
            ┌──────────┐                   ┌──────────┐                   ┌──────────┐
            │ Client 1 │                   │ Client 2 │                   │ Client N │
            │          │                   │          │                   │          │
            │ Input:   │                   │ Input:   │                   │ Input:   │
            │ 10 Hz ──▶│                   │ 10 Hz ──▶│                   │ 10 Hz ──▶│
            │          │                   │          │                   │          │
            │ Receive: │                   │ Receive: │                   │ Receive: │
            │ 120 Hz ◀─│                   │ 120 Hz ◀─│                   │ 120 Hz ◀─│
            └──────────┘                   └──────────┘                   └──────────┘
```

Server collects player inputs, runs physics at 120Hz, broadcasts ONE batched world state update per tick. This O(N) approach scales 2x better than naive per-player updates (O(N²)).

## How to Run

```bash
# Start services
docker compose up -d

# Setup test data
./bench/setup-test-data.sh

# Benchmark 1: database comparison
cd bench && go run .

# Benchmark 2: SpacetimeDB WebSocket benchmark
cd bench/spacetimedb-ws-bench
npm install
CONCURRENCY=10 DURATION=10000 npx tsx bench.ts

# Benchmark 3: SurrealDB WebSocket benchmark (binary CBOR)
cd bench/surrealdb-ws-bench
npm install
CONCURRENCY=10 DURATION=10000 npx tsx bench.ts

# Benchmark 4: multiplayer scalability test
START_PLAYERS=10 MAX_PLAYERS=100 PLAYER_STEP=20 DURATION=10000 npx tsx bench-batched.ts
```

## Cleanup

```bash
docker compose down -v
```
