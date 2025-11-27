# Distributed Database Comparison Demo

Compare CouchDB, CockroachDB, YugabyteDB, and PostgreSQL with a real-time galleries and comments application.

## Architecture

- **CouchDB** (3-node cluster): Document database with native change streams
- **CockroachDB** (3-node cluster): Distributed SQL with PostgreSQL compatibility
- **YugabyteDB** (3-node cluster): Distributed SQL with PostgreSQL compatibility
- **PostgreSQL** (single instance): Traditional SQL database for baseline comparison
- **React Frontend**: Tab-based UI to interact with all databases
- **Go API Server**: REST API for SQL databases
- **Benchmark Tool**: Stress test comparing throughput across all databases

## Quick Start

```bash
# Start all services (CouchDB cluster auto-initializes)
docker compose up -d

# Wait for services to initialize (about 30-60 seconds)
# Check status:
docker compose ps

# Setup benchmark test data
./bench/setup-test-data.sh

# Open the frontend
open http://localhost:3000
```

## Endpoints

### Frontend
- http://localhost:3000 - React application

### CouchDB
- http://localhost:5984 - Node 1
- http://localhost:5985 - Node 2
- http://localhost:5986 - Node 3

### CockroachDB
- http://localhost:26257 - Node 1 (SQL)
- http://localhost:26258 - Node 2 (SQL)
- http://localhost:26259 - Node 3 (SQL)
- http://localhost:8080 - Admin UI

### YugabyteDB
- http://localhost:5433 - TServer 1 (PostgreSQL)
- http://localhost:5434 - TServer 2 (PostgreSQL)
- http://localhost:5435 - TServer 3 (PostgreSQL)
- http://localhost:7000 - Master UI

### PostgreSQL
- http://localhost:5436 - PostgreSQL (single instance)

### API Server
- http://localhost:9090 - Go API for SQL databases
  - `/health` - Health check
  - `/crdb/users`, `/crdb/galleries`, `/crdb/comments` - CockroachDB endpoints
  - `/yb/users`, `/yb/galleries`, `/yb/comments` - YugabyteDB endpoints

## Running the Benchmark

The benchmark tool connects directly to each database (bypassing the API server) to measure true database performance.

```bash
cd bench

# Run with defaults (50-500 concurrent connections, 5s per stage)
go run .

# Custom settings
go run . --start 50 --max 500 --step 50 --stage-duration 5s

# Limit max time per database (useful if a DB is slow)
go run . --max-time 2m

# Log error details
go run . --log-errors

# Adjust connection pool size
go run . --pool-size 200
```

### Benchmark Options

| Flag | Default | Description |
|------|---------|-------------|
| `--start` | 50 | Starting concurrency level |
| `--max` | 500 | Maximum concurrency level |
| `--step` | 50 | Concurrency increment per stage |
| `--stage-duration` | 5s | Duration of each stage |
| `--max-time` | 2m | Maximum time per database |
| `--pool-size` | 200 | Connection pool size |
| `--log-errors` | false | Print sample errors |

### Benchmark Results

Results from direct database access benchmark (70% reads, 30% writes):

| Rank | Database | Peak Throughput | Writes/s | Reads/s | Latency (p95) | Notes |
|------|----------|-----------------|----------|---------|---------------|-------|
| 1 | **PostgreSQL** | **19,137 ops/s** | 5,749 | 13,388 | W: 51ms, R: 25ms | Single instance, no replication overhead |
| 2 | CouchDB | 567 ops/s | 174 | 394 | W: 3.2s, R: 1.1s | HTTP protocol overhead |
| 3 | YugabyteDB | 288 ops/s | 88 | 199 | W: 134ms, R: 389ms | Distributed consensus overhead |
| 4 | CockroachDB | 244 ops/s | 75 | 169 | W: 2s, R: 2s | Distributed consensus overhead |

### Sample Output

```
================================================================
  FINAL SUMMARY
================================================================

Ranked by Peak Throughput (0% failure rate):
─────────────────────────────────────────────────────────────────
Database        Peak Write/s  Peak Read/s Peak Total/s  Saturation
─────────────────────────────────────────────────────────────────
1. postgres           5749.4      13387.6      19137.0       none
2. couchdb             173.6        393.6        567.2   200 conc
3. yugabyte             88.4        199.2        287.6   100 conc
4. cockroachdb          74.8        169.0        243.8   100 conc
─────────────────────────────────────────────────────────────────
```

### Analysis

**Why is PostgreSQL so much faster?**

PostgreSQL achieves ~33-80x higher throughput than the distributed databases because:

1. **No replication overhead**: Single instance means no network round-trips for consensus
2. **No distributed transactions**: Writes complete locally without coordinating with other nodes
3. **Optimized for single-node**: Years of optimization for traditional deployment

**Why are distributed databases slower?**

CockroachDB and YugabyteDB use Raft consensus, meaning every write must:
1. Be sent to the leader node
2. Replicated to at least 2/3 nodes
3. Wait for majority acknowledgment before confirming

This is by design - they prioritize **strong consistency** and **fault tolerance** over raw throughput.

**When to use distributed databases:**

- Multi-region deployments where data must survive datacenter failures
- Applications requiring global data distribution with local reads
- Systems where consistency across regions is critical

**When to use PostgreSQL:**

- Single-region deployments
- Maximum performance is the priority
- You can handle replication/failover at the infrastructure level

## Features Demonstrated

1. **Users**: Create and list users
2. **Galleries**: Create galleries associated with users
3. **Comments**: Add comments to galleries
4. **Real-time Updates**:
   - CouchDB: Native `_changes` feed (long polling)
   - SQL databases: Server-Sent Events (polling-based)

## Data Model

### CouchDB (Document)
```json
// User
{
  "_id": "user:123",
  "type": "user",
  "name": "John",
  "email": "john@example.com"
}

// Gallery
{
  "_id": "gallery:456",
  "type": "gallery",
  "user_id": "user:123",
  "title": "My Photos"
}

// Comment
{
  "_id": "comment:789",
  "type": "comment",
  "gallery_id": "gallery:456",
  "user_id": "user:123",
  "text": "Great photo!"
}
```

### SQL (CockroachDB/YugabyteDB)
```sql
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255),
  email VARCHAR(255),
  created_at TIMESTAMP
);

CREATE TABLE galleries (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id),
  title VARCHAR(255),
  description TEXT,
  created_at TIMESTAMP
);

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  gallery_id INTEGER REFERENCES galleries(id),
  user_id INTEGER REFERENCES users(id),
  text TEXT,
  created_at TIMESTAMP
);
```

## Cleanup

```bash
# Stop and remove containers
docker compose down

# Also remove volumes
docker compose down -v
```
