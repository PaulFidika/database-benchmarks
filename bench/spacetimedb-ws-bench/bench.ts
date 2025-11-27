// SpacetimeDB WebSocket Benchmark using official SDK (binary BSATN protocol)
import { DbConnection } from './spacetimedb-types';

const WS_URL = process.env.STDB_URL || 'ws://localhost:3001';
const MODULE_NAME = process.env.STDB_MODULE || 'benchmark';
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '10', 10);
const DURATION_MS = parseInt(process.env.DURATION || '10000', 10);
const USER_ID = BigInt(process.env.USER_ID || '1');
const GALLERY_ID = BigInt(process.env.GALLERY_ID || '1');

async function connectClient(id) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`Client ${id} connection timeout`)), 10000);

    const connection = DbConnection.builder()
      .withUri(WS_URL)
      .withModuleName(MODULE_NAME)
      .onConnect((conn, identity, token) => {
        clearTimeout(timeout);
        // Subscribe to comments table so we can query it locally
        conn.subscriptionBuilder().subscribe('SELECT * FROM comments');
        resolve(conn);
      })
      .onConnectError((ctx, err) => {
        clearTimeout(timeout);
        reject(new Error(`Connection error: ${err}`));
      })
      .onDisconnect(() => {
        // ignore disconnect during benchmark
      })
      .build();
  });
}

async function runBenchmark() {
  console.log(`SpacetimeDB WebSocket Benchmark (Binary BSATN Protocol)`);
  console.log(`URL: ${WS_URL}, Module: ${MODULE_NAME}`);
  console.log(`Concurrency: ${CONCURRENCY}, Duration: ${DURATION_MS}ms`);
  console.log(`User ID: ${USER_ID}, Gallery ID: ${GALLERY_ID}`);
  console.log('');

  // Connect all clients
  console.log(`Connecting ${CONCURRENCY} clients...`);
  const clients = [];
  try {
    for (let i = 0; i < CONCURRENCY; i++) {
      const client = await connectClient(i);
      clients.push(client);
    }
  } catch (err) {
    console.error(`Failed to connect clients: ${err.message}`);
    process.exit(1);
  }
  console.log(`All ${CONCURRENCY} clients connected!`);

  // Wait a bit for subscriptions to be established
  await new Promise(r => setTimeout(r, 500));

  let totalOps = 0;
  let errors = 0;
  const startTime = Date.now();
  const endTime = startTime + DURATION_MS;

  // Track pending operations for each client
  const pendingOps = new Map();

  // Run the benchmark
  console.log('Starting benchmark...');

  const runWorker = async (clientIdx) => {
    const client = clients[clientIdx];
    let localOps = 0;
    let localErrors = 0;

    while (Date.now() < endTime) {
      try {
        // This sends a binary BSATN message over WebSocket to the server
        // The call is fire-and-forget (doesn't wait for server acknowledgment)
        client.reducers.addComment({
          galleryId: GALLERY_ID,
          userId: USER_ID,
          text: `Benchmark comment at ${Date.now()}`
        });
        localOps++;

        // Small yield to prevent blocking
        if (localOps % 100 === 0) {
          await new Promise(r => setImmediate(r));
        }
      } catch (err: any) {
        localErrors++;
        if (localErrors === 1) {
          console.error(`First error in worker ${clientIdx}:`, err?.message || err);
        }
      }
    }

    return { ops: localOps, errors: localErrors };
  };

  // Run all workers concurrently
  const workerPromises = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    workerPromises.push(runWorker(i));
  }

  const results = await Promise.all(workerPromises);

  // Aggregate results
  for (const result of results) {
    totalOps += result.ops;
    errors += result.errors;
  }

  const actualDuration = Date.now() - startTime;
  const opsPerSecond = (totalOps / actualDuration) * 1000;

  console.log('');
  console.log('=== Results ===');
  console.log(`Total operations: ${totalOps}`);
  console.log(`Errors: ${errors}`);
  console.log(`Duration: ${actualDuration}ms`);
  console.log(`Throughput: ${opsPerSecond.toFixed(2)} ops/sec`);

  // Output in a format that can be parsed by the Go benchmark
  console.log('');
  console.log(`RESULT: spacetimedb-ws,${CONCURRENCY},${opsPerSecond.toFixed(2)}`);

  // Cleanup
  for (const client of clients) {
    try {
      client.disconnect();
    } catch (e) {
      // ignore
    }
  }

  process.exit(0);
}

runBenchmark().catch(err => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
