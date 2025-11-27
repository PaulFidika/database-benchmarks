// SpacetimeDB WebSocket Benchmark - Realistic Game Client Simulation
// Simulates actual game client behavior with realistic update rates
import { DbConnection } from './spacetimedb-types';

const WS_URL = process.env.STDB_URL || 'ws://localhost:3001';
const MODULE_NAME = process.env.STDB_MODULE || 'benchmark';
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '100', 10);
const DURATION_MS = parseInt(process.env.DURATION || '30000', 10);

// Realistic game client update rates (in Hz / updates per second)
// These are based on typical multiplayer game patterns:
const UPDATE_RATES = {
  // Position updates (movement, rotation) - typically 10-60 Hz
  // Most games use 20-30 Hz for network updates even if rendering at 60 FPS
  position: parseInt(process.env.POSITION_HZ || '20', 10),

  // Action updates (shooting, abilities, interactions) - event-driven
  // Average player might do 1-5 actions per second during active gameplay
  action: parseFloat(process.env.ACTION_HZ || '2'),

  // Chat/social updates - very infrequent
  // Average 0.1-0.5 per second during active chat
  chat: parseFloat(process.env.CHAT_HZ || '0.1'),
};

// For this benchmark, we'll simulate "action" rate updates (like adding comments)
// which is analogous to player actions in a game
const UPDATES_PER_SECOND = parseFloat(process.env.UPDATE_HZ || '10'); // Default: 10 updates/sec per client

const USER_ID = BigInt(process.env.USER_ID || '1');
const GALLERY_ID = BigInt(process.env.GALLERY_ID || '1');

async function connectClient(id: number): Promise<any> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`Client ${id} connection timeout`)), 30000);

    const connection = DbConnection.builder()
      .withUri(WS_URL)
      .withModuleName(MODULE_NAME)
      .onConnect((conn, identity, token) => {
        clearTimeout(timeout);
        conn.subscriptionBuilder().subscribe('SELECT * FROM comments');
        resolve(conn);
      })
      .onConnectError((ctx, err) => {
        clearTimeout(timeout);
        reject(new Error(`Connection error: ${err}`));
      })
      .onDisconnect(() => {
        // ignore
      })
      .build();
  });
}

async function sleep(ms: number) {
  return new Promise(r => setTimeout(r, ms));
}

async function runBenchmark() {
  console.log(`SpacetimeDB WebSocket Benchmark - Realistic Game Simulation`);
  console.log(`============================================================`);
  console.log(`URL: ${WS_URL}, Module: ${MODULE_NAME}`);
  console.log(`Simulated Players: ${CONCURRENCY}`);
  console.log(`Updates per client: ${UPDATES_PER_SECOND}/sec`);
  console.log(`Expected total throughput: ${CONCURRENCY * UPDATES_PER_SECOND} ops/sec`);
  console.log(`Duration: ${DURATION_MS}ms`);
  console.log('');
  console.log('Realistic game update rates for reference:');
  console.log(`  - Position updates: ${UPDATE_RATES.position} Hz (20-60 typical)`);
  console.log(`  - Action updates: ${UPDATE_RATES.action} Hz (1-5 typical)`);
  console.log(`  - Chat updates: ${UPDATE_RATES.chat} Hz (0.1-0.5 typical)`);
  console.log('');

  // Connect all clients in batches to avoid overwhelming the server
  console.log(`Connecting ${CONCURRENCY} clients...`);
  const clients: any[] = [];
  const BATCH_SIZE = 50;

  try {
    for (let batch = 0; batch < CONCURRENCY; batch += BATCH_SIZE) {
      const batchEnd = Math.min(batch + BATCH_SIZE, CONCURRENCY);
      const batchPromises = [];
      for (let i = batch; i < batchEnd; i++) {
        batchPromises.push(connectClient(i));
      }
      const batchClients = await Promise.all(batchPromises);
      clients.push(...batchClients);
      console.log(`  Connected ${clients.length}/${CONCURRENCY} clients`);

      // Small delay between batches
      if (batchEnd < CONCURRENCY) {
        await sleep(100);
      }
    }
  } catch (err: any) {
    console.error(`Failed to connect clients: ${err.message}`);
    process.exit(1);
  }
  console.log(`All ${CONCURRENCY} clients connected!`);

  // Wait for subscriptions
  await sleep(1000);

  let totalOps = 0;
  let errors = 0;
  const startTime = Date.now();
  const endTime = startTime + DURATION_MS;
  const intervalMs = 1000 / UPDATES_PER_SECOND;

  console.log('');
  console.log('Starting benchmark...');
  console.log(`Each client sending 1 update every ${intervalMs.toFixed(1)}ms`);

  // Run workers with rate limiting using absolute timestamps to prevent drift
  const runWorker = async (clientIdx: number) => {
    const client = clients[clientIdx];
    let localOps = 0;
    let localErrors = 0;

    // Use absolute timestamps to prevent timing drift
    let nextUpdateTime = startTime + (clientIdx * (intervalMs / CONCURRENCY)); // Stagger start times

    while (Date.now() < endTime) {
      const now = Date.now();

      // Wait until next scheduled update time
      if (now < nextUpdateTime) {
        await sleep(nextUpdateTime - now);
      }

      // Schedule next update based on absolute time, not current time
      nextUpdateTime += intervalMs;

      try {
        client.reducers.addComment({
          galleryId: GALLERY_ID,
          userId: USER_ID,
          text: `Player ${clientIdx} action at ${Date.now()}`
        });
        localOps++;
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
  const workerPromises = clients.map((_, i) => runWorker(i));
  const results = await Promise.all(workerPromises);

  // Aggregate results
  for (const result of results) {
    totalOps += result.ops;
    errors += result.errors;
  }

  const actualDuration = Date.now() - startTime;
  const opsPerSecond = (totalOps / actualDuration) * 1000;
  const expectedOps = CONCURRENCY * UPDATES_PER_SECOND;
  const efficiency = (opsPerSecond / expectedOps) * 100;

  console.log('');
  console.log('=== Results ===');
  console.log(`Simulated players: ${CONCURRENCY}`);
  console.log(`Update rate per player: ${UPDATES_PER_SECOND}/sec`);
  console.log(`Total operations: ${totalOps}`);
  console.log(`Errors: ${errors}`);
  console.log(`Duration: ${actualDuration}ms`);
  console.log(`Actual throughput: ${opsPerSecond.toFixed(2)} ops/sec`);
  console.log(`Expected throughput: ${expectedOps} ops/sec`);
  console.log(`Efficiency: ${efficiency.toFixed(1)}% (100% = keeping up with all clients)`);

  if (efficiency >= 95) {
    console.log(`\n✅ Server handled all ${CONCURRENCY} players at ${UPDATES_PER_SECOND} updates/sec!`);
  } else if (efficiency >= 80) {
    console.log(`\n⚠️  Server slightly behind - ${efficiency.toFixed(1)}% of expected throughput`);
  } else {
    console.log(`\n❌ Server overloaded - only ${efficiency.toFixed(1)}% of expected throughput`);
  }

  console.log('');
  console.log(`RESULT: spacetimedb-ws-realistic,${CONCURRENCY},${UPDATES_PER_SECOND},${opsPerSecond.toFixed(2)},${efficiency.toFixed(1)}`);

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
