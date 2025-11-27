// SpacetimeDB WebSocket Benchmark - Player Scalability Test
// Tests how many concurrent players can be sustained at a fixed 10-tick update rate
import { DbConnection } from './spacetimedb-types';

const WS_URL = process.env.STDB_URL || 'ws://localhost:3001';
const MODULE_NAME = process.env.STDB_MODULE || 'benchmark';
const TICK_RATE = parseInt(process.env.TICK_RATE || '10', 10); // 10 updates/sec per player (realistic)
const DURATION_MS = parseInt(process.env.DURATION || '15000', 10);

// Scalability test parameters
const START_PLAYERS = parseInt(process.env.START_PLAYERS || '50', 10);
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS || '500', 10);
const PLAYER_STEP = parseInt(process.env.PLAYER_STEP || '50', 10);

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

interface TestResult {
  players: number;
  expectedOps: number;
  actualOps: number;
  opsPerSec: number;
  efficiency: number;
  errors: number;
}

async function runTest(playerCount: number): Promise<TestResult> {
  const clients: any[] = [];
  const BATCH_SIZE = 50;
  const intervalMs = 1000 / TICK_RATE;

  console.log(`\n--- Testing ${playerCount} players at ${TICK_RATE} Hz ---`);

  // Connect all clients in batches
  try {
    for (let batch = 0; batch < playerCount; batch += BATCH_SIZE) {
      const batchEnd = Math.min(batch + BATCH_SIZE, playerCount);
      const batchPromises = [];
      for (let i = batch; i < batchEnd; i++) {
        batchPromises.push(connectClient(i));
      }
      const batchClients = await Promise.all(batchPromises);
      clients.push(...batchClients);
      process.stdout.write(`  Connecting: ${clients.length}/${playerCount}\r`);

      // Small delay between batches to avoid overwhelming
      if (batchEnd < playerCount) {
        await sleep(100);
      }
    }
  } catch (err: any) {
    console.error(`\nFailed to connect at ${clients.length} players: ${err.message}`);
    // Cleanup partial connections
    for (const client of clients) {
      try { client.disconnect(); } catch (e) {}
    }
    return {
      players: playerCount,
      expectedOps: 0,
      actualOps: 0,
      opsPerSec: 0,
      efficiency: 0,
      errors: -1 // Signal connection failure
    };
  }

  console.log(`  Connected ${playerCount} players`);

  // Wait for subscriptions
  await sleep(500);

  let totalOps = 0;
  let errors = 0;
  const startTime = Date.now();
  const endTime = startTime + DURATION_MS;

  // Run workers with rate limiting using absolute timestamps
  const runWorker = async (clientIdx: number) => {
    const client = clients[clientIdx];
    let localOps = 0;
    let localErrors = 0;

    // Stagger start times to spread load
    let nextUpdateTime = startTime + (clientIdx * (intervalMs / playerCount));

    while (Date.now() < endTime) {
      const now = Date.now();

      if (now < nextUpdateTime) {
        await sleep(nextUpdateTime - now);
      }

      nextUpdateTime += intervalMs;

      try {
        client.reducers.addComment({
          galleryId: GALLERY_ID,
          userId: USER_ID,
          text: `P${clientIdx} t${Date.now()}`
        });
        localOps++;
      } catch (err: any) {
        localErrors++;
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
  const expectedOps = playerCount * TICK_RATE;
  const efficiency = (opsPerSecond / expectedOps) * 100;

  console.log(`  Results: ${opsPerSecond.toFixed(0)} ops/sec (${efficiency.toFixed(1)}% of ${expectedOps} expected)`);

  // Cleanup
  for (const client of clients) {
    try { client.disconnect(); } catch (e) {}
  }

  // Wait a bit for server cleanup
  await sleep(1000);

  return {
    players: playerCount,
    expectedOps: expectedOps,
    actualOps: Math.round(opsPerSecond),
    opsPerSec: opsPerSecond,
    efficiency: efficiency,
    errors: errors
  };
}

async function runScalabilityTest() {
  console.log(`SpacetimeDB Player Scalability Test`);
  console.log(`====================================`);
  console.log(`URL: ${WS_URL}, Module: ${MODULE_NAME}`);
  console.log(`Tick Rate: ${TICK_RATE} Hz (updates/sec per player)`);
  console.log(`Duration per test: ${DURATION_MS}ms`);
  console.log(`Testing from ${START_PLAYERS} to ${MAX_PLAYERS} players (step: ${PLAYER_STEP})`);
  console.log('');
  console.log('Game server context:');
  console.log(`  - Server internal tick rate: ~120 Hz (server-side simulation)`);
  console.log(`  - Client update rate: ${TICK_RATE} Hz (network updates from players)`);
  console.log(`  - At ${TICK_RATE} Hz, each player sends an update every ${(1000/TICK_RATE).toFixed(0)}ms`);
  console.log('');

  const results: TestResult[] = [];

  for (let players = START_PLAYERS; players <= MAX_PLAYERS; players += PLAYER_STEP) {
    const result = await runTest(players);
    results.push(result);

    // If we had connection failures or efficiency dropped below 50%, stop scaling
    if (result.errors === -1) {
      console.log(`\n⚠️  Connection failures at ${players} players - stopping test`);
      break;
    }

    if (result.efficiency < 50) {
      console.log(`\n⚠️  Efficiency dropped below 50% at ${players} players - stopping test`);
      break;
    }
  }

  // Print summary
  console.log('\n');
  console.log('='.repeat(70));
  console.log('SCALABILITY RESULTS SUMMARY');
  console.log('='.repeat(70));
  console.log(`Tick Rate: ${TICK_RATE} Hz per player`);
  console.log('');
  console.log('Players | Expected ops/s | Actual ops/s | Efficiency | Status');
  console.log('-'.repeat(70));

  let peakPlayers = 0;
  let peakOps = 0;

  for (const r of results) {
    if (r.errors === -1) {
      console.log(`${r.players.toString().padStart(7)} | ${r.expectedOps.toString().padStart(14)} | CONNECTION FAILED`);
    } else {
      const status = r.efficiency >= 95 ? '✅' : r.efficiency >= 80 ? '⚠️' : '❌';
      console.log(`${r.players.toString().padStart(7)} | ${r.expectedOps.toString().padStart(14)} | ${r.actualOps.toString().padStart(12)} | ${r.efficiency.toFixed(1).padStart(9)}% | ${status}`);

      if (r.efficiency >= 80 && r.players > peakPlayers) {
        peakPlayers = r.players;
        peakOps = r.actualOps;
      }
    }
  }

  console.log('-'.repeat(70));
  console.log('');

  if (peakPlayers > 0) {
    console.log(`🎮 PEAK SUSTAINABLE LOAD: ${peakPlayers} players at ${TICK_RATE} Hz = ${peakOps} ops/sec`);
    console.log(`   (Sustainable = ≥80% efficiency, meaning server keeps up with demand)`);
  } else {
    console.log(`❌ Could not sustain even ${START_PLAYERS} players at ${TICK_RATE} Hz`);
  }

  console.log('');
  console.log('CSV: players,tick_rate,expected_ops,actual_ops,efficiency');
  for (const r of results) {
    if (r.errors !== -1) {
      console.log(`CSV: ${r.players},${TICK_RATE},${r.expectedOps},${r.actualOps},${r.efficiency.toFixed(1)}`);
    }
  }

  process.exit(0);
}

runScalabilityTest().catch(err => {
  console.error('Scalability test failed:', err);
  process.exit(1);
});
