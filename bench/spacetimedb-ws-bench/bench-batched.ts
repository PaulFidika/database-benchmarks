// SpacetimeDB WebSocket Benchmark - Batched World State Architecture
// Tests scalability with proper game server architecture:
// - Server runs internal loop at 120Hz, broadcasts ONE world state update per tick
// - Clients send input at 10Hz, receive world state updates at 120Hz
// - Each client receives exactly 120 updates/second regardless of player count

import { DbConnection } from './spacetimedb-types';

const WS_URL = process.env.STDB_URL || 'ws://localhost:3001';
const MODULE_NAME = process.env.STDB_MODULE || 'benchmark';
const CLIENT_TICK_RATE = parseInt(process.env.CLIENT_TICK || '10', 10); // 10 Hz client input
const SERVER_TICK_RATE = 120; // 120 Hz server loop
const DURATION_MS = parseInt(process.env.DURATION || '15000', 10);

// Scalability test parameters
const START_PLAYERS = parseInt(process.env.START_PLAYERS || '10', 10);
const MAX_PLAYERS = parseInt(process.env.MAX_PLAYERS || '200', 10);
const PLAYER_STEP = parseInt(process.env.PLAYER_STEP || '20', 10);

async function connectClient(id: number): Promise<any> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error(`Client ${id} connection timeout`)), 30000);

    const connection = DbConnection.builder()
      .withUri(WS_URL)
      .withModuleName(MODULE_NAME)
      .onConnect((conn, identity, token) => {
        clearTimeout(timeout);
        // Subscribe ONLY to world_state - single row with all player data
        conn.subscriptionBuilder().subscribe(['SELECT * FROM world_state']);
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
  expectedInputsPerSec: number;
  actualInputsPerSec: number;
  worldStateUpdatesPerClient: number;
  efficiency: number;
  errors: number;
}

async function runTest(playerCount: number): Promise<TestResult> {
  const clients: any[] = [];
  const BATCH_SIZE = 25;
  const intervalMs = 1000 / CLIENT_TICK_RATE;

  console.log(`\n--- Testing ${playerCount} players (10Hz input, 120Hz server broadcast) ---`);

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

      if (batchEnd < playerCount) {
        await sleep(200);
      }
    }
  } catch (err: any) {
    console.error(`\nFailed to connect at ${clients.length} players: ${err.message}`);
    for (const client of clients) {
      try { client.disconnect(); } catch (e) {}
    }
    return {
      players: playerCount,
      expectedInputsPerSec: 0,
      actualInputsPerSec: 0,
      worldStateUpdatesPerClient: 0,
      efficiency: 0,
      errors: -1
    };
  }

  console.log(`  Connected ${playerCount} players`);

  // Wait for subscriptions
  await sleep(1000);

  // Start the game loop on the server (only one client needs to do this)
  try {
    console.log('  Starting server game loop (120Hz batched)...');
    clients[0].reducers.startGameLoop();
    await sleep(500);
  } catch (err: any) {
    // Game loop might already be running
    console.log(`  Game loop note: ${err.message}`);
  }

  // Join all players
  console.log('  Joining players to game...');
  for (let i = 0; i < clients.length; i++) {
    try {
      clients[i].reducers.joinGame({ playerId: BigInt(i + 1) });
    } catch (e) {
      // Player might already exist
    }
  }
  await sleep(500);

  let totalInputs = 0;
  let errors = 0;
  const startTime = Date.now();
  const endTime = startTime + DURATION_MS;

  // Track world state updates received per client
  const updateCounts = new Map<number, number>();
  for (let i = 0; i < clients.length; i++) {
    updateCounts.set(i, 0);
    // Subscribe to world_state updates - this is ONE update per tick with ALL player data
    clients[i].db.worldState.onUpdate(() => {
      updateCounts.set(i, (updateCounts.get(i) || 0) + 1);
    });
  }

  console.log('  Running benchmark...');

  // Run workers sending player input at 10Hz
  const runWorker = async (clientIdx: number) => {
    const client = clients[clientIdx];
    let localInputs = 0;
    let localErrors = 0;
    const playerId = BigInt(clientIdx + 1);

    // Stagger start times
    let nextUpdateTime = startTime + (clientIdx * (intervalMs / playerCount));

    while (Date.now() < endTime) {
      const now = Date.now();

      if (now < nextUpdateTime) {
        await sleep(nextUpdateTime - now);
      }

      nextUpdateTime += intervalMs;

      try {
        // Submit player input (buffered by server until next tick)
        client.reducers.submitInput({
          playerId,
          x: Math.random() * 100,
          y: Math.random() * 100,
          z: 0,
          rotation: Math.random() * 360,
          velocityX: Math.random() * 10 - 5,
          velocityY: Math.random() * 10 - 5,
          velocityZ: 0
        });
        localInputs++;
      } catch (err: any) {
        localErrors++;
      }
    }

    return { inputs: localInputs, errors: localErrors };
  };

  // Run all workers concurrently
  const workerPromises = clients.map((_, i) => runWorker(i));
  const results = await Promise.all(workerPromises);

  // Aggregate results
  for (const result of results) {
    totalInputs += result.inputs;
    errors += result.errors;
  }

  // Calculate average world state updates per client
  let totalUpdates = 0;
  for (const count of updateCounts.values()) {
    totalUpdates += count;
  }
  const avgUpdatesPerClient = totalUpdates / clients.length;

  const actualDuration = Date.now() - startTime;
  const inputsPerSecond = (totalInputs / actualDuration) * 1000;
  const expectedInputsPerSec = playerCount * CLIENT_TICK_RATE;
  const efficiency = (inputsPerSecond / expectedInputsPerSec) * 100;
  const updatesPerSecondPerClient = (avgUpdatesPerClient / actualDuration) * 1000;

  console.log(`  Inputs: ${inputsPerSecond.toFixed(0)} ops/sec (${efficiency.toFixed(1)}% efficiency)`);
  console.log(`  World state updates: ${updatesPerSecondPerClient.toFixed(0)}/sec per client (expected: 120)`);

  // Stop game loop and cleanup
  try {
    clients[0].reducers.stopGameLoop();
  } catch (e) {}

  for (let i = 0; i < clients.length; i++) {
    try {
      clients[i].reducers.leaveGame({ playerId: BigInt(i + 1) });
    } catch (e) {}
  }

  for (const client of clients) {
    try { client.disconnect(); } catch (e) {}
  }

  await sleep(1000);

  return {
    players: playerCount,
    expectedInputsPerSec: expectedInputsPerSec,
    actualInputsPerSec: Math.round(inputsPerSecond),
    worldStateUpdatesPerClient: Math.round(updatesPerSecondPerClient),
    efficiency: efficiency,
    errors: errors
  };
}

async function runBatchedBenchmark() {
  console.log(`SpacetimeDB Batched World State Scalability Test`);
  console.log(`================================================`);
  console.log(`URL: ${WS_URL}, Module: ${MODULE_NAME}`);
  console.log(`Client input rate: ${CLIENT_TICK_RATE} Hz (per player)`);
  console.log(`Server game loop: ${SERVER_TICK_RATE} Hz (batched broadcast)`);
  console.log(`Duration per test: ${DURATION_MS}ms`);
  console.log(`Testing from ${START_PLAYERS} to ${MAX_PLAYERS} players (step: ${PLAYER_STEP})`);
  console.log('');
  console.log('Architecture (proper game server):');
  console.log(`  - Each player sends input at ${CLIENT_TICK_RATE}Hz`);
  console.log(`  - Server collects all inputs, runs physics at ${SERVER_TICK_RATE}Hz`);
  console.log(`  - Server broadcasts ONE world_state update per tick to all clients`);
  console.log(`  - Each client receives exactly ${SERVER_TICK_RATE} updates/sec (batched)`);
  console.log('');

  const results: TestResult[] = [];

  for (let players = START_PLAYERS; players <= MAX_PLAYERS; players += PLAYER_STEP) {
    const result = await runTest(players);
    results.push(result);

    if (result.errors === -1) {
      console.log(`\n  Connection failures at ${players} players - stopping test`);
      break;
    }

    if (result.efficiency < 50) {
      console.log(`\n  Efficiency dropped below 50% at ${players} players - stopping test`);
      break;
    }
  }

  // Print summary
  console.log('\n');
  console.log('='.repeat(85));
  console.log('BATCHED WORLD STATE SCALABILITY RESULTS');
  console.log('='.repeat(85));
  console.log(`Client: ${CLIENT_TICK_RATE} Hz input | Server: ${SERVER_TICK_RATE} Hz batched broadcast`);
  console.log('');
  console.log('Players | Expected in/s | Actual in/s | Updates/client/s | Efficiency');
  console.log('-'.repeat(85));

  let peakPlayers = 0;

  for (const r of results) {
    if (r.errors === -1) {
      console.log(`${r.players.toString().padStart(7)} | CONNECTION FAILED`);
    } else {
      const status = r.efficiency >= 95 ? ' ' : r.efficiency >= 80 ? ' ' : ' ';
      console.log(`${r.players.toString().padStart(7)} | ${r.expectedInputsPerSec.toString().padStart(13)} | ${r.actualInputsPerSec.toString().padStart(11)} | ${r.worldStateUpdatesPerClient.toString().padStart(16)} | ${r.efficiency.toFixed(1).padStart(9)}%${status}`);

      if (r.efficiency >= 80 && r.players > peakPlayers) {
        peakPlayers = r.players;
      }
    }
  }

  console.log('-'.repeat(85));
  console.log('');

  if (peakPlayers > 0) {
    const peakResult = results.find(r => r.players === peakPlayers);
    console.log(`PEAK SUSTAINABLE LOAD: ${peakPlayers} players`);
    console.log(`  - Input throughput: ${peakResult?.actualInputsPerSec} inputs/sec`);
    console.log(`  - Each client receiving ~${peakResult?.worldStateUpdatesPerClient} world state updates/sec`);
    console.log(`  - Total outbound: ${peakPlayers} clients x 120 updates = ${peakPlayers * 120} updates/sec`);
  } else {
    console.log(`Could not sustain even ${START_PLAYERS} players at ${CLIENT_TICK_RATE} Hz input`);
  }

  console.log('');
  console.log('CSV: players,client_hz,server_hz,expected_inputs,actual_inputs,updates_per_client,efficiency');
  for (const r of results) {
    if (r.errors !== -1) {
      console.log(`CSV: ${r.players},${CLIENT_TICK_RATE},${SERVER_TICK_RATE},${r.expectedInputsPerSec},${r.actualInputsPerSec},${r.worldStateUpdatesPerClient},${r.efficiency.toFixed(1)}`);
    }
  }

  process.exit(0);
}

runBatchedBenchmark().catch(err => {
  console.error('Batched benchmark failed:', err);
  process.exit(1);
});
