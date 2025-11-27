// SpacetimeDB WebSocket Benchmark - Game Loop Scalability Test
// Tests how many concurrent players can be sustained with a 120Hz server game loop
// Players send input at 10Hz, receive game state updates at 120Hz from the server

import { DbConnection } from './spacetimedb-types';

const WS_URL = process.env.STDB_URL || 'ws://localhost:3001';
const MODULE_NAME = process.env.STDB_MODULE || 'benchmark';
const CLIENT_TICK_RATE = parseInt(process.env.CLIENT_TICK || '10', 10); // 10 Hz client input
const SERVER_TICK_RATE = 120; // 120 Hz server loop (informational)
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
        // Subscribe to game state and player states - these are updated by the server at 120Hz
        conn.subscriptionBuilder().subscribe([
          'SELECT * FROM game_state',
          'SELECT * FROM player_states'
        ]);
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
  serverUpdatesReceived: number;
  efficiency: number;
  errors: number;
}

async function runTest(playerCount: number): Promise<TestResult> {
  const clients: any[] = [];
  const BATCH_SIZE = 25;
  const intervalMs = 1000 / CLIENT_TICK_RATE;

  console.log(`\n--- Testing ${playerCount} players (10Hz input, 120Hz server loop) ---`);

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
      serverUpdatesReceived: 0,
      efficiency: 0,
      errors: -1
    };
  }

  console.log(`  Connected ${playerCount} players`);

  // Wait for subscriptions
  await sleep(1000);

  // Start the game loop on the server (only one client needs to do this)
  try {
    console.log('  Starting server game loop (120Hz)...');
    clients[0].reducers.startGameLoop();
    await sleep(500);
  } catch (err: any) {
    // Game loop might already be running
    console.log(`  Game loop already running or error: ${err.message}`);
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
  let serverUpdates = 0;
  const startTime = Date.now();
  const endTime = startTime + DURATION_MS;

  // Track server updates received
  const updateCounts = new Map<number, number>();
  for (let i = 0; i < clients.length; i++) {
    updateCounts.set(i, 0);
    // Subscribe to game_state updates
    clients[i].db.gameState.onUpdate(() => {
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
        // Send player input (position/velocity update)
        client.reducers.playerInput({
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

  // Count total server updates received
  for (const count of updateCounts.values()) {
    serverUpdates += count;
  }

  const actualDuration = Date.now() - startTime;
  const inputsPerSecond = (totalInputs / actualDuration) * 1000;
  const expectedInputsPerSec = playerCount * CLIENT_TICK_RATE;
  const efficiency = (inputsPerSecond / expectedInputsPerSec) * 100;
  const serverUpdatesPerSec = (serverUpdates / actualDuration) * 1000;

  console.log(`  Inputs: ${inputsPerSecond.toFixed(0)} ops/sec (${efficiency.toFixed(1)}% efficiency)`);
  console.log(`  Server updates received: ${serverUpdatesPerSec.toFixed(0)}/sec across all clients`);

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
    serverUpdatesReceived: Math.round(serverUpdatesPerSec),
    efficiency: efficiency,
    errors: errors
  };
}

async function runGameLoopBenchmark() {
  console.log(`SpacetimeDB Game Loop Scalability Test`);
  console.log(`======================================`);
  console.log(`URL: ${WS_URL}, Module: ${MODULE_NAME}`);
  console.log(`Client input rate: ${CLIENT_TICK_RATE} Hz (per player)`);
  console.log(`Server game loop: ${SERVER_TICK_RATE} Hz`);
  console.log(`Duration per test: ${DURATION_MS}ms`);
  console.log(`Testing from ${START_PLAYERS} to ${MAX_PLAYERS} players (step: ${PLAYER_STEP})`);
  console.log('');
  console.log('Architecture:');
  console.log(`  - Each player sends input at ${CLIENT_TICK_RATE}Hz (position, velocity)`);
  console.log(`  - Server runs game loop at ${SERVER_TICK_RATE}Hz (physics, state updates)`);
  console.log(`  - All players subscribed to game_state table (receive 120 updates/sec)`);
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
  console.log('='.repeat(80));
  console.log('GAME LOOP SCALABILITY RESULTS');
  console.log('='.repeat(80));
  console.log(`Client: ${CLIENT_TICK_RATE} Hz input | Server: ${SERVER_TICK_RATE} Hz game loop`);
  console.log('');
  console.log('Players | Expected inputs/s | Actual inputs/s | Server updates/s | Efficiency');
  console.log('-'.repeat(80));

  let peakPlayers = 0;

  for (const r of results) {
    if (r.errors === -1) {
      console.log(`${r.players.toString().padStart(7)} | CONNECTION FAILED`);
    } else {
      const status = r.efficiency >= 95 ? ' ' : r.efficiency >= 80 ? ' ' : ' ';
      console.log(`${r.players.toString().padStart(7)} | ${r.expectedInputsPerSec.toString().padStart(17)} | ${r.actualInputsPerSec.toString().padStart(15)} | ${r.serverUpdatesReceived.toString().padStart(16)} | ${r.efficiency.toFixed(1).padStart(9)}%${status}`);

      if (r.efficiency >= 80 && r.players > peakPlayers) {
        peakPlayers = r.players;
      }
    }
  }

  console.log('-'.repeat(80));
  console.log('');

  if (peakPlayers > 0) {
    const peakResult = results.find(r => r.players === peakPlayers);
    console.log(`PEAK SUSTAINABLE LOAD: ${peakPlayers} players`);
    console.log(`  - Client input throughput: ${peakResult?.actualInputsPerSec} inputs/sec`);
    console.log(`  - Server broadcasting updates to all clients at 120Hz`);
  } else {
    console.log(`Could not sustain even ${START_PLAYERS} players at ${CLIENT_TICK_RATE} Hz input`);
  }

  console.log('');
  console.log('CSV: players,client_hz,server_hz,expected_inputs,actual_inputs,server_updates,efficiency');
  for (const r of results) {
    if (r.errors !== -1) {
      console.log(`CSV: ${r.players},${CLIENT_TICK_RATE},${SERVER_TICK_RATE},${r.expectedInputsPerSec},${r.actualInputsPerSec},${r.serverUpdatesReceived},${r.efficiency.toFixed(1)}`);
    }
  }

  process.exit(0);
}

runGameLoopBenchmark().catch(err => {
  console.error('Game loop benchmark failed:', err);
  process.exit(1);
});
