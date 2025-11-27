// SurrealDB WebSocket Benchmark (Binary CBOR Protocol)
// Fair comparison with PostgreSQL binary protocol (pgx)

import { createRequire } from 'module';
const require = createRequire(import.meta.url);
import Surreal from 'surrealdb';

const WS_URL = process.env.SURREAL_URL || 'ws://127.0.0.1:8000/rpc';
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '10', 10);
const DURATION_MS = parseInt(process.env.DURATION || '10000', 10);
const READ_RATIO = 0.7; // 70% reads, 30% writes (same as Go benchmark)

interface BenchResult {
  totalOps: number;
  writes: number;
  reads: number;
  errors: number;
  durationMs: number;
}

async function setupDatabase(db: Surreal): Promise<void> {
  // Setup schema with indexes (same as we did manually)
  await db.query(`
    REMOVE TABLE IF EXISTS comments;
    REMOVE TABLE IF EXISTS galleries;
    REMOVE TABLE IF EXISTS users;

    DEFINE TABLE users SCHEMAFULL;
    DEFINE FIELD name ON users TYPE string;
    DEFINE FIELD email ON users TYPE string;
    DEFINE FIELD created_at ON users TYPE datetime;

    DEFINE TABLE galleries SCHEMAFULL;
    DEFINE FIELD user_id ON galleries TYPE record<users>;
    DEFINE FIELD title ON galleries TYPE string;
    DEFINE FIELD description ON galleries TYPE string;
    DEFINE FIELD created_at ON galleries TYPE datetime;

    DEFINE TABLE comments SCHEMAFULL;
    DEFINE FIELD gallery_id ON comments TYPE record<galleries>;
    DEFINE FIELD user_id ON comments TYPE record<users>;
    DEFINE FIELD text ON comments TYPE string;
    DEFINE FIELD created_at ON comments TYPE datetime;

    DEFINE INDEX idx_comments_gallery ON comments FIELDS gallery_id;
    DEFINE INDEX idx_comments_created ON comments FIELDS created_at;
  `);

  // Create test data
  await db.query(`
    CREATE users:1 SET name = 'Benchmark User', email = 'bench@test.com', created_at = time::now();
    CREATE galleries:1 SET user_id = users:1, title = 'Benchmark Gallery', description = 'For benchmarking', created_at = time::now();
  `);
}

async function runWorker(
  workerId: number,
  endTime: number
): Promise<{ writes: number; reads: number; errors: number }> {
  // Each worker gets its own connection (like pgx connection pool)
  const db = new Surreal();

  try {
    await db.connect(WS_URL);
    await db.signin({ username: 'root', password: 'root' });
    await db.use({ namespace: 'bench', database: 'demo' });
  } catch (err) {
    console.error(`Worker ${workerId} connection failed:`, err);
    return { writes: 0, reads: 0, errors: 1 };
  }

  let writes = 0;
  let reads = 0;
  let errors = 0;

  while (Date.now() < endTime) {
    const isRead = Math.random() < READ_RATIO;

    try {
      if (isRead) {
        // Read: get recent comments for gallery
        await db.query(
          'SELECT * FROM comments WHERE gallery_id = galleries:1 ORDER BY created_at DESC LIMIT 20'
        );
        reads++;
      } else {
        // Write: create a comment
        await db.query(
          `CREATE comments SET
            gallery_id = galleries:1,
            user_id = users:1,
            text = $text,
            created_at = time::now()`,
          { text: `Benchmark comment ${Date.now()}` }
        );
        writes++;
      }
    } catch (err) {
      errors++;
    }
  }

  await db.close();
  return { writes, reads, errors };
}

async function runBenchmark(): Promise<BenchResult> {
  console.log(`SurrealDB WebSocket Benchmark (Binary CBOR Protocol)`);
  console.log(`URL: ${WS_URL}`);
  console.log(`Concurrency: ${CONCURRENCY}, Duration: ${DURATION_MS}ms`);
  console.log(`Read/Write ratio: ${READ_RATIO * 100}% reads, ${(1 - READ_RATIO) * 100}% writes`);
  console.log('');

  // Setup database with one connection
  console.log('Setting up database with indexes...');
  const setupDb = new Surreal();
  await setupDb.connect(WS_URL);
  await setupDb.signin({ username: 'root', password: 'root' });
  await setupDb.use({ namespace: 'bench', database: 'demo' });
  await setupDatabase(setupDb);
  await setupDb.close();
  console.log('Database ready with indexes on gallery_id and created_at');
  console.log('');

  console.log(`Starting ${CONCURRENCY} concurrent workers...`);
  const startTime = Date.now();
  const endTime = startTime + DURATION_MS;

  // Launch all workers concurrently
  const workerPromises = Array.from({ length: CONCURRENCY }, (_, i) =>
    runWorker(i, endTime)
  );

  const results = await Promise.all(workerPromises);

  const totalWrites = results.reduce((sum, r) => sum + r.writes, 0);
  const totalReads = results.reduce((sum, r) => sum + r.reads, 0);
  const totalErrors = results.reduce((sum, r) => sum + r.errors, 0);
  const actualDuration = Date.now() - startTime;

  return {
    totalOps: totalWrites + totalReads,
    writes: totalWrites,
    reads: totalReads,
    errors: totalErrors,
    durationMs: actualDuration,
  };
}

async function main() {
  try {
    const result = await runBenchmark();

    const opsPerSec = (result.totalOps / result.durationMs) * 1000;
    const writesPerSec = (result.writes / result.durationMs) * 1000;
    const readsPerSec = (result.reads / result.durationMs) * 1000;
    const errorRate = (result.errors / (result.totalOps + result.errors)) * 100;

    console.log('');
    console.log('=== Results ===');
    console.log(`Total operations: ${result.totalOps}`);
    console.log(`  Writes: ${result.writes} (${writesPerSec.toFixed(1)}/sec)`);
    console.log(`  Reads: ${result.reads} (${readsPerSec.toFixed(1)}/sec)`);
    console.log(`Errors: ${result.errors} (${errorRate.toFixed(2)}%)`);
    console.log(`Duration: ${result.durationMs}ms`);
    console.log(`Throughput: ${opsPerSec.toFixed(2)} ops/sec`);
    console.log('');
    console.log(`RESULT: surrealdb-ws,${CONCURRENCY},${opsPerSec.toFixed(2)}`);

    process.exit(0);
  } catch (err) {
    console.error('Benchmark failed:', err);
    process.exit(1);
  }
}

main();
