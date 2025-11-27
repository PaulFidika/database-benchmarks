package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Target struct {
	Name      string
	Type      string   // "couchdb", "sql", "surrealdb", "spacetimedb", "postgres-http"
	URLs      []string // For HTTP endpoints
	DSNs      []string // For SQL database connection strings
	UserID    int64
	GalleryID int64
	pool      *pgxpool.Pool // Connection pool for SQL databases
}

type StageResult struct {
	Concurrency     int
	WriteOps        int64
	ReadOps         int64
	WriteFailed     int64
	ReadFailed      int64
	WritePerSec     float64
	ReadPerSec      float64
	TotalPerSec     float64
	WriteAvgLat     time.Duration
	ReadAvgLat      time.Duration
	WriteP95Lat     time.Duration
	ReadP95Lat      time.Duration
	WriteFailRate   float64
	ReadFailRate    float64
	OverallFailRate float64
}

type BenchmarkResult struct {
	Name            string
	Stages          []StageResult
	MaxWritePerSec  float64
	MaxReadPerSec   float64
	MaxTotalPerSec  float64
	PeakCleanWrite  float64
	PeakCleanRead   float64
	PeakCleanTotal  float64
	PeakCleanConc   int
	BreakingPoint   int
	SaturationPoint int
	Saturated       bool
}

var (
	stageDuration    time.Duration
	maxTimePerDB     time.Duration
	startConc        int
	maxConc          int
	concStep         int
	failThreshold    float64
	readWriteRatio   float64
	saturationWindow int
	logErrors        bool
	onlyDB           string
	poolSize         int
	errorSamples     sync.Map
)

func init() {
	flag.DurationVar(&stageDuration, "stage-duration", 10*time.Second, "duration per concurrency stage")
	flag.DurationVar(&maxTimePerDB, "max-time", 2*time.Minute, "maximum time to spend testing each database")
	flag.IntVar(&startConc, "start", 10, "starting concurrency")
	flag.IntVar(&maxConc, "max", 500, "maximum concurrency")
	flag.IntVar(&concStep, "step", 10, "concurrency increase per stage")
	flag.Float64Var(&failThreshold, "fail-threshold", 5.0, "failure rate % to consider as breaking point")
	flag.Float64Var(&readWriteRatio, "read-ratio", 0.7, "ratio of reads vs writes (0.7 = 70% reads, 30% writes)")
	flag.IntVar(&saturationWindow, "saturation-window", 3, "number of stages with <5% improvement to detect saturation")
	flag.BoolVar(&logErrors, "log-errors", false, "log sample errors during benchmark")
	flag.StringVar(&onlyDB, "only", "", "only test specific database (couchdb, cockroachdb, yugabyte, postgres, surrealdb, spacetimedb)")
	flag.IntVar(&poolSize, "pool-size", 200, "connection pool size for SQL databases")
}

func main() {
	flag.Parse()

	// Define targets with DIRECT database connections
	targets := []Target{
		{
			Name: "couchdb",
			Type: "couchdb",
			URLs: []string{
				"http://localhost:5984",
				"http://localhost:5985",
				"http://localhost:5986",
			},
		},
		{
			Name: "cockroachdb",
			Type: "sql",
			DSNs: []string{
				"postgresql://root@localhost:26257/demo?sslmode=disable",
				"postgresql://root@localhost:26258/demo?sslmode=disable",
				"postgresql://root@localhost:26259/demo?sslmode=disable",
			},
		},
		{
			Name: "yugabyte",
			Type: "sql",
			DSNs: []string{
				"postgresql://yugabyte@localhost:5433/demo?sslmode=disable",
				"postgresql://yugabyte@localhost:5434/demo?sslmode=disable",
				"postgresql://yugabyte@localhost:5435/demo?sslmode=disable",
			},
		},
		{
			Name: "postgres-binary",
			Type: "sql",
			DSNs: []string{
				"postgresql://postgres:password@localhost:5436/demo?sslmode=disable",
			},
		},
		{
			Name: "postgres-http",
			Type: "postgres-http",
			URLs: []string{
				"http://localhost:9090",
			},
		},
		{
			Name: "surrealdb",
			Type: "surrealdb",
			URLs: []string{
				"http://localhost:8000",
			},
		},
		{
			Name: "spacetimedb-http",
			Type: "spacetimedb",
			URLs: []string{
				"http://localhost:3001",
			},
		},
		// NOTE: spacetimedb-ws benchmark is done separately in Node.js
		// See bench/spacetimedb-ws-bench/bench.ts
	}

	fmt.Println("================================================================")
	fmt.Println("  Database Stress Test - DIRECT DATABASE ACCESS")
	fmt.Println("================================================================")
	fmt.Printf("Stage duration:    %v\n", stageDuration)
	fmt.Printf("Max time per DB:   %v\n", maxTimePerDB)
	fmt.Printf("Concurrency:       %d -> %d (step %d)\n", startConc, maxConc, concStep)
	fmt.Printf("Pool size:         %d connections\n", poolSize)
	fmt.Printf("Failure threshold: %.1f%%\n", failThreshold)
	fmt.Printf("Read/Write ratio:  %.0f%% reads, %.0f%% writes\n", readWriteRatio*100, (1-readWriteRatio)*100)
	fmt.Printf("Saturation window: %d stages with <5%% improvement\n", saturationWindow)
	fmt.Println()

	// Initialize SQL connection pools and set up test data
	fmt.Println("Initializing database connections...")
	ctx := context.Background()
	for i := range targets {
		if onlyDB != "" && targets[i].Name != onlyDB {
			continue
		}

		if targets[i].Type == "sql" {
			pool, err := createPool(ctx, targets[i].DSNs, poolSize)
			if err != nil {
				fmt.Printf("  %s: FAILED - %v\n", targets[i].Name, err)
				continue
			}
			targets[i].pool = pool

			// Ensure schema exists and get test data IDs
			userID, galleryID, err := setupSQLTestData(ctx, pool, targets[i].Name)
			if err != nil {
				fmt.Printf("  %s: FAILED to setup test data - %v\n", targets[i].Name, err)
				continue
			}
			targets[i].UserID = userID
			targets[i].GalleryID = galleryID
			fmt.Printf("  %s: OK (user_id=%d, gallery_id=%d)\n", targets[i].Name, userID, galleryID)
		} else if targets[i].Type == "couchdb" {
			// CouchDB uses HTTP, verify it's accessible
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(targets[i].URLs[0] + "/")
			if err != nil {
				fmt.Printf("  %s: FAILED - %v\n", targets[i].Name, err)
				continue
			}
			resp.Body.Close()
			targets[i].UserID = 1 // Dummy, CouchDB uses string IDs
			targets[i].GalleryID = 1
			fmt.Printf("  %s: OK (3 nodes via HTTP)\n", targets[i].Name)
		} else if targets[i].Type == "surrealdb" {
			// SurrealDB uses HTTP, verify it's accessible and setup test data
			err := setupSurrealDB(targets[i].URLs[0])
			if err != nil {
				fmt.Printf("  %s: FAILED - %v\n", targets[i].Name, err)
				continue
			}
			targets[i].UserID = 1
			targets[i].GalleryID = 1
			fmt.Printf("  %s: OK (single instance via HTTP)\n", targets[i].Name)
		} else if targets[i].Type == "spacetimedb" {
			// SpacetimeDB uses HTTP, verify it's accessible
			err := setupSpacetimeDB(targets[i].URLs[0])
			if err != nil {
				fmt.Printf("  %s: FAILED - %v\n", targets[i].Name, err)
				continue
			}
			targets[i].UserID = 1
			targets[i].GalleryID = 1
			fmt.Printf("  %s: OK (single instance via HTTP)\n", targets[i].Name)
		} else if targets[i].Type == "postgres-http" {
			// PostgreSQL via HTTP API server
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(targets[i].URLs[0] + "/health")
			if err != nil {
				fmt.Printf("  %s: FAILED - %v\n", targets[i].Name, err)
				continue
			}
			resp.Body.Close()
			targets[i].UserID = 1
			targets[i].GalleryID = 1
			fmt.Printf("  %s: OK (via HTTP API server)\n", targets[i].Name)
		}
	}
	fmt.Println()

	// Run benchmarks
	var results []BenchmarkResult
	for i, target := range targets {
		if onlyDB != "" && target.Name != onlyDB {
			continue
		}
		if target.Type == "sql" && target.pool == nil {
			continue
		}

		result := runRampBenchmark(&targets[i])
		results = append(results, result)
		fmt.Println()

		// Close pool after benchmark
		if target.pool != nil {
			target.pool.Close()
			targets[i].pool = nil
		}

		// Cooldown between tests
		if i < len(targets)-1 && onlyDB == "" {
			fmt.Println("  Cooldown period (3s)...")
			time.Sleep(3 * time.Second)
		}
	}

	printSummary(results)
}

func createPool(ctx context.Context, dsns []string, maxConns int) (*pgxpool.Pool, error) {
	// Use first DSN for the pool (pgxpool handles connection distribution)
	// For distributed DBs, we'll round-robin across DSNs manually in the benchmark
	config, err := pgxpool.ParseConfig(dsns[0])
	if err != nil {
		return nil, err
	}

	config.MaxConns = int32(maxConns)
	config.MinConns = int32(maxConns / 4)
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func setupSQLTestData(ctx context.Context, pool *pgxpool.Pool, name string) (int64, int64, error) {
	// Create schema
	schema := `
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		);
		CREATE TABLE IF NOT EXISTS galleries (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES users(id),
			title VARCHAR(255) NOT NULL,
			description TEXT,
			created_at TIMESTAMP DEFAULT NOW()
		);
		CREATE TABLE IF NOT EXISTS comments (
			id SERIAL PRIMARY KEY,
			gallery_id INTEGER REFERENCES galleries(id),
			user_id INTEGER REFERENCES users(id),
			text TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		);
		CREATE INDEX IF NOT EXISTS idx_comments_gallery ON comments(gallery_id);
	`
	_, err := pool.Exec(ctx, schema)
	if err != nil {
		return 0, 0, fmt.Errorf("schema creation failed: %w", err)
	}

	// Check for existing test user
	var userID int64
	err = pool.QueryRow(ctx, "SELECT id FROM users WHERE email = 'bench@test.com' LIMIT 1").Scan(&userID)
	if err != nil {
		// Create test user
		err = pool.QueryRow(ctx,
			"INSERT INTO users (name, email) VALUES ('Benchmark User', 'bench@test.com') RETURNING id",
		).Scan(&userID)
		if err != nil {
			return 0, 0, fmt.Errorf("user creation failed: %w", err)
		}
	}

	// Check for existing test gallery
	var galleryID int64
	err = pool.QueryRow(ctx, "SELECT id FROM galleries WHERE user_id = $1 LIMIT 1", userID).Scan(&galleryID)
	if err != nil {
		// Create test gallery
		err = pool.QueryRow(ctx,
			"INSERT INTO galleries (user_id, title, description) VALUES ($1, 'Benchmark Gallery', 'For benchmarking') RETURNING id",
			userID,
		).Scan(&galleryID)
		if err != nil {
			return 0, 0, fmt.Errorf("gallery creation failed: %w", err)
		}
	}

	return userID, galleryID, nil
}

func runRampBenchmark(target *Target) BenchmarkResult {
	fmt.Printf("================================================================\n")
	fmt.Printf("  Testing: %s (DIRECT)\n", target.Name)
	fmt.Printf("================================================================\n")

	result := BenchmarkResult{Name: target.Name}
	var prevThroughput float64
	stagnantStages := 0
	benchmarkStart := time.Now()

	for conc := startConc; conc <= maxConc; conc += concStep {
		if time.Since(benchmarkStart) >= maxTimePerDB {
			fmt.Printf("\n  *** TIME LIMIT reached (max %v per database) ***\n", maxTimePerDB)
			break
		}

		fmt.Printf("\n--- Concurrency: %d ---\n", conc)

		stage := runStage(target, conc, stageDuration)
		result.Stages = append(result.Stages, stage)

		if stage.WritePerSec > result.MaxWritePerSec {
			result.MaxWritePerSec = stage.WritePerSec
		}
		if stage.ReadPerSec > result.MaxReadPerSec {
			result.MaxReadPerSec = stage.ReadPerSec
		}
		if stage.TotalPerSec > result.MaxTotalPerSec {
			result.MaxTotalPerSec = stage.TotalPerSec
		}

		if stage.OverallFailRate == 0 && stage.TotalPerSec > result.PeakCleanTotal {
			result.PeakCleanWrite = stage.WritePerSec
			result.PeakCleanRead = stage.ReadPerSec
			result.PeakCleanTotal = stage.TotalPerSec
			result.PeakCleanConc = conc
		}

		fmt.Printf("  Writes: %7.1f/s (p95: %8v) failed: %5.2f%%\n",
			stage.WritePerSec, stage.WriteP95Lat.Round(time.Microsecond*100), stage.WriteFailRate)
		fmt.Printf("  Reads:  %7.1f/s (p95: %8v) failed: %5.2f%%\n",
			stage.ReadPerSec, stage.ReadP95Lat.Round(time.Microsecond*100), stage.ReadFailRate)
		fmt.Printf("  Total:  %7.1f/s  |  Fail rate: %.2f%%\n", stage.TotalPerSec, stage.OverallFailRate)

		if prevThroughput > 0 {
			improvement := (stage.TotalPerSec - prevThroughput) / prevThroughput * 100
			if improvement < 5.0 {
				stagnantStages++
				if stagnantStages >= saturationWindow && !result.Saturated {
					result.SaturationPoint = conc - (saturationWindow-1)*concStep
					result.Saturated = true
					fmt.Printf("  ** SATURATION detected at ~%d concurrent (throughput plateau) **\n", result.SaturationPoint)
				}
			} else {
				stagnantStages = 0
			}
		}
		prevThroughput = stage.TotalPerSec

		if stage.OverallFailRate >= failThreshold {
			result.BreakingPoint = conc
			fmt.Printf("\n  *** BREAKING POINT at concurrency %d (%.1f%% failures) ***\n", conc, stage.OverallFailRate)
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	if result.BreakingPoint == 0 {
		result.BreakingPoint = maxConc
		fmt.Printf("\n  Completed all stages without breaking (max conc: %d)\n", maxConc)
	}

	if logErrors {
		if readErr, ok := errorSamples.Load(target.Name + "_read"); ok {
			fmt.Printf("  Sample read error: %s\n", readErr)
		}
		if writeErr, ok := errorSamples.Load(target.Name + "_write"); ok {
			fmt.Printf("  Sample write error: %s\n", writeErr)
		}
		errorSamples.Delete(target.Name + "_read")
		errorSamples.Delete(target.Name + "_write")
	}

	return result
}

func runStage(target *Target, conc int, duration time.Duration) StageResult {
	var writeOps, readOps, writeFail, readFail int64
	var writeLatSum, readLatSum int64
	var writeLats, readLats []time.Duration
	var latMu sync.Mutex

	deadline := time.Now().Add(duration)
	ctx := context.Background()

	// For HTTP-based databases (CouchDB, SurrealDB, SpacetimeDB, postgres-http), create shared HTTP client
	var httpClient *http.Client
	if target.Type == "couchdb" || target.Type == "surrealdb" || target.Type == "spacetimedb" || target.Type == "postgres-http" {
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        500,
				MaxIdleConnsPerHost: 500,
				MaxConnsPerHost:     0,
				IdleConnTimeout:     90 * time.Second,
			},
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()

			for time.Now().Before(deadline) {
				isRead := rand.Float64() < readWriteRatio
				start := time.Now()

				var err error
				if target.Type == "couchdb" {
					url := target.URLs[workerID%len(target.URLs)]
					if isRead {
						err = readCouchDB(httpClient, url)
					} else {
						err = writeCouchDB(httpClient, url)
					}
				} else if target.Type == "surrealdb" {
					url := target.URLs[workerID%len(target.URLs)]
					if isRead {
						err = readSurrealDB(httpClient, url)
					} else {
						err = writeSurrealDB(httpClient, url)
					}
				} else if target.Type == "spacetimedb" {
					url := target.URLs[workerID%len(target.URLs)]
					if isRead {
						err = readSpacetimeDB(httpClient, url)
					} else {
						err = writeSpacetimeDB(httpClient, url)
					}
				} else if target.Type == "postgres-http" {
					url := target.URLs[workerID%len(target.URLs)]
					if isRead {
						err = readPostgresHTTP(httpClient, url, target.GalleryID)
					} else {
						err = writePostgresHTTP(httpClient, url, target.UserID, target.GalleryID)
					}
				} else {
					// Direct SQL
					if isRead {
						err = readSQL(ctx, target.pool, target.GalleryID)
					} else {
						err = writeSQL(ctx, target.pool, target.UserID, target.GalleryID)
					}
				}

				lat := time.Since(start)

				if isRead {
					atomic.AddInt64(&readOps, 1)
					atomic.AddInt64(&readLatSum, int64(lat))
					latMu.Lock()
					readLats = append(readLats, lat)
					latMu.Unlock()
					if err != nil {
						atomic.AddInt64(&readFail, 1)
						if logErrors {
							errorSamples.LoadOrStore(target.Name+"_read", err.Error())
						}
					}
				} else {
					atomic.AddInt64(&writeOps, 1)
					atomic.AddInt64(&writeLatSum, int64(lat))
					latMu.Lock()
					writeLats = append(writeLats, lat)
					latMu.Unlock()
					if err != nil {
						atomic.AddInt64(&writeFail, 1)
						if logErrors {
							errorSamples.LoadOrStore(target.Name+"_write", err.Error())
						}
					}
				}
			}
		}()
	}
	wg.Wait()

	elapsed := duration.Seconds()
	result := StageResult{
		Concurrency: conc,
		WriteOps:    writeOps,
		ReadOps:     readOps,
		WriteFailed: writeFail,
		ReadFailed:  readFail,
	}

	result.WritePerSec = float64(writeOps-writeFail) / elapsed
	result.ReadPerSec = float64(readOps-readFail) / elapsed
	result.TotalPerSec = result.WritePerSec + result.ReadPerSec

	if writeOps > 0 {
		result.WriteAvgLat = time.Duration(writeLatSum / writeOps)
		result.WriteFailRate = float64(writeFail) / float64(writeOps) * 100
	}
	if readOps > 0 {
		result.ReadAvgLat = time.Duration(readLatSum / readOps)
		result.ReadFailRate = float64(readFail) / float64(readOps) * 100
	}

	totalOps := writeOps + readOps
	totalFail := writeFail + readFail
	if totalOps > 0 {
		result.OverallFailRate = float64(totalFail) / float64(totalOps) * 100
	}

	// Calculate p95 latencies
	if len(writeLats) > 0 {
		sort.Slice(writeLats, func(i, j int) bool { return writeLats[i] < writeLats[j] })
		result.WriteP95Lat = writeLats[int(float64(len(writeLats))*0.95)]
	}
	if len(readLats) > 0 {
		sort.Slice(readLats, func(i, j int) bool { return readLats[i] < readLats[j] })
		result.ReadP95Lat = readLats[int(float64(len(readLats))*0.95)]
	}

	return result
}

// CouchDB operations (HTTP)
func writeCouchDB(client *http.Client, baseURL string) error {
	doc := map[string]interface{}{
		"_id":        fmt.Sprintf("bench:%d:%d", time.Now().UnixNano(), rand.Int63()),
		"type":       "comment",
		"gallery_id": "gallery:bench:1",
		"user_id":    "user:bench:1",
		"user_name":  "Benchmark User",
		"text":       fmt.Sprintf("Benchmark comment %d", rand.Int()),
		"created_at": time.Now().Format(time.RFC3339),
	}

	data, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", baseURL+"/comments", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth("admin", "password")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("write status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func readCouchDB(client *http.Client, baseURL string) error {
	req, err := http.NewRequest("GET", baseURL+"/comments/_all_docs?limit=20&include_docs=true", nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth("admin", "password")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("read status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// SQL operations (direct database connection)
func writeSQL(ctx context.Context, pool *pgxpool.Pool, userID, galleryID int64) error {
	_, err := pool.Exec(ctx,
		"INSERT INTO comments (gallery_id, user_id, text) VALUES ($1, $2, $3)",
		galleryID, userID, fmt.Sprintf("Benchmark comment %d", rand.Int()),
	)
	return err
}

func readSQL(ctx context.Context, pool *pgxpool.Pool, galleryID int64) error {
	rows, err := pool.Query(ctx,
		"SELECT id, gallery_id, user_id, text, created_at FROM comments WHERE gallery_id = $1 ORDER BY created_at DESC LIMIT 20",
		galleryID,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Actually iterate through results
	for rows.Next() {
		var id, gid, uid int64
		var text string
		var createdAt time.Time
		if err := rows.Scan(&id, &gid, &uid, &text, &createdAt); err != nil {
			return err
		}
	}
	return rows.Err()
}

// SurrealDB operations (HTTP)
func setupSurrealDB(baseURL string) error {
	client := &http.Client{Timeout: 10 * time.Second}

	// Create test user
	userQuery := `CREATE users:1 SET name = 'Benchmark User', email = 'bench@test.com', created_at = time::now();`
	if err := execSurrealSQL(client, baseURL, userQuery); err != nil {
		// Ignore errors if already exists
	}

	// Create test gallery
	galleryQuery := `CREATE galleries:1 SET user_id = users:1, title = 'Benchmark Gallery', description = 'For benchmarking', created_at = time::now();`
	if err := execSurrealSQL(client, baseURL, galleryQuery); err != nil {
		// Ignore errors if already exists
	}

	// Verify connectivity with a simple query
	testQuery := `SELECT * FROM users LIMIT 1;`
	return execSurrealSQL(client, baseURL, testQuery)
}

func execSurrealSQL(client *http.Client, baseURL, query string) error {
	req, err := http.NewRequest("POST", baseURL+"/sql", strings.NewReader(query))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("surreal-ns", "bench")
	req.Header.Set("surreal-db", "demo")
	req.SetBasicAuth("root", "root")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("surrealdb status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// Check for errors in response
	var results []map[string]interface{}
	if err := json.Unmarshal(body, &results); err != nil {
		return fmt.Errorf("failed to parse response: %w", err)
	}
	for _, r := range results {
		if status, ok := r["status"].(string); ok && status != "OK" {
			return fmt.Errorf("surrealdb error: %v", r)
		}
	}

	return nil
}

func writeSurrealDB(client *http.Client, baseURL string) error {
	query := fmt.Sprintf(`CREATE comments SET gallery_id = galleries:1, user_id = users:1, text = 'Benchmark comment %d', created_at = time::now();`, rand.Int())

	req, err := http.NewRequest("POST", baseURL+"/sql", strings.NewReader(query))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("surreal-ns", "bench")
	req.Header.Set("surreal-db", "demo")
	req.SetBasicAuth("root", "root")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("write status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

func readSurrealDB(client *http.Client, baseURL string) error {
	query := `SELECT * FROM comments WHERE gallery_id = galleries:1 ORDER BY created_at DESC LIMIT 20;`

	req, err := http.NewRequest("POST", baseURL+"/sql", strings.NewReader(query))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("surreal-ns", "bench")
	req.Header.Set("surreal-db", "demo")
	req.SetBasicAuth("root", "root")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("read status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

// SpacetimeDB operations (HTTP)
func setupSpacetimeDB(baseURL string) error {
	// SpacetimeDB module was pre-published with init reducer
	// Just verify connectivity by pinging the server
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get(baseURL + "/v1/ping")
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("spacetimedb ping failed: %d", resp.StatusCode)
	}
	return nil
}

func writeSpacetimeDB(client *http.Client, baseURL string) error {
	// Call the add_comment reducer: add_comment(gallery_id: u64, user_id: u64, text: String)
	payload := fmt.Sprintf(`[1, 1, "Benchmark comment %d"]`, rand.Int())

	req, err := http.NewRequest("POST", baseURL+"/v1/database/benchmark/call/add_comment", strings.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("write status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func readSpacetimeDB(client *http.Client, baseURL string) error {
	// Call the get_comments_for_gallery reducer
	payload := `[1]`

	req, err := http.NewRequest("POST", baseURL+"/v1/database/benchmark/call/get_comments_for_gallery", strings.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("read status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func writePostgresHTTP(client *http.Client, baseURL string, userID, galleryID int64) error {
	// POST /pg/comments with JSON body
	payload := fmt.Sprintf(`{"gallery_id": %d, "user_id": %d, "text": "Benchmark comment %d"}`, galleryID, userID, rand.Int())

	req, err := http.NewRequest("POST", baseURL+"/pg/comments", strings.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("write status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func readPostgresHTTP(client *http.Client, baseURL string, galleryID int64) error {
	// GET /pg/comments?gallery_id=X
	resp, err := client.Get(fmt.Sprintf("%s/pg/comments?gallery_id=%d", baseURL, galleryID))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("read status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	// Read and discard the body to allow connection reuse
	io.Copy(io.Discard, resp.Body)
	return nil
}

func printSummary(results []BenchmarkResult) {
	fmt.Println("\n================================================================")
	fmt.Println("  FINAL SUMMARY")
	fmt.Println("================================================================")

	// Sort by peak clean throughput
	sorted := make([]BenchmarkResult, len(results))
	copy(sorted, results)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].PeakCleanTotal > sorted[j].PeakCleanTotal
	})

	fmt.Println("\nRanked by Peak Throughput (0% failure rate):")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────")
	fmt.Printf("%-15s %12s %12s %12s %10s %14s %12s\n",
		"Database", "Peak Write/s", "Peak Read/s", "Peak Total/s", "@ Conc", "Saturation", "Break Point")
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────")

	for i, r := range sorted {
		satStr := "none"
		if r.Saturated {
			satStr = fmt.Sprintf("%d conc", r.SaturationPoint)
		}
		breakStr := fmt.Sprintf("%d conc", r.BreakingPoint)
		if r.BreakingPoint >= maxConc {
			breakStr = fmt.Sprintf(">%d conc", maxConc)
		}
		concStr := "N/A"
		if r.PeakCleanConc > 0 {
			concStr = fmt.Sprintf("%d", r.PeakCleanConc)
		}
		fmt.Printf("%d. %-12s %12.1f %12.1f %12.1f %10s %14s %12s\n",
			i+1, r.Name, r.PeakCleanWrite, r.PeakCleanRead, r.PeakCleanTotal,
			concStr, satStr, breakStr)
	}
	fmt.Println("─────────────────────────────────────────────────────────────────────────────────────")

	// Detailed results
	fmt.Println("\nDetailed Performance at Each Stage:")
	for _, r := range sorted {
		fmt.Printf("\n%s:\n", r.Name)
		fmt.Printf("  %-6s %10s %10s %10s %10s %10s %8s\n",
			"Conc", "Write/s", "Read/s", "Total/s", "W p95", "R p95", "Fail%")
		for _, s := range r.Stages {
			fmt.Printf("  %-6d %10.1f %10.1f %10.1f %10v %10v %7.2f%%\n",
				s.Concurrency, s.WritePerSec, s.ReadPerSec, s.TotalPerSec,
				s.WriteP95Lat.Round(time.Millisecond), s.ReadP95Lat.Round(time.Millisecond),
				s.OverallFailRate)
		}
	}
}
