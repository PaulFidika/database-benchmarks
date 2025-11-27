package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// Pool size per node - allows high concurrency
	poolSizePerNode = 100
)

type User struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

type Gallery struct {
	ID          int64     `json:"id"`
	UserID      int64     `json:"user_id"`
	UserName    string    `json:"user_name,omitempty"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}

type Comment struct {
	ID        int64     `json:"id"`
	GalleryID int64     `json:"gallery_id"`
	UserID    int64     `json:"user_id"`
	UserName  string    `json:"user_name"`
	Text      string    `json:"text"`
	CreatedAt time.Time `json:"created_at"`
}

// ClusterPool manages connections to multiple database nodes
type ClusterPool struct {
	pools   []*pgxpool.Pool
	counter uint64
}

// GetPool returns the next pool using round-robin
func (cp *ClusterPool) GetPool() *pgxpool.Pool {
	if len(cp.pools) == 0 {
		return nil
	}
	idx := atomic.AddUint64(&cp.counter, 1) % uint64(len(cp.pools))
	return cp.pools[idx]
}

// Close closes all pools
func (cp *ClusterPool) Close() {
	for _, p := range cp.pools {
		p.Close()
	}
}

type Server struct {
	crdbCluster *ClusterPool
	ybCluster   *ClusterPool
	pgPool      *pgxpool.Pool
}

func createPoolWithConfig(ctx context.Context, url string, poolSize int) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	config.MaxConns = int32(poolSize)
	config.MinConns = int32(poolSize / 4)
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

func connectToCluster(ctx context.Context, urls []string, name string) *ClusterPool {
	cluster := &ClusterPool{}

	for i, url := range urls {
		var pool *pgxpool.Pool
		var err error

		// Retry connection a few times
		for retry := 0; retry < 10; retry++ {
			pool, err = createPoolWithConfig(ctx, url, poolSizePerNode)
			if err == nil {
				break
			}
			log.Printf("Waiting for %s node %d... (%d/10): %v", name, i+1, retry+1, err)
			time.Sleep(2 * time.Second)
		}

		if err != nil {
			log.Printf("WARNING: Could not connect to %s node %d: %v", name, i+1, err)
			continue
		}

		cluster.pools = append(cluster.pools, pool)
		log.Printf("Connected to %s node %d (pool size: %d)", name, i+1, poolSizePerNode)
	}

	if len(cluster.pools) == 0 {
		return nil
	}

	log.Printf("%s cluster: %d/%d nodes connected (total pool: %d connections)",
		name, len(cluster.pools), len(urls), len(cluster.pools)*poolSizePerNode)
	return cluster
}

func main() {
	ctx := context.Background()

	log.Printf("Starting API server with pool size %d per node", poolSizePerNode)

	// CockroachDB cluster - all 3 nodes
	crdbURLs := []string{
		"postgresql://root@crdb1:26257/demo?sslmode=disable",
		"postgresql://root@crdb2:26257/demo?sslmode=disable",
		"postgresql://root@crdb3:26257/demo?sslmode=disable",
	}

	// First create the database on one node
	log.Println("Initializing CockroachDB...")
	initPool, err := createPoolWithConfig(ctx, "postgresql://root@crdb1:26257/defaultdb?sslmode=disable", 1)
	if err == nil {
		initPool.Exec(ctx, "CREATE DATABASE IF NOT EXISTS demo")
		initPool.Close()
	}

	crdbCluster := connectToCluster(ctx, crdbURLs, "CockroachDB")

	// YugabyteDB cluster - all 3 nodes
	ybURLs := []string{
		"postgresql://yugabyte@yb-tserver1:5433/demo?sslmode=disable",
		"postgresql://yugabyte@yb-tserver2:5433/demo?sslmode=disable",
		"postgresql://yugabyte@yb-tserver3:5433/demo?sslmode=disable",
	}

	// First create the database on one node
	log.Println("Initializing YugabyteDB...")
	initPool, err = createPoolWithConfig(ctx, "postgresql://yugabyte@yb-tserver1:5433/yugabyte?sslmode=disable", 1)
	if err == nil {
		initPool.Exec(ctx, "CREATE DATABASE demo")
		initPool.Close()
	}

	ybCluster := connectToCluster(ctx, ybURLs, "YugabyteDB")

	// PostgreSQL single instance (large pool since single node)
	log.Println("Initializing PostgreSQL...")
	var pgPool *pgxpool.Pool
	pgURL := getEnv("PG_URL", "postgresql://postgres:password@postgres:5432/demo?sslmode=disable")
	for retry := 0; retry < 10; retry++ {
		pgPool, err = createPoolWithConfig(ctx, pgURL, 300) // 300 connections for single instance
		if err == nil {
			break
		}
		log.Printf("Waiting for PostgreSQL... (%d/10): %v", retry+1, err)
		time.Sleep(2 * time.Second)
	}
	if pgPool != nil {
		log.Printf("Connected to PostgreSQL (pool size: 300)")
	} else {
		log.Printf("WARNING: Could not connect to PostgreSQL")
	}

	server := &Server{
		crdbCluster: crdbCluster,
		ybCluster:   ybCluster,
		pgPool:      pgPool,
	}

	// Initialize database schemas
	if crdbCluster != nil {
		if err := server.initSchema(ctx, crdbCluster.GetPool(), "crdb"); err != nil {
			log.Printf("Failed to init CockroachDB schema: %v", err)
		}
	}
	if ybCluster != nil {
		if err := server.initSchema(ctx, ybCluster.GetPool(), "yb"); err != nil {
			log.Printf("Failed to init YugabyteDB schema: %v", err)
		}
	}
	if pgPool != nil {
		if err := server.initSchema(ctx, pgPool, "pg"); err != nil {
			log.Printf("Failed to init PostgreSQL schema: %v", err)
		}
	}

	// Setup routes
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", server.handleHealth)

	// CockroachDB endpoints
	mux.HandleFunc("/crdb/users", server.withCORS(server.handleCRDBUsers))
	mux.HandleFunc("/crdb/galleries", server.withCORS(server.handleCRDBGalleries))
	mux.HandleFunc("/crdb/comments", server.withCORS(server.handleCRDBComments))
	mux.HandleFunc("/crdb/comments/stream", server.withCORS(server.handleCRDBCommentsStream))

	// YugabyteDB endpoints
	mux.HandleFunc("/yb/users", server.withCORS(server.handleYBUsers))
	mux.HandleFunc("/yb/galleries", server.withCORS(server.handleYBGalleries))
	mux.HandleFunc("/yb/comments", server.withCORS(server.handleYBComments))
	mux.HandleFunc("/yb/comments/stream", server.withCORS(server.handleYBCommentsStream))

	// PostgreSQL endpoints
	mux.HandleFunc("/pg/users", server.withCORS(server.handlePGUsers))
	mux.HandleFunc("/pg/galleries", server.withCORS(server.handlePGGalleries))
	mux.HandleFunc("/pg/comments", server.withCORS(server.handlePGComments))
	mux.HandleFunc("/pg/comments/stream", server.withCORS(server.handlePGCommentsStream))

	// Create HTTP server with higher concurrency limits
	httpServer := &http.Server{
		Addr:         ":9090",
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
		// MaxHeaderBytes defaults to 1MB which is fine
	}

	log.Println("API server starting on :9090")
	log.Fatal(httpServer.ListenAndServe())
}

func (s *Server) withCORS(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		handler(w, r)
	}
}

func (s *Server) initSchema(ctx context.Context, pool *pgxpool.Pool, name string) error {
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

		CREATE INDEX IF NOT EXISTS idx_galleries_user ON galleries(user_id);
		CREATE INDEX IF NOT EXISTS idx_comments_gallery ON comments(gallery_id);
		CREATE INDEX IF NOT EXISTS idx_comments_created ON comments(created_at);
	`

	_, err := pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to create schema on %s: %w", name, err)
	}

	log.Printf("Initialized database schema on %s", name)
	return nil
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"status": "ok",
		"crdb":   map[string]interface{}{"connected": false, "nodes": 0},
		"yb":     map[string]interface{}{"connected": false, "nodes": 0},
		"pg":     map[string]interface{}{"connected": false},
	}

	if s.crdbCluster != nil && len(s.crdbCluster.pools) > 0 {
		status["crdb"] = map[string]interface{}{
			"connected": true,
			"nodes":     len(s.crdbCluster.pools),
			"pool_size": len(s.crdbCluster.pools) * poolSizePerNode,
		}
	}
	if s.ybCluster != nil && len(s.ybCluster.pools) > 0 {
		status["yb"] = map[string]interface{}{
			"connected": true,
			"nodes":     len(s.ybCluster.pools),
			"pool_size": len(s.ybCluster.pools) * poolSizePerNode,
		}
	}
	if s.pgPool != nil {
		status["pg"] = map[string]interface{}{
			"connected": true,
			"pool_size": 300,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// CockroachDB handlers
func (s *Server) handleCRDBUsers(w http.ResponseWriter, r *http.Request) {
	if s.crdbCluster == nil {
		http.Error(w, "CockroachDB not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleUsers(w, r, s.crdbCluster.GetPool())
}

func (s *Server) handleCRDBGalleries(w http.ResponseWriter, r *http.Request) {
	if s.crdbCluster == nil {
		http.Error(w, "CockroachDB not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleGalleries(w, r, s.crdbCluster.GetPool())
}

func (s *Server) handleCRDBComments(w http.ResponseWriter, r *http.Request) {
	if s.crdbCluster == nil {
		http.Error(w, "CockroachDB not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleComments(w, r, s.crdbCluster.GetPool())
}

func (s *Server) handleCRDBCommentsStream(w http.ResponseWriter, r *http.Request) {
	if s.crdbCluster == nil {
		http.Error(w, "CockroachDB not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleCommentsStream(w, r, s.crdbCluster.GetPool())
}

// YugabyteDB handlers
func (s *Server) handleYBUsers(w http.ResponseWriter, r *http.Request) {
	if s.ybCluster == nil {
		http.Error(w, "YugabyteDB not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleUsers(w, r, s.ybCluster.GetPool())
}

func (s *Server) handleYBGalleries(w http.ResponseWriter, r *http.Request) {
	if s.ybCluster == nil {
		http.Error(w, "YugabyteDB not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleGalleries(w, r, s.ybCluster.GetPool())
}

func (s *Server) handleYBComments(w http.ResponseWriter, r *http.Request) {
	if s.ybCluster == nil {
		http.Error(w, "YugabyteDB not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleComments(w, r, s.ybCluster.GetPool())
}

func (s *Server) handleYBCommentsStream(w http.ResponseWriter, r *http.Request) {
	if s.ybCluster == nil {
		http.Error(w, "YugabyteDB not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleCommentsStream(w, r, s.ybCluster.GetPool())
}

// PostgreSQL handlers
func (s *Server) handlePGUsers(w http.ResponseWriter, r *http.Request) {
	if s.pgPool == nil {
		http.Error(w, "PostgreSQL not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleUsers(w, r, s.pgPool)
}

func (s *Server) handlePGGalleries(w http.ResponseWriter, r *http.Request) {
	if s.pgPool == nil {
		http.Error(w, "PostgreSQL not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleGalleries(w, r, s.pgPool)
}

func (s *Server) handlePGComments(w http.ResponseWriter, r *http.Request) {
	if s.pgPool == nil {
		http.Error(w, "PostgreSQL not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleComments(w, r, s.pgPool)
}

func (s *Server) handlePGCommentsStream(w http.ResponseWriter, r *http.Request) {
	if s.pgPool == nil {
		http.Error(w, "PostgreSQL not connected", http.StatusServiceUnavailable)
		return
	}
	s.handleCommentsStream(w, r, s.pgPool)
}

// Generic handlers
func (s *Server) handleUsers(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if pool == nil {
		http.Error(w, "Database not connected", http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "GET":
		rows, err := pool.Query(ctx, "SELECT id, name, email, created_at FROM users ORDER BY created_at DESC")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var users []User
		for rows.Next() {
			var u User
			if err := rows.Scan(&u.ID, &u.Name, &u.Email, &u.CreatedAt); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			users = append(users, u)
		}
		json.NewEncoder(w).Encode(users)

	case "POST":
		var u User
		if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := pool.QueryRow(ctx,
			"INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, created_at",
			u.Name, u.Email,
		).Scan(&u.ID, &u.CreatedAt)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(u)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGalleries(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if pool == nil {
		http.Error(w, "Database not connected", http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "GET":
		rows, err := pool.Query(ctx, `
			SELECT g.id, g.user_id, u.name, g.title, g.description, g.created_at
			FROM galleries g
			JOIN users u ON g.user_id = u.id
			ORDER BY g.created_at DESC
		`)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var galleries []Gallery
		for rows.Next() {
			var g Gallery
			if err := rows.Scan(&g.ID, &g.UserID, &g.UserName, &g.Title, &g.Description, &g.CreatedAt); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			galleries = append(galleries, g)
		}
		json.NewEncoder(w).Encode(galleries)

	case "POST":
		var g Gallery
		if err := json.NewDecoder(r.Body).Decode(&g); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := pool.QueryRow(ctx,
			"INSERT INTO galleries (user_id, title, description) VALUES ($1, $2, $3) RETURNING id, created_at",
			g.UserID, g.Title, g.Description,
		).Scan(&g.ID, &g.CreatedAt)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(g)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleComments(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if pool == nil {
		http.Error(w, "Database not connected", http.StatusServiceUnavailable)
		return
	}

	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case "GET":
		galleryID := r.URL.Query().Get("gallery_id")
		var rows interface{ Close(); Next() bool; Scan(...interface{}) error }
		var err error

		if galleryID != "" {
			rows, err = pool.Query(ctx, `
				SELECT c.id, c.gallery_id, c.user_id, u.name, c.text, c.created_at
				FROM comments c
				JOIN users u ON c.user_id = u.id
				WHERE c.gallery_id = $1
				ORDER BY c.created_at DESC
				LIMIT 20
			`, galleryID)
		} else {
			rows, err = pool.Query(ctx, `
				SELECT c.id, c.gallery_id, c.user_id, u.name, c.text, c.created_at
				FROM comments c
				JOIN users u ON c.user_id = u.id
				ORDER BY c.created_at DESC
				LIMIT 20
			`)
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var comments []Comment
		for rows.Next() {
			var c Comment
			if err := rows.Scan(&c.ID, &c.GalleryID, &c.UserID, &c.UserName, &c.Text, &c.CreatedAt); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			comments = append(comments, c)
		}
		json.NewEncoder(w).Encode(comments)

	case "POST":
		var c Comment
		if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err := pool.QueryRow(ctx,
			"INSERT INTO comments (gallery_id, user_id, text) VALUES ($1, $2, $3) RETURNING id, created_at",
			c.GalleryID, c.UserID, c.Text,
		).Scan(&c.ID, &c.CreatedAt)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Get user name
		pool.QueryRow(ctx, "SELECT name FROM users WHERE id = $1", c.UserID).Scan(&c.UserName)

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(c)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// SSE streaming for comments (polling-based for SQL databases)
func (s *Server) handleCommentsStream(w http.ResponseWriter, r *http.Request, pool *pgxpool.Pool) {
	if pool == nil {
		http.Error(w, "Database not connected", http.StatusServiceUnavailable)
		return
	}

	galleryID := r.URL.Query().Get("gallery_id")
	if galleryID == "" {
		http.Error(w, "gallery_id required", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	lastID := int64(0)

	// Poll for new comments
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rows, err := pool.Query(ctx, `
				SELECT c.id, c.gallery_id, c.user_id, u.name, c.text, c.created_at
				FROM comments c
				JOIN users u ON c.user_id = u.id
				WHERE c.gallery_id = $1 AND c.id > $2
				ORDER BY c.id ASC
			`, galleryID, lastID)

			if err != nil {
				continue
			}

			for rows.Next() {
				var c Comment
				if err := rows.Scan(&c.ID, &c.GalleryID, &c.UserID, &c.UserName, &c.Text, &c.CreatedAt); err != nil {
					continue
				}

				lastID = c.ID
				data, _ := json.Marshal(c)
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
			rows.Close()
		}
	}
}

// Helper to get env with default
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Unused but kept for compatibility
var _ = strings.Contains
