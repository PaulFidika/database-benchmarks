#!/bin/bash

# Setup test data for benchmarking
# Creates initial users and galleries in each database

echo "Setting up test data for benchmarks..."

# Wait for services to be ready
echo "Waiting for services..."
sleep 2

# CouchDB - create test user and gallery for comments
echo ""
echo "Setting up CouchDB test data..."

# Create a test user document
curl -s -X POST "http://admin:password@localhost:5984/app_users" \
  -H "Content-Type: application/json" \
  -d '{
    "_id": "user:bench:1",
    "type": "user",
    "name": "Benchmark User",
    "email": "bench@test.com",
    "created_at": "2024-01-01T00:00:00Z"
  }' || true
echo ""

# Create a test gallery
curl -s -X POST "http://admin:password@localhost:5984/galleries" \
  -H "Content-Type: application/json" \
  -d '{
    "_id": "gallery:bench:1",
    "type": "gallery",
    "user_id": "user:bench:1",
    "title": "Benchmark Gallery",
    "description": "Gallery for benchmark testing",
    "created_at": "2024-01-01T00:00:00Z"
  }' || true
echo ""

# SQL Databases (CockroachDB and YugabyteDB via API)
echo ""
echo "Setting up CockroachDB test data..."

# Create user and capture the ID
CRDB_USER=$(curl -s -X POST "http://localhost:9090/crdb/users" \
  -H "Content-Type: application/json" \
  -d '{"name": "Benchmark User", "email": "bench@test.com"}')
echo "$CRDB_USER"

# Extract ID from response
CRDB_USER_ID=$(echo "$CRDB_USER" | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2)
echo "Created user with ID: $CRDB_USER_ID"

# Create gallery with correct user_id
if [ -n "$CRDB_USER_ID" ]; then
  curl -s -X POST "http://localhost:9090/crdb/galleries" \
    -H "Content-Type: application/json" \
    -d "{\"user_id\": $CRDB_USER_ID, \"title\": \"Benchmark Gallery\", \"description\": \"Gallery for benchmark testing\"}"
  echo ""
fi

echo ""
echo "Setting up YugabyteDB test data..."

# Create user and capture the ID
YB_USER=$(curl -s -X POST "http://localhost:9090/yb/users" \
  -H "Content-Type: application/json" \
  -d '{"name": "Benchmark User", "email": "bench@test.com"}')
echo "$YB_USER"

# Extract ID from response
YB_USER_ID=$(echo "$YB_USER" | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2)
echo "Created user with ID: $YB_USER_ID"

# Create gallery with correct user_id
if [ -n "$YB_USER_ID" ]; then
  curl -s -X POST "http://localhost:9090/yb/galleries" \
    -H "Content-Type: application/json" \
    -d "{\"user_id\": $YB_USER_ID, \"title\": \"Benchmark Gallery\", \"description\": \"Gallery for benchmark testing\"}"
  echo ""
fi

echo ""
echo "Setting up PostgreSQL test data..."

# Create user and capture the ID
PG_USER=$(curl -s -X POST "http://localhost:9090/pg/users" \
  -H "Content-Type: application/json" \
  -d '{"name": "Benchmark User", "email": "bench@test.com"}')
echo "$PG_USER"

# Extract ID from response
PG_USER_ID=$(echo "$PG_USER" | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2)
echo "Created user with ID: $PG_USER_ID"

# Create gallery with correct user_id
if [ -n "$PG_USER_ID" ]; then
  curl -s -X POST "http://localhost:9090/pg/galleries" \
    -H "Content-Type: application/json" \
    -d "{\"user_id\": $PG_USER_ID, \"title\": \"Benchmark Gallery\", \"description\": \"Gallery for benchmark testing\"}"
  echo ""
fi

echo ""
echo "Test data setup complete!"
echo ""
echo "You can now run the benchmark with:"
echo "  cd bench && go run ."
