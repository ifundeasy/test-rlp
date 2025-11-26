package cockroachdb

import (
	"context"
	"log"
	"os"
	"time"

	"test-tls/infrastructure"
)

// default location schemas.sql relative ke root project.
const defaultSchemasFile = "cmd/cockroachdb/schemas.sql"

// CockroachdbCreateSchemas mengeksekusi file schemas.sql ke database Cockroachdb.
// Path bisa dioverride via env COCKROACHDB_SCHEMAS_FILE.
func CockroachdbCreateSchemas() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, cleanup, err := infrastructure.NewCockroachDBFromEnv(ctx)
	if err != nil {
		log.Fatalf("[cockroachdb] create_schemas: connect failed: %v", err)
	}
	defer cleanup()

	path := os.Getenv("COCKROACHDB_SCHEMAS_FILE")
	if path == "" {
		path = defaultSchemasFile
	}

	log.Printf("[cockroachdb] Creating schemas using %s ...", path)

	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("[cockroachdb] create_schemas: read %s failed: %v", path, err)
	}

	if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
		log.Fatalf("[cockroachdb] create_schemas: executing schemas.sql failed: %v", err)
	}

	log.Println("[cockroachdb] Schemas created successfully.")
}
