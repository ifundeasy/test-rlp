package cockroachdb_1

import (
	"context"
	"log"
	"os"
	"time"

	"test-tls/infrastructure"
)

// default location schemas.sql relative ke root project.
const defaultSchemasFile = "cmd/cockroachdb_1/schemas.sql"

// CockroachdbCreateSchemas mengeksekusi file schemas.sql ke database Cockroachdb.
// Path bisa dioverride via env POSTGRES_SCHEMAS_FILE (reused biar simple).
func CockroachdbCreateSchemas() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, cleanup, err := infrastructure.NewCockroachFromEnv(ctx)
	if err != nil {
		log.Fatalf("[cockroachdb_1] create_schemas: connect failed: %v", err)
	}
	defer cleanup()

	path := os.Getenv("POSTGRES_SCHEMAS_FILE")
	if path == "" {
		path = defaultSchemasFile
	}

	log.Printf("[cockroachdb_1] Creating schemas using %s ...", path)

	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("[cockroachdb_1] create_schemas: read %s failed: %v", path, err)
	}

	if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
		log.Fatalf("[cockroachdb_1] create_schemas: executing schemas.sql failed: %v", err)
	}

	log.Println("[cockroachdb_1] Schemas created successfully.")
}
