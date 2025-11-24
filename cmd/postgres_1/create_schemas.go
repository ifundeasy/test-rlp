package postgres_1

import (
	"context"
	"log"
	"os"
	"time"

	"test-tls/infrastructure"
)

// default location schemas.sql relative ke root project.
const defaultSchemasFile = "cmd/postgres_1/schemas.sql"

// PostgresCreateSchemas mengeksekusi file schemas.sql ke database Postgres.
// Path bisa dioverride via env POSTGRES_SCHEMAS_FILE.
func PostgresCreateSchemas() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, cleanup, err := infrastructure.NewPostgresFromEnv(ctx)
	if err != nil {
		log.Fatalf("[postgres_1] create_schemas: connect failed: %v", err)
	}
	defer cleanup()

	path := os.Getenv("POSTGRES_SCHEMAS_FILE")
	if path == "" {
		path = defaultSchemasFile
	}

	log.Printf("[postgres_1] Creating schemas using %s ...", path)

	sqlBytes, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("[postgres_1] create_schemas: read %s failed: %v", path, err)
	}

	if _, err := db.ExecContext(ctx, string(sqlBytes)); err != nil {
		log.Fatalf("[postgres_1] create_schemas: executing schemas.sql failed: %v", err)
	}

	log.Println("[postgres_1] Schemas created successfully.")
}
