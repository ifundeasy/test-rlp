package authzed_pgdb

import (
	"context"
	"log"
	"os"

	"test-tls/infrastructure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

func AuthzedCreateSchema() {
	schemaPath := "cmd/authzed_pgdb/schemas.zed"
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		log.Fatalf("[authzed_pgdb] read schema file %s: %v", schemaPath, err)
	}

	client, ctx, cancel, err := infrastructure.NewAuthzedPgdbClientFromEnv(context.Background())
	if err != nil {
		log.Fatalf("[authzed_pgdb] create authzed client: %v", err)
	}
	defer cancel()
	defer client.Close()

	log.Printf("[authzed_pgdb] == Writing schema to SpiceDB from %s ==", schemaPath)

	resp, err := client.WriteSchema(ctx, &v1.WriteSchemaRequest{
		// WARNING: this overwrites the entire schema in SpiceDB
		Schema: string(schemaBytes),
	})
	if err != nil {
		log.Fatalf("[authzed_pgdb] WriteSchema failed: %v", err)
	}

	log.Printf("[authzed_pgdb] Schema written at revision: %s", resp.WrittenAt.Token)
}
