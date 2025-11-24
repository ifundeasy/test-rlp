package authzed_crdb_1

import (
	"context"
	"log"
	"os"

	"test-tls/infrastructure"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

func AuthzedCreateSchema() {
	schemaPath := "cmd/authzed_crdb_1/schemas.zed"
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		log.Fatalf("[authzed_crdb_1] read schema file %s: %v", schemaPath, err)
	}

	client, ctx, cancel, err := infrastructure.NewAuthzedCrdbClientFromEnv(context.Background())
	if err != nil {
		log.Fatalf("[authzed_crdb_1] create authzed client: %v", err)
	}
	defer cancel()
	defer client.Close()

	log.Printf("[authzed_crdb_1] == Writing schema to SpiceDB from %s ==", schemaPath)

	resp, err := client.WriteSchema(ctx, &v1.WriteSchemaRequest{
		// WARNING: this overwrites the entire schema in SpiceDB
		Schema: string(schemaBytes),
	})
	if err != nil {
		log.Fatalf("[authzed_crdb_1] WriteSchema failed: %v", err)
	}

	log.Printf("[authzed_crdb_1] Schema written at revision: %s", resp.WrittenAt.Token)
}
