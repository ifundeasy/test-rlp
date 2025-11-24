package authzed_crdb_1

import (
	"context"
	"log"
	"strings"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	authzed "github.com/authzed/authzed-go/v1"

	"test-tls/infrastructure"
)

// AuthzedDropSchemas deletes ALL relationship data for the resource types we care about.
// It does NOT drop the schema itself, so you can recreate/recreate data afterwards.
func AuthzedDropSchemas() {
	client, _, cancel, err := infrastructure.NewAuthzedCrdbClientFromEnv(context.Background())
	if err != nil {
		log.Fatalf("[authzed_crdb_1] failed to create authzed client: %v", err)
	}
	defer cancel()
	defer client.Close()

	log.Printf("[authzed_crdb_1] == Dropping relationships for known resource types ==")

	// Read the current schema once
	schemaText := readCurrentSchema(client)

	// Resource types we expect to exist in the schema
	resourceTypes := []string{
		"resource",
		"organization",
		"usergroup",
	}

	for _, rt := range resourceTypes {
		if !schemaHasDefinition(schemaText, rt) {
			log.Printf("[authzed_crdb_1] SKIP: no `definition %s` in current schema, nothing to delete", rt)
			continue
		}
		dropRelationshipsForType(client, rt)
	}

	log.Println("[authzed_crdb_1] DONE: delete attempt finished for resource, organization, usergroup (only existing types were processed)")
}

// readCurrentSchema reads the current schema text from SpiceDB.
func readCurrentSchema(client *authzed.Client) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
	if err != nil {
		log.Fatalf("[authzed_crdb_1] ReadSchema failed: %v", err)
	}

	return resp.SchemaText
}

// schemaHasDefinition checks if there is a `definition <typeName>` in the schema text.
func schemaHasDefinition(schemaText, typeName string) bool {
	// Check a few common formatting variants
	needle1 := "definition " + typeName + " {"
	needle2 := "definition " + typeName + "{"
	needle3 := "\ndefinition " + typeName + " "
	return strings.Contains(schemaText, needle1) ||
		strings.Contains(schemaText, needle2) ||
		strings.Contains(schemaText, needle3) ||
		strings.HasPrefix(schemaText, "definition "+typeName+" ")
}

// dropRelationshipsForType calls DeleteRelationships for a single resource_type.
// This deletes ALL relationships for that resource type.
func dropRelationshipsForType(client *authzed.Client, resourceType string) {
	log.Printf("[authzed_crdb_1] Deleting all relationships with resource_type=%q ...", resourceType)

	// Use a generous timeout per request; deleting many relationships can take time.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	_, err := client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{
		RelationshipFilter: &v1.RelationshipFilter{
			ResourceType: resourceType,
		},
	})
	if err != nil {
		// Do not kill the whole process: log the error and continue with other types
		log.Printf("[authzed_crdb_1] DeleteRelationships for %s failed: %v", resourceType, err)
		return
	}

	log.Printf("[authzed_crdb_1] Deleted all relationships for resource_type=%q", resourceType)
}
