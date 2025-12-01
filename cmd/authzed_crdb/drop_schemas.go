package authzed_crdb

import (
	"context"
	"log"
	"regexp"
	"strings"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	authzed "github.com/authzed/authzed-go/v1"

	"test-tls/infrastructure"
)

// AuthzedDropSchemas deletes ALL relationship data for the resource types we care about.
// It does NOT drop the schema itself, so you can recreate/recreate data afterwards.
func AuthzedDropSchemas() {
	start := time.Now()
	client, _, cancel, err := infrastructure.NewAuthzedCrdbClientFromEnv(context.Background())
	if err != nil {
		log.Fatalf("[authzed_crdb] failed to create authzed client: %v", err)
	}
	defer cancel()
	defer client.Close()

	log.Printf("[authzed_crdb] == Dropping relationships for known resource types ==")

	// Read the current schema and attempt to delete relationships for every
	// resource type defined there. If schema reading or parsing fails, fall
	// back to a conservative default set to ensure cleanup.
	schemaText := readCurrentSchema(client)

	// regex to capture "definition <name> {"
	defRe := regexp.MustCompile(`definition\s+([a-zA-Z0-9_]+)\s*{`)
	matches := defRe.FindAllStringSubmatch(schemaText, -1)

	var resourceTypes []string
	seen := make(map[string]struct{})
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		name := strings.TrimSpace(m[1])
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		resourceTypes = append(resourceTypes, name)
	}

	// If we couldn't parse any definitions, fall back to the common types.
	if len(resourceTypes) == 0 {
		resourceTypes = []string{"resource", "organization", "usergroup", "user"}
	}

	for _, rt := range resourceTypes {
		dropRelationshipsForType(client, rt)
	}

	// Aggressive deletion: remove all relationships unconditionally to ensure
	// no stale tuples remain in the backend (use on test/dev instances).
	log.Printf("[authzed_crdb] Aggressive: deleting ALL relationships (unconditional)")
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	if _, err := client.DeleteRelationships(ctx, &v1.DeleteRelationshipsRequest{RelationshipFilter: &v1.RelationshipFilter{}}); err != nil {
		log.Printf("[authzed_crdb] Aggressive DeleteRelationships failed: %v", err)
	} else {
		log.Printf("[authzed_crdb] Aggressive: DeleteRelationships succeeded (all relationships deleted)")
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[authzed_crdb] DONE: delete attempt finished for resource, organization, usergroup. elapsed=%s", elapsed)
}

// readCurrentSchema reads the current schema text from SpiceDB.
func readCurrentSchema(client *authzed.Client) string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ReadSchema(ctx, &v1.ReadSchemaRequest{})
	if err != nil {
		log.Fatalf("[authzed_crdb] ReadSchema failed: %v", err)
	}

	return resp.SchemaText
}

// dropRelationshipsForType calls DeleteRelationships for a single resource_type.
// This deletes ALL relationships for that resource type.
func dropRelationshipsForType(client *authzed.Client, resourceType string) {
	log.Printf("[authzed_crdb] Deleting all relationships with resource_type=%q ...", resourceType)

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
		log.Printf("[authzed_crdb] DeleteRelationships for %s failed: %v", resourceType, err)
		return
	}

	log.Printf("[authzed_crdb] Deleted all relationships for resource_type=%q", resourceType)
}
