package mongodb_1

import (
	"context"
	"errors"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"

	"test-tls/infrastructure"
)

// MongodbDropSchemas drops all collections used by the mongodb_1 benchmarks.
// It does NOT drop the database itself, only the collections we created.
func MongodbDropSchemas() {
	ctx := context.Background()

	_, db, cleanup, err := infrastructure.NewMongoFromEnv(ctx)
	if err != nil {
		log.Fatalf("[mongodb_1] failed to create mongo client: %v", err)
	}
	defer cleanup()

	log.Printf("[mongodb_1] == Dropping MongoDB collections for benchmarks ==")

	opTimeout := 30 * time.Second

	// Keep this list in sync with:
	//   - MongodbCreateSchemas (explicit collection creation)
	//   - clearCollections in load_data.go
	collections := []string{
		"organizations",
		"users",
		"groups",
		"org_memberships",
		"group_memberships",
		"resources",
		"resource_acl",
		"user_resource_perms",
	}

	for _, name := range collections {
		coll := db.Collection(name)

		ctxDrop, cancel := context.WithTimeout(ctx, opTimeout)
		err := coll.Drop(ctxDrop)
		cancel()

		if err != nil {
			// When a collection does not exist, MongoDB returns a command error
			// with code 26 (NamespaceNotFound). Treat that as a successful no-op
			// so the command is idempotent, like "DROP TABLE IF EXISTS".
			var cmdErr mongo.CommandError
			if errors.As(err, &cmdErr) && cmdErr.Code == 26 {
				log.Printf("[mongodb_1] DropCollection %s: namespace not found (already dropped)", name)
				continue
			}

			log.Fatalf("[mongodb_1] DropCollection %s failed: %v", name, err)
		}

		log.Printf("[mongodb_1] Dropped collection: %s", name)
	}

	log.Printf("[mongodb_1] MongoDB collections drop DONE")
}
