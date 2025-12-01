package mongodb

import (
	"context"
	"log"
	"time"

	"test-tls/infrastructure"
)

// MongodbDropSchemas drops collections created for benchmarks. Dropping
// collections also drops their indexes. Follows child-first semantics.
func MongodbDropSchemas() {
	ctx := context.Background()
	client, db, cleanup, err := infrastructure.NewMongoFromEnv(ctx)
	if err != nil {
		log.Fatalf("[mongodb] drop: connect failed: %v", err)
	}
	defer cleanup()
	_ = client

	start := time.Now()
	log.Printf("[mongodb] == Starting MongoDB drop schemas ==")

	// Drop order: child-like collections first for safety.
	cols := []string{
		"resources",
		"groups",
		"organizations",
		"users",
	}
	for _, c := range cols {
		dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := db.Collection(c).Drop(dctx)
		cancel()
		if err != nil {
			log.Printf("[mongodb] warning: drop collection %s failed: %v", c, err)
			continue
		}
		log.Printf("[mongodb] Dropped collection: %s", c)
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[mongodb] MongoDB drop schemas DONE: elapsed=%s", elapsed)
}
