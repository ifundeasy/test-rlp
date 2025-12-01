package elasticsearch

import (
	"context"
	"log"
	"time"

	"test-tls/infrastructure"
)

// ElasticsearchDropSchemas removes the benchmark index and all documents.
// It follows the logging style used by other loaders.
func ElasticsearchDropSchemas() {
	ctx := context.Background()
	es, cleanup, err := infrastructure.NewElasticsearchFromEnv(ctx)
	if err != nil {
		log.Fatalf("[elasticsearch] drop: create client failed: %v", err)
	}
	defer cleanup()

	start := time.Now()
	log.Printf("[elasticsearch] == Starting Elasticsearch drop schemas ==")

	// Delete the index if exists.
	delCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	res, err := es.Indices.Delete([]string{IndexName}, es.Indices.Delete.WithContext(delCtx))
	if err != nil {
		log.Printf("[elasticsearch] delete index %q failed: %v", IndexName, err)
	} else {
		if res.IsError() {
			log.Printf("[elasticsearch] delete index %q returned: %s", IndexName, res.Status())
		} else {
			log.Printf("[elasticsearch] deleted index %q", IndexName)
		}
		res.Body.Close()
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[elasticsearch] Elasticsearch drop schemas DONE: elapsed=%s", elapsed)
}
