package elasticsearch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	esv9 "github.com/elastic/go-elasticsearch/v9"

	"test-tls/infrastructure"
)

// ElasticsearchCreateSchemas creates the index and mappings optimized for
// fast permission lookups. We denormalize allowed user IDs directly on the
// resource document to enable single-indexed term queries.
func ElasticsearchCreateSchemas() {
	ctx := context.Background()
	es, cleanup, err := infrastructure.NewElasticsearchFromEnv(ctx)
	if err != nil {
		log.Fatalf("[elasticsearch] connect error: %v", err)
		return
	}
	defer cleanup()

	ensureResourceIndex(ctx, es)
}

func ensureResourceIndex(ctx context.Context, es *esv9.Client) {
	// Mapping notes:
	// - allowed_manage_user_id / allowed_view_user_id are arrays of integers
	//   (multi-valued numeric fields) for fast term lookups.
	// - acl is optional and modeled as nested for future auditing.
	// - dynamic is false to keep mapping stable.
	mapping := `{
			"settings": {
				"number_of_shards": 1,
				"number_of_replicas": 0
			},
			"mappings": {
				"dynamic": false,
				"properties": {
					"resource_id": {"type": "integer"},
					"org_id": {"type": "integer"},
					"allowed_manage_user_id": {"type": "integer"},
					"allowed_view_user_id": {"type": "integer"},
					"acl": {
						"type": "nested",
						"properties": {
							"subject_type": {"type": "keyword"},
							"subject_id": {"type": "integer"},
							"relation": {"type": "keyword"}
						}
					}
				}
			}
		}`

	// Create index if missing; otherwise put mapping (idempotent).
	existsCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	res, err := es.Indices.Exists([]string{IndexName}, es.Indices.Exists.WithContext(existsCtx))
	if err != nil {
		log.Fatalf("[elasticsearch] index exists check failed: %v", err)
	}
	defer safeClose(res.Body)

	if res.StatusCode == 404 {
		// Create index with settings+mappings
		createCtx, ccancel := context.WithTimeout(ctx, 60*time.Second)
		defer ccancel()
		cres, err := es.Indices.Create(IndexName,
			es.Indices.Create.WithBody(bytes.NewReader([]byte(mapping))),
			es.Indices.Create.WithContext(createCtx),
		)
		if err != nil {
			log.Fatalf("[elasticsearch] create index %q failed: %v", IndexName, err)
		}
		defer safeClose(cres.Body)
		if cres.IsError() {
			body := readBodyString(cres.Body)
			log.Fatalf("[elasticsearch] create index %q error: %s body=%s", IndexName, cres.Status(), body)
		}
		log.Printf("[elasticsearch] created index %q with mappings", IndexName)
		return
	}

	// Exists: try to update mappings to ensure fields exist (no breaking changes).
	putMapCtx, mcancel := context.WithTimeout(ctx, 60*time.Second)
	defer mcancel()
	mres, err := es.Indices.PutMapping([]string{IndexName}, bytes.NewReader([]byte(`{
			"properties": {
				"allowed_manage_user_id": {"type": "integer"},
				"allowed_view_user_id": {"type": "integer"}
			}
		}`)), es.Indices.PutMapping.WithContext(putMapCtx))
	if err != nil {
		log.Fatalf("[elasticsearch] put mapping on %q failed: %v", IndexName, err)
	}
	defer safeClose(mres.Body)
	if mres.IsError() {
		body := readBodyString(mres.Body)
		log.Printf("[elasticsearch] put mapping on %q returned: %s body=%s", IndexName, mres.Status(), body)
	} else {
		log.Printf("[elasticsearch] ensured mappings on %q", IndexName)
	}
}

func safeClose(c io.Closer) {
	_ = c.Close()
}

func readBodyString(r io.Reader) string {
	if r == nil {
		return ""
	}
	b := new(bytes.Buffer)
	if _, err := b.ReadFrom(r); err != nil {
		return fmt.Sprintf("<read error: %v>", err)
	}
	return b.String()
}
