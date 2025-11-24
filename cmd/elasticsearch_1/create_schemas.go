package elasticsearch_1

import (
	"context"
	"log"
	"strings"
	"time"

	"test-tls/infrastructure"
)

// ElasticsearchCreateSchemas creates the Elasticsearch index and mappings
// used for the RLS benchmarks.
//
// Design goals:
//
//   - Optimize for two main patterns:
//     1) RLS check: can user U manage/view resource R?
//     2) RLS list: which resources can user U manage/view?
//   - Fully embrace Elasticsearch strengths: denormalized documents,
//     inverted index on user IDs, nested ACL edges for authzed-style graph queries.
//   - Avoid any "relational-style" normalization that would require joins.
//
// Index: rls_resources
//
//   - One document per resource.
//
//     Fields:
//
//     resource_id: long            -> resource identifier
//     org_id:      long            -> owning organization
//
//     acl: nested {
//     relation:     keyword      -> "manager" | "viewer"
//     subject_type: keyword      -> "user" | "group" | "org"
//     subject_id:   long         -> ID of that subject
//     }
//
//     allowed_user_ids_manage: long[]
//
//   - compiled closure: all users that can manage this resource
//
//     allowed_user_ids_view: long[]
//
//   - compiled closure: all users that can view this resource
//
//   - superset of allowed_user_ids_manage
//
// RLS check:
//
//   - Query by resource_id + user_id on allowed_user_ids_*.
//
// RLS list:
//
//   - Query by user_id on allowed_user_ids_view and page through results.
func ElasticsearchCreateSchemas() {
	ctx := context.Background()

	es, cleanup, err := infrastructure.NewElasticsearchFromEnv(ctx)
	if err != nil {
		log.Fatalf("[elasticsearch_1] NewElasticsearchFromEnv failed: %v", err)
	}
	defer cleanup()

	log.Printf("[elasticsearch_1] == Creating Elasticsearch index and mappings ==")

	// 1) Drop existing index if present.
	{
		ctxDrop, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		res, err := es.Indices.Delete(
			[]string{IndexName},
			es.Indices.Delete.WithContext(ctxDrop),
		)
		if err != nil {
			log.Fatalf("[elasticsearch_1] indices.delete %q failed: %v", IndexName, err)
		}
		defer res.Body.Close()

		// 404 is fine (index does not exist yet).
		if res.IsError() && !strings.Contains(res.Status(), "404") {
			log.Fatalf("[elasticsearch_1] indices.delete %q returned error: %s", IndexName, res.Status())
		}
	}

	// 2) Create index with optimized settings + mappings.
	mapping := `
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "resource_id": {
        "type": "long"
      },
      "org_id": {
        "type": "long"
      },
      "acl": {
        "type": "nested",
        "properties": {
          "relation": {
            "type": "keyword"
          },
          "subject_type": {
            "type": "keyword"
          },
          "subject_id": {
            "type": "long"
          }
        }
      },
      "allowed_user_ids_manage": {
        "type": "long"
      },
      "allowed_user_ids_view": {
        "type": "long"
      }
    }
  }
}
`

	ctxCreate, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := es.Indices.Create(
		IndexName,
		es.Indices.Create.WithBody(strings.NewReader(mapping)),
		es.Indices.Create.WithContext(ctxCreate),
	)
	if err != nil {
		log.Fatalf("[elasticsearch_1] indices.create %q failed: %v", IndexName, err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("[elasticsearch_1] indices.create %q returned error: %s", IndexName, res.Status())
	}

	log.Printf("[elasticsearch_1] Index %q created successfully.", IndexName)
}
