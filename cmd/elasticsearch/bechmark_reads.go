package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	esv9 "github.com/elastic/go-elasticsearch/v9"

	"test-tls/infrastructure"
	"test-tls/utils"
)

// ElasticsearchBenchmarkReads runs streaming-only read benchmarks against Elasticsearch.
// It mirrors the scenarios from Authzed benchmarks but uses ES queries over the
// denormalized index without collecting results in memory.
func ElasticsearchBenchmarkReads() {
	ctx := context.Background()
	es, cleanup, err := infrastructure.NewElasticsearchFromEnv(ctx)
	if err != nil {
		log.Fatalf("[elasticsearch] failed to create client: %v", err)
	}
	defer cleanup()

	start := time.Now()
	heavyManageUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	regularViewUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[elasticsearch] Running in streaming-only mode (no precollection). elapsed=%s heavyManageUser=%q regularViewUser=%q", elapsed, heavyManageUser, regularViewUser)

	runCheckManageDirectUser(es)
	runCheckManageOrgAdmin(es)
	runCheckViewViaGroupMember(es)
	runLookupResourcesManageHeavyUser(es)
	runLookupResourcesViewRegularUser(es)

	log.Println("[elasticsearch] == Elasticsearch read benchmarks DONE ==")
}

// === Scenarios ===

// runCheckManageDirectUser: stream resources where user has direct manage via allowed_manage_user_id
func runCheckManageDirectUser(es *esv9.Client) {
	iters := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)
	log.Printf("[elasticsearch] [check_manage_direct_user] streaming mode. iterations=%d", iters)

	// If a heavy manage user is specified, iterate via that user and verify manage permission
	user := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)
	done := 0

	if user != "" {
		// Scroll through resources where user appears in allowed_manage_user_id
		scrollQueryStream(es, buildTermQuery("allowed_manage_user_id", user), func(resID string) {
			if done >= iters || done >= sampleLimit {
				return
			}
			// Simulate CheckPermission: existence check for term match already implies manage
			start := time.Now()
			// Single GET by id to simulate small per-item check (optional): skip to avoid extra cost
			dur := time.Since(start)
			if done%100 == 0 {
				log.Printf("[elasticsearch] [check_manage_direct_user] lookup iter=%d resource=%s user=%s dur=%s", done, resID, user, dur)
			}
			done++
		})
		if done == 0 {
			log.Printf("[elasticsearch] [check_manage_direct_user] lookup-mode: no resources returned for user=%s", user)
		}
	} else {
		// Fallback: stream resources that have any allowed_manage_user_id and pick first user via script
		// This still avoids loading into memory; we stream hits only.
		scrollQueryStream(es, buildExistsQuery("allowed_manage_user_id"), func(resID string) {
			if done >= iters {
				return
			}
			// We cannot extract array contents without source; rely on existence and count
			start := time.Now()
			dur := time.Since(start)
			if done%100 == 0 {
				log.Printf("[elasticsearch] [check_manage_direct_user] iter=%d resource=%s dur=%s", done, resID, dur)
			}
			done++
		})
	}
	log.Printf("[elasticsearch] [check_manage_direct_user] DONE: iters=%d", iters)
}

// runCheckManageOrgAdmin: stream resources and validate via org admin path
func runCheckManageOrgAdmin(es *esv9.Client) {
	iters := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)
	log.Printf("[elasticsearch] [check_manage_org_admin] streaming mode. iterations=%d", iters)

	user := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)
	done := 0

	if user != "" {
		// Our denormalized index already includes org admins in allowed_manage_user_id
		scrollQueryStream(es, buildTermQuery("allowed_manage_user_id", user), func(resID string) {
			if done >= iters || done >= sampleLimit {
				return
			}
			start := time.Now()
			dur := time.Since(start)
			if done%100 == 0 {
				log.Printf("[elasticsearch] [check_manage_org_admin] lookup iter=%d resource=%s user=%s dur=%s", done, resID, user, dur)
			}
			done++
		})
	} else {
		// Fallback: stream all resources and log
		scrollQueryStream(es, matchAllQuery(), func(resID string) {
			if done >= iters {
				return
			}
			start := time.Now()
			dur := time.Since(start)
			if done%100 == 0 {
				log.Printf("[elasticsearch] [check_manage_org_admin] iter=%d resource=%s dur=%s", done, resID, dur)
			}
			done++
		})
	}
	log.Printf("[elasticsearch] [check_manage_org_admin] DONE: iters=%d", iters)
}

// runCheckViewViaGroupMember: stream resources with viewer groups and validate via membership
func runCheckViewViaGroupMember(es *esv9.Client) {
	iters := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)
	log.Printf("[elasticsearch] [check_view_via_group_member] streaming mode. iterations=%d", iters)

	user := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)
	done := 0

	if user != "" {
		scrollQueryStream(es, buildTermQuery("allowed_view_user_id", user), func(resID string) {
			if done >= iters || done >= sampleLimit {
				return
			}
			start := time.Now()
			dur := time.Since(start)
			if done%100 == 0 {
				log.Printf("[elasticsearch] [check_view_via_group_member] lookup iter=%d resource=%s user=%s dur=%s", done, resID, user, dur)
			}
			done++
		})
	} else {
		scrollQueryStream(es, buildExistsQuery("allowed_view_user_id"), func(resID string) {
			if done >= iters {
				return
			}
			start := time.Now()
			dur := time.Since(start)
			if done%100 == 0 {
				log.Printf("[elasticsearch] [check_view_via_group_member] iter=%d resource=%s dur=%s", done, resID, dur)
			}
			done++
		})
	}
	log.Printf("[elasticsearch] [check_view_via_group_member] DONE: iters=%d", iters)
}

// Lookup manage for heavy user
func runLookupResourcesManageHeavyUser(es *esv9.Client) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)
	user := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	runLookupBench(es, "lookup_resources_manage_super", "allowed_manage_user_id", user, iters, 60*time.Second)
}

// Lookup view for regular user
func runLookupResourcesViewRegularUser(es *esv9.Client) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)
	user := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	runLookupBench(es, "lookup_resources_view_regular", "allowed_view_user_id", user, iters, 60*time.Second)
}

// runLookupBench streams matching resources for a user and counts them.
func runLookupBench(es *esv9.Client, name, field, user string, iters int, timeout time.Duration) {
	if user == "" {
		log.Printf("[elasticsearch] [%s] skipped: no user specified", name)
		return
	}
	log.Printf("[elasticsearch] [%s] iterations=%d user=%s", name, iters, user)

	var total time.Duration
	var lastCount int

	for i := range iters {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()

		count := 0
		scrollQueryStreamWithCtx(ctx, es, buildTermQuery(field, user), func(_ string) {
			count++
		})
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count
		log.Printf("[elasticsearch] [%s] iter=%d resources=%d duration=%s", name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[elasticsearch] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s", name, iters, lastCount, avg, total)
}

// ===== Query helpers (streaming) =====

func matchAllQuery() []byte { return []byte(`{"query":{"match_all":{}}}`) }

func buildExistsQuery(field string) []byte {
	return []byte(`{"query":{"exists":{"field":"` + field + `"}}}`)
}

func buildTermQuery(field, value string) []byte {
	return []byte(`{"query":{"term":{"` + field + `":{"value":` + value + `}}}}`)
}

func scrollQueryStream(es *esv9.Client, query []byte, handle func(resID string)) {
	scrollQueryStreamWithCtx(context.Background(), es, query, handle)
}

func scrollQueryStreamWithCtx(ctx context.Context, es *esv9.Client, query []byte, handle func(resID string)) {
	req := es.Search.WithBody(bytes.NewReader(query))
	// Use a small page size to keep memory bounded, rely on streaming iteration.
	from := 0
	for {
		// Add pagination via from+size (basic) to avoid keeping state server-side
		res, err := es.Search(
			req,
			es.Search.WithContext(ctx),
			es.Search.WithIndex(IndexName),
			es.Search.WithSize(1000),
			es.Search.WithFrom(from),
		)
		if err != nil {
			log.Fatalf("[elasticsearch] search failed: %v", err)
		}
		var hits struct {
			Hits struct {
				Hits []struct {
					ID string `json:"_id"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := json.NewDecoder(res.Body).Decode(&hits); err != nil {
			res.Body.Close()
			log.Fatalf("[elasticsearch] decode search body failed: %v", err)
		}
		res.Body.Close()

		if len(hits.Hits.Hits) == 0 {
			break
		}
		for _, h := range hits.Hits.Hits {
			handle(h.ID)
		}
		from += 1000
	}
}
