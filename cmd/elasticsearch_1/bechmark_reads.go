package elasticsearch_1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"test-tls/infrastructure"

	elasticsearch "github.com/elastic/go-elasticsearch/v9"
)

// benchPair couples a resource with a user that should have manage permission.
type benchPair struct {
	UserID     int
	ResourceID int
}

// benchDataset holds precomputed samples and heavy users for the benchmarks.
type benchDataset struct {
	directManagerPairs []benchPair

	heavyManageUser  string
	regularViewUser  string
	heavyManageCount int
	regularViewCount int
}

// esHitSource represents the subset of fields we need from Elasticsearch.
type esHitSource struct {
	ResourceID           int   `json:"resource_id"`
	AllowedUserIDsManage []int `json:"allowed_user_ids_manage"`
	AllowedUserIDsView   []int `json:"allowed_user_ids_view"`
}

type esSearchPage struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []struct {
			Source esHitSource `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// esSearchResponse is a minimal response shape for counting hits.
type esSearchResponse struct {
	Hits struct {
		Hits []json.RawMessage `json:"hits"`
	} `json:"hits"`
}

// ElasticsearchBenchmarkReads is the entry point, invoked via:
//
//	go run cmd/main.go elasticsearch_1 benchmark
func ElasticsearchBenchmarkReads() {
	ctx := context.Background()

	es, cleanup, err := infrastructure.NewElasticsearchFromEnv(ctx)
	if err != nil {
		log.Fatalf("[elasticsearch_1] NewElasticsearchFromEnv failed: %v", err)
	}
	defer cleanup()

	log.Printf("[elasticsearch_1] == Building benchmark dataset from Elasticsearch ==")
	data, err := buildBenchDatasetFromElasticsearch(ctx, es)
	if err != nil {
		log.Fatalf("[elasticsearch_1] build benchmark dataset: %v", err)
	}

	log.Printf("[elasticsearch_1] == Running Elasticsearch read benchmarks on index-backed dataset ==")

	runCheckManageDirectUser(ctx, es, data)
	runListViewHeavyUser(ctx, es, data)
	runListViewRegularUser(ctx, es, data)

	log.Println("[elasticsearch_1] == Elasticsearch read benchmarks DONE ==")
}

// buildBenchDatasetFromElasticsearch scans the rlp_resources index and builds
// a benchmark dataset from the compiled permission closure stored in ES:
//
//	directManagerPairs: all (user, resource) where user can manage resource
//	heavyManageUser:    user with most manage resources (or env override)
//	regularViewUser:    user with many view resources (or env override)
func buildBenchDatasetFromElasticsearch(ctx context.Context, es *elasticsearch.Client) (*benchDataset, error) {
	start := time.Now()

	managePairs := make([]benchPair, 0, 64_000)
	manageCount := make(map[int]int)
	viewCount := make(map[int]int)

	// Scroll entire index to build dataset.
	queryBody := map[string]interface{}{
		"query": map[string]interface{}{"match_all": map[string]interface{}{}},
		"_source": []string{
			"resource_id",
			"allowed_user_ids_manage",
			"allowed_user_ids_view",
		},
		"size": envInt("BENCH_ES_SCROLL_BATCH", 1000),
	}
	payload, err := json.Marshal(queryBody)
	if err != nil {
		return nil, err
	}

	res, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(IndexName),
		es.Search.WithBody(bytes.NewReader(payload)),
		es.Search.WithScroll(time.Minute),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("initial search on index %q returned error: %s", IndexName, res.Status())
	}

	var page esSearchPage
	if err := json.NewDecoder(res.Body).Decode(&page); err != nil {
		return nil, err
	}

	scrollID := page.ScrollID
	defer func() {
		if scrollID != "" {
			_, _ = es.ClearScroll(es.ClearScroll.WithScrollID(scrollID))
		}
	}()

	processPage := func(p esSearchPage) {
		for _, h := range p.Hits.Hits {
			src := h.Source
			resID := src.ResourceID

			for _, uid := range src.AllowedUserIDsManage {
				managePairs = append(managePairs, benchPair{UserID: uid, ResourceID: resID})
				manageCount[uid]++
			}
			for _, uid := range src.AllowedUserIDsView {
				viewCount[uid]++
			}
		}
	}

	processPage(page)

	for {
		if len(page.Hits.Hits) == 0 || scrollID == "" {
			break
		}

		scrollRes, err := es.Scroll(
			es.Scroll.WithContext(ctx),
			es.Scroll.WithScrollID(scrollID),
			es.Scroll.WithScroll(time.Minute),
		)
		if err != nil {
			return nil, err
		}
		if scrollRes.IsError() {
			scrollRes.Body.Close()
			return nil, fmt.Errorf("scroll on index %q returned error: %s", IndexName, scrollRes.Status())
		}

		var nextPage esSearchPage
		if err := json.NewDecoder(scrollRes.Body).Decode(&nextPage); err != nil {
			scrollRes.Body.Close()
			return nil, err
		}
		scrollRes.Body.Close()

		scrollID = nextPage.ScrollID
		page = nextPage

		if len(page.Hits.Hits) == 0 {
			break
		}
		processPage(page)
	}

	if len(managePairs) == 0 {
		return nil, fmt.Errorf("no manage pairs found in index %q; did you run elasticsearch_1 load_data?", IndexName)
	}

	// Compute heavy/regular users.
	var heavyUserID, heavyCount int
	for uid, c := range manageCount {
		if c > heavyCount {
			heavyCount = c
			heavyUserID = uid
		}
	}

	var regularUserID, regularCount int
	for uid, c := range viewCount {
		if uid == heavyUserID || c == 0 {
			continue
		}
		if regularUserID == 0 || c > regularCount {
			regularUserID = uid
			regularCount = c
		}
	}
	if regularUserID == 0 {
		regularUserID = heavyUserID
		regularCount = viewCount[heavyUserID]
	}

	heavy := strconv.Itoa(heavyUserID)
	regular := strconv.Itoa(regularUserID)

	// Env overrides for consistency with other backends.
	if v := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER"); v != "" {
		heavy = v
	}
	if v := os.Getenv("BENCH_LOOKUPRES_VIEW_USER"); v != "" {
		regular = v
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[elasticsearch_1] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
		elapsed, len(managePairs), 0, 0, heavy, regular)

	return &benchDataset{
		directManagerPairs: managePairs,
		heavyManageUser:    heavy,
		regularViewUser:    regular,
		heavyManageCount:   heavyCount,
		regularViewCount:   regularCount,
	}, nil
}

// ===============================
// Bench 1: Check "manage" via compiled allowed_user_ids_manage
// ===============================

func runCheckManageDirectUser(parent context.Context, es *elasticsearch.Client, data *benchDataset) {
	iters := envInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)
	pairs := data.directManagerPairs
	if len(pairs) == 0 {
		log.Printf("[elasticsearch_1] [check_manage_direct_user] skipped: no manage pairs in dataset")
		return
	}

	name := "check_manage_direct_user"
	log.Printf("[elasticsearch_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	start := time.Now()
	allowed := 0

	for i := 0; i < iters; i++ {
		p := pairs[i%len(pairs)]

		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		ok, err := checkManage(ctx, es, p.UserID, p.ResourceID)
		cancel()
		if err != nil {
			log.Fatalf("[elasticsearch_1] [%s] query failed: %v", name, err)
		}
		if ok {
			allowed++
		}
	}

	total := time.Since(start)
	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[elasticsearch_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s", name, iters, allowed, avg, total)
}

func checkManage(ctx context.Context, es *elasticsearch.Client, userID, resourceID int) (bool, error) {
	body := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"filter": []interface{}{
					map[string]interface{}{"term": map[string]interface{}{"resource_id": resourceID}},
					map[string]interface{}{"term": map[string]interface{}{"allowed_user_ids_manage": userID}},
				},
			},
		},
		"size": 1,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return false, err
	}

	res, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(IndexName),
		es.Search.WithBody(bytes.NewReader(payload)),
		es.Search.WithTrackTotalHits(false),
		es.Search.WithRequestCache(true),
		es.Search.WithFilterPath("hits.hits._id"),
	)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return false, nil
	}

	var sr esSearchResponse
	if err := json.NewDecoder(res.Body).Decode(&sr); err != nil {
		return false, err
	}

	return len(sr.Hits.Hits) > 0, nil
}

// ===============================
// Bench 2: Lookup "view" for heavy and regular users
// ===============================

// Default keeps ES fast (100). For strict Postgres parity set BENCH_ES_LIST_LIMIT=2000.
func listLimit() int { return envInt("BENCH_ES_LIST_LIMIT", 100) }

func runListViewHeavyUser(ctx context.Context, es *elasticsearch.Client, data *benchDataset) {
	iters := envInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)

	if data.heavyManageUser == "" {
		log.Printf("[elasticsearch_1] [lookup_resources_manage_super] skipped: heavyManageUser is empty")
		return
	}

	userStr := data.heavyManageUser
	userID, err := strconv.Atoi(userStr)
	if err != nil {
		log.Fatalf("[elasticsearch_1] [lookup_resources_manage_super] invalid heavyManageUser=%q: %v", userStr, err)
	}

	// Warm-up to populate ES request cache and FS cache
	_, _ = listViewResources(ctx, es, userID)

	runListViewUser(ctx, es, "lookup_resources_manage_super", iters, userStr, userID)
}

func runListViewRegularUser(ctx context.Context, es *elasticsearch.Client, data *benchDataset) {
	iters := envInt("BENCH_LOOKUPRES_VIEW_ITER", 10)

	if data.regularViewUser == "" {
		log.Printf("[elasticsearch_1] [lookup_resources_view_regular] skipped: regularViewUser is empty")
		return
	}

	userStr := data.regularViewUser
	userID, err := strconv.Atoi(userStr)
	if err != nil {
		log.Fatalf("[elasticsearch_1] [lookup_resources_view_regular] invalid regularViewUser=%q: %v", userStr, err)
	}

	// Warm-up
	_, _ = listViewResources(ctx, es, userID)

	runListViewUser(ctx, es, "lookup_resources_view_regular", iters, userStr, userID)
}

func runListViewUser(parent context.Context, es *elasticsearch.Client, name string, iters int, userStr string, userID int) {
	log.Printf("[elasticsearch_1] [%s] iterations=%d user=%s", name, iters, userStr)

	var total time.Duration
	lastCount := 0

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		start := time.Now()

		n, err := listViewResources(ctx, es, userID)
		cancel()
		if err != nil {
			log.Fatalf("[elasticsearch_1] [%s] query failed: %v", name, err)
		}

		dur := time.Since(start)
		total += dur
		lastCount = n

		log.Printf("[elasticsearch_1] [%s] iter=%d resources=%d duration=%s", name, i, n, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[elasticsearch_1] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s", name, iters, lastCount, avg, total)
}

func listViewResources(ctx context.Context, es *elasticsearch.Client, userID int) (int, error) {
	body := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"allowed_user_ids_view": userID,
			},
		},
		"size":    listLimit(),
		"_source": false,
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}

	res, err := es.Search(
		es.Search.WithContext(ctx),
		es.Search.WithIndex(IndexName),
		es.Search.WithBody(bytes.NewReader(payload)),
		es.Search.WithTrackTotalHits(false),
		es.Search.WithRequestCache(true),
		es.Search.WithFilterPath("hits.hits._id"),
	)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	if res.IsError() {
		return 0, nil
	}

	var sr esSearchResponse
	if err := json.NewDecoder(res.Body).Decode(&sr); err != nil {
		return 0, err
	}

	return len(sr.Hits.Hits), nil
}

// envInt reads an int from env, falling back to default if unset or invalid.
func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("[elasticsearch_1] invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}
