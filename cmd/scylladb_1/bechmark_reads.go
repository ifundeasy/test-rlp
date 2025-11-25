package scylladb_1

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/gocql/gocql"

	"test-tls/infrastructure"
)

type benchPair struct {
	userID     int
	resourceID int
}

type benchDataset struct {
	directManagerPairs []benchPair
	orgAdminPairs      []benchPair // reserved for future, kept for symmetry with other backends
	groupViewPairs     []benchPair // reserved for future, ditto

	heavyManageUser  string // user with the most manage edges (for heavy list)
	regularViewUser  string // lighter user with view edges (for regular list)
	heavyManageCount int
	regularViewCount int
}

// ScylladbBenchmarkReads runs read benchmarks for ScyllaDB using the
// compiled permission tables:
//
//   - user_resource_perms_by_resource: optimized for RLS check
//   - user_resource_perms_by_user: optimized for RLS list
//
// It follows the same logging style as mongodb_1/postgres_1 so you can
// compare latency profiles.
func ScylladbBenchmarkReads() {
	ctx := context.Background()

	session, cleanup, err := infrastructure.NewScyllaFromEnv(ctx)
	if err != nil {
		log.Fatalf("[scylladb_1] NewScyllaFromEnv failed: %v", err)
	}
	defer cleanup()

	data := buildBenchDataset(ctx, session)

	log.Printf("[scylladb_1] == Running ScyllaDB read benchmarks on DB-backed dataset ==")

	runCheckManageDirectUser(ctx, session, data)
	runListViewHeavyUser(ctx, session, data)
	runListViewRegularUser(ctx, session, data)
}

// ==========================================================
// Dataset builder (from compiled perms tables)
// ==========================================================

func buildBenchDataset(ctx context.Context, session *gocql.Session) *benchDataset {
	log.Printf("[scylladb_1] == Building benchmark dataset from ScyllaDB ==")
	start := time.Now()

	managePairs := make([]benchPair, 0, 64_000)

	manageCount := make(map[int]int)
	viewCount := make(map[int]int)

	// We use the compiled closure here so we don't re-implement the graph logic.
	// This is effectively "all known user-resource permissions", with flags.
	iter := session.
		Query("SELECT resource_id, user_id, can_manage, can_view FROM user_resource_perms_by_resource").
		WithContext(ctx).
		Iter()

	var (
		resID, userID      int
		canManage, canView bool
	)
	rows := 0

	for iter.Scan(&resID, &userID, &canManage, &canView) {
		rows++

		if canManage {
			managePairs = append(managePairs, benchPair{
				userID:     userID,
				resourceID: resID,
			})
			manageCount[userID]++
		}
		if canView {
			viewCount[userID]++
		}
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("[scylladb_1] buildBenchDataset: scan failed: %v", err)
	}

	if rows == 0 {
		log.Fatalf("[scylladb_1] buildBenchDataset: user_resource_perms_by_resource is empty, run ScylladbCreateData first")
	}

	// Heavy manage user = highest can_manage count
	var heavyUserID, heavyCount int
	for u, c := range manageCount {
		if c > heavyCount {
			heavyCount = c
			heavyUserID = u
		}
	}

	// "Regular" view user = a lighter (but non-zero) viewer
	var regularUserID, regularCount int
	for u, c := range viewCount {
		if u == heavyUserID || c == 0 {
			continue
		}
		if regularUserID == 0 || c < regularCount {
			regularUserID = u
			regularCount = c
		}
	}

	if heavyUserID == 0 {
		// Fallback: if no manage edges, pick any viewer
		for u, c := range viewCount {
			if c == 0 {
				continue
			}
			heavyUserID = u
			heavyCount = c
			break
		}
	}
	if regularUserID == 0 {
		// Fallback: if distribution is weird, reuse heavy user as regular
		regularUserID = heavyUserID
		regularCount = viewCount[heavyUserID]
	}

	data := &benchDataset{
		directManagerPairs: managePairs,
		orgAdminPairs:      nil,
		groupViewPairs:     nil,
		heavyManageUser:    strconv.Itoa(heavyUserID),
		regularViewUser:    strconv.Itoa(regularUserID),
		heavyManageCount:   heavyCount,
		regularViewCount:   regularCount,
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	// For comparability with other backends, report the same set of
	// benchmark dataset fields. Scylla's builder doesn't currently
	// compute `orgAdminPairs` or `groupViewPairs`, so report 0 for them.
	orgAdminPairs := 0
	groupViewPairs := 0

	log.Printf(
		"[scylladb_1] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
		elapsed,
		len(data.directManagerPairs),
		orgAdminPairs,
		groupViewPairs,
		data.heavyManageUser,
		data.regularViewUser,
	)

	return data
}

// ==========================================================
// RLS check benchmark: can user manage resource?
// ==========================================================

func runCheckManageDirectUser(ctx context.Context, session *gocql.Session, data *benchDataset) {
	const iterations = 1000

	samplePairs := len(data.directManagerPairs)
	if samplePairs == 0 {
		log.Printf("[scylladb_1] [check_manage_direct_user] SKIP: no directManagerPairs in dataset")
		return
	}

	log.Printf("[scylladb_1] [check_manage_direct_user] iterations=%d samplePairs=%d", iterations, samplePairs)

	start := time.Now()
	allowed := 0

	for i := 0; i < iterations; i++ {
		p := data.directManagerPairs[i%samplePairs]

		iterCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ok, err := checkManage(iterCtx, session, p.userID, p.resourceID)
		cancel()
		if err != nil {
			log.Fatalf("[scylladb_1] [check_manage_direct_user] query failed: %v", err)
		}
		if ok {
			allowed++
		}
	}

	total := time.Since(start)
	avg := total / iterations

	log.Printf("[scylladb_1] [check_manage_direct_user] DONE: iters=%d allowed=%d avg=%s total=%s",
		iterations, allowed, avg, total)
}

func checkManage(ctx context.Context, session *gocql.Session, userID, resourceID int) (bool, error) {
	var (
		canManage bool
		canView   bool
	)

	// Single-partition lookup: (resource_id, user_id)
	err := session.
		Query(
			"SELECT can_manage, can_view FROM user_resource_perms_by_resource WHERE resource_id = ? AND user_id = ?",
			resourceID, userID,
		).
		WithContext(ctx).
		Consistency(gocql.LocalOne).
		Scan(&canManage, &canView)

	if err == gocql.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return canManage, nil
}

// ==========================================================
// RLS list benchmarks: which resources can user view?
// ==========================================================

const listLimit = 100 // how many rows we try to fetch per list query

func runListViewHeavyUser(ctx context.Context, session *gocql.Session, data *benchDataset) {
	userID, err := strconv.Atoi(data.heavyManageUser)
	if err != nil {
		log.Fatalf("[scylladb_1] [list_view_heavy_user] invalid heavyManageUser=%q: %v", data.heavyManageUser, err)
	}
	runListViewUser(ctx, session, "list_view_heavy_user", userID)
}

func runListViewRegularUser(ctx context.Context, session *gocql.Session, data *benchDataset) {
	userID, err := strconv.Atoi(data.regularViewUser)
	if err != nil {
		log.Fatalf("[scylladb_1] [list_view_regular_user] invalid regularViewUser=%q: %v", data.regularViewUser, err)
	}
	runListViewUser(ctx, session, "list_view_regular_user", userID)
}

func runListViewUser(ctx context.Context, session *gocql.Session, benchName string, userID int) {
	const iterations = 200

	log.Printf("[scylladb_1] [%s] iterations=%d user_id=%d", benchName, iterations, userID)

	start := time.Now()
	totalLists := 0
	totalRows := 0

	for i := 0; i < iterations; i++ {
		iterCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		n, err := listViewResources(iterCtx, session, userID)
		cancel()
		if err != nil {
			log.Fatalf("[scylladb_1] [%s] query failed: %v", benchName, err)
		}
		totalLists++
		totalRows += n
	}

	total := time.Since(start)
	avg := total / iterations
	avgRows := 0
	if totalLists > 0 {
		avgRows = totalRows / totalLists
	}

	log.Printf("[scylladb_1] [%s] DONE: iters=%d avg=%s total=%s avg_rows_per_list=%d",
		benchName, iterations, avg, total, avgRows)
}

func listViewResources(ctx context.Context, session *gocql.Session, userID int) (int, error) {
	// Single-partition scan by user_id. can_view is not part of the
	// primary key, so filtering in CQL would require ALLOW FILTERING.
	// Instead we fetch rows for the user and filter client-side, with
	// a sensible page size and an early exit once we've collected
	// `listLimit` visible resources.

	iter := session.
		Query(
			"SELECT resource_id, can_view FROM user_resource_perms_by_user WHERE user_id = ?",
			userID,
		).
		WithContext(ctx).
		Consistency(gocql.LocalOne).
		PageSize(500).
		Iter()

	count := 0
	var resID int
	var canView bool
	for iter.Scan(&resID, &canView) {
		if canView {
			count++
			if count >= listLimit {
				// we have enough rows for this list request; stop early
				break
			}
		}
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}
	return count, nil
}
