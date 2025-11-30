package scylladb

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/gocql/gocql"

	"test-tls/infrastructure"
	"test-tls/utils"
)

type benchPair struct {
	userID     int
	resourceID int
}

type benchDataset struct {
	directManagerPairs []benchPair
	orgAdminPairs      []benchPair // reserved for future, kept for symmetry with other backends
	groupViewPairs     []benchPair // reserved for future, kept for symmetry with other backends

	heavyManageUser string // user ID (string) used for "manage" list benchmarks
	regularViewUser string // user ID (string) used for "view" list benchmarks
}

// ScylladbBenchmarkReads runs read benchmarks for ScyllaDB using the
// compiled permission tables:
//
//   - user_resource_perms_by_resource: optimized for point checks
//   - user_resource_perms_by_user: optimized for listing resources
//
// It follows the same logging style as other backends so you can
// compare latency profiles across engines.
func ScylladbBenchmarkReads() {
	ctx := context.Background()

	session, cleanup, err := infrastructure.NewScyllaFromEnv(ctx)
	if err != nil {
		log.Fatalf("[scylladb] NewScyllaFromEnv failed: %v", err)
	}
	defer cleanup()

	data := buildBenchDataset(ctx, session)

	log.Printf("[scylladb] == Running ScyllaDB read benchmarks on DB-backed dataset ==")

	runCheckManageDirectUser(ctx, session, data)
	runCheckManageOrgAdmin(ctx, session, data)
	runCheckViewViaGroupMember(ctx, session, data)
	runLookupResourcesManageHeavyUser(ctx, session, data)
	runListViewRegularUser(ctx, session, data)

	log.Println("[scylladb] == ScyllaDB read benchmarks DONE ==")
}

// ==========================================================
// Dataset builder (from compiled permission tables)
// ==========================================================

// buildBenchDataset prepares the in-memory benchmark dataset:
//
//   - uses heavy/regular users provided via env vars if set
//   - otherwise picks random users from sampled data (ACL-based pairs)
//   - builds directManagerPairs from ACL
//   - builds orgAdminPairs from org memberships
//   - builds groupViewPairs from group ACL + expanded membership
//
// There is intentionally NO full-table scan of the closure tables.
func buildBenchDataset(ctx context.Context, session *gocql.Session) *benchDataset {
	log.Printf("[scylladb] == Building benchmark dataset from ScyllaDB ==")
	start := time.Now()

	// ======================================================
	// 1) Environment overrides (optional)
	// ======================================================

	envHeavy := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	envRegular := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")

	// ======================================================
	// 2) Build directManagerPairs from ACL (for direct manage checks)
	// ======================================================

	var directManagerPairs []benchPair

	aclIter := session.
		Query("SELECT resource_id, relation, subject_type, subject_id FROM resource_acl_by_resource").
		WithContext(ctx).
		Iter()

	var (
		resID    int
		rel      string
		subjType string
		subjID   int
	)

	for aclIter.Scan(&resID, &rel, &subjType, &subjID) {
		if subjType == "user" && (rel == "manager_user" || rel == "manager") {
			directManagerPairs = append(directManagerPairs, benchPair{
				userID:     subjID,
				resourceID: resID,
			})
		}
	}
	if err := aclIter.Close(); err != nil {
		log.Fatalf("[scylladb] buildBenchDataset: ACL scan failed: %v", err)
	}

	// ======================================================
	// 3) Build orgAdminPairs (resource -> org -> admin user)
	// ======================================================

	resourceOrg := make(map[int]int)
	resIter := session.
		Query("SELECT resource_id, org_id FROM resources").
		WithContext(ctx).
		Iter()
	var orgID int
	for resIter.Scan(&resID, &orgID) {
		resourceOrg[resID] = orgID
	}
	if err := resIter.Close(); err != nil {
		log.Fatalf("[scylladb] buildBenchDataset: resources scan failed: %v", err)
	}

	orgAdmins := make(map[int][]int)
	orgIter := session.
		Query("SELECT org_id, user_id, role FROM org_memberships").
		WithContext(ctx).
		Iter()
	var userIDtmp int
	var role string
	for orgIter.Scan(&orgID, &userIDtmp, &role) {
		if role == "admin" {
			orgAdmins[orgID] = append(orgAdmins[orgID], userIDtmp)
		}
	}
	if err := orgIter.Close(); err != nil {
		log.Fatalf("[scylladb] buildBenchDataset: org_memberships scan failed: %v", err)
	}

	var orgAdminPairs []benchPair
	adminRoundRobin := make(map[int]int)
	for res, org := range resourceOrg {
		admins := orgAdmins[org]
		if len(admins) == 0 {
			continue
		}
		idx := adminRoundRobin[org] % len(admins)
		adminRoundRobin[org]++
		orgAdminPairs = append(orgAdminPairs, benchPair{
			userID:     admins[idx],
			resourceID: res,
		})
	}

	// ======================================================
	// 4) Build groupViewPairs (viewer_group -> effective member user)
	// ======================================================

	var groupViewPairs []benchPair
	groupSampleIndex := make(map[int]int)

	aclIter2 := session.
		Query("SELECT resource_id, relation, subject_type, subject_id FROM resource_acl_by_resource").
		WithContext(ctx).
		Iter()
	for aclIter2.Scan(&resID, &rel, &subjType, &subjID) {
		if subjType == "group" && (rel == "viewer_group" || rel == "viewer") {
			// For each viewer_group edge, sample a single effective member
			// from group_members_expanded (role = 'member').
			gmIter := session.
				Query("SELECT user_id FROM group_members_expanded WHERE group_id = ? AND role = ?",
					subjID, "member").
				WithContext(ctx).
				Iter()
			var members []int
			var mUser int
			for gmIter.Scan(&mUser) {
				members = append(members, mUser)
			}
			_ = gmIter.Close()

			if len(members) == 0 {
				continue
			}

			idx := groupSampleIndex[subjID] % len(members)
			groupSampleIndex[subjID]++
			sampleUser := members[idx]

			groupViewPairs = append(groupViewPairs, benchPair{
				userID:     sampleUser,
				resourceID: resID,
			})
		}
	}
	if err := aclIter2.Close(); err != nil {
		log.Fatalf("[scylladb] buildBenchDataset: ACL scan (2) failed: %v", err)
	}

	// ======================================================
	// 5) Pick heavy / regular users (env override, else random)
	// ======================================================

	rand.Seed(time.Now().UnixNano())

	heavyUserStr := envHeavy
	regularUserStr := envRegular

	// If no env for heavy user, pick a random user from directManagerPairs.
	if heavyUserStr == "" && len(directManagerPairs) > 0 {
		idx := rand.Intn(len(directManagerPairs))
		heavyUserStr = strconv.Itoa(directManagerPairs[idx].userID)
	}

	// If no env for regular user, pick a random user from groupViewPairs,
	// trying to avoid the same user as heavyManageUser if possible.
	if regularUserStr == "" && len(groupViewPairs) > 0 {
		var heavyInt = -1
		if heavyUserStr != "" {
			if v, err := strconv.Atoi(heavyUserStr); err == nil {
				heavyInt = v
			}
		}

		candidates := make([]int, 0, len(groupViewPairs))
		for _, p := range groupViewPairs {
			if p.userID == heavyInt {
				continue
			}
			candidates = append(candidates, p.userID)
		}

		if len(candidates) == 0 {
			idx := rand.Intn(len(groupViewPairs))
			regularUserStr = strconv.Itoa(groupViewPairs[idx].userID)
		} else {
			idx := rand.Intn(len(candidates))
			regularUserStr = strconv.Itoa(candidates[idx])
		}
	}

	// Safety fallback: if one side is still empty but the other is set,
	// reuse the known one.
	if heavyUserStr == "" && regularUserStr != "" {
		heavyUserStr = regularUserStr
	}
	if regularUserStr == "" && heavyUserStr != "" {
		regularUserStr = heavyUserStr
	}

	// ======================================================
	// 6) Finalize dataset and log summary
	// ======================================================

	data := &benchDataset{
		directManagerPairs: directManagerPairs,
		orgAdminPairs:      orgAdminPairs,
		groupViewPairs:     groupViewPairs,
		heavyManageUser:    heavyUserStr,
		regularViewUser:    regularUserStr,
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf(
		"[scylladb] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
		elapsed,
		len(data.directManagerPairs),
		len(data.orgAdminPairs),
		len(data.groupViewPairs),
		data.heavyManageUser,
		data.regularViewUser,
	)

	return data
}

// ==========================================================
// RLS check benchmark: can user manage/view a resource?
// ==========================================================

func runCheckManageDirectUser(ctx context.Context, session *gocql.Session, data *benchDataset) {
	iterations := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)

	samplePairs := len(data.directManagerPairs)
	if samplePairs == 0 {
		log.Printf("[scylladb] [check_manage_direct_user] SKIP: no directManagerPairs in dataset")
		return
	}

	log.Printf("[scylladb] [check_manage_direct_user] iterations=%d samplePairs=%d", iterations, samplePairs)

	start := time.Now()
	allowed := 0

	for i := 0; i < iterations; i++ {
		p := data.directManagerPairs[i%samplePairs]

		iterCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ok, err := checkManage(iterCtx, session, p.userID, p.resourceID)
		cancel()
		if err != nil {
			log.Fatalf("[scylladb] [check_manage_direct_user] query failed: %v", err)
		}
		if ok {
			allowed++
		}
	}

	total := time.Since(start)
	avg := total / time.Duration(iterations)

	log.Printf("[scylladb] [check_manage_direct_user] DONE: iters=%d allowed=%d avg=%s total=%s",
		iterations, allowed, avg, total)
}

// checkManage checks the compiled can_manage flag for a given (user, resource).
func checkManage(ctx context.Context, session *gocql.Session, userID, resourceID int) (bool, error) {
	var (
		canManage bool
		canView   bool
	)

	// Single-partition lookup: partitioned by resource_id, keyed by user_id.
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

// checkView checks the compiled can_view flag for a given (user, resource).
func checkView(ctx context.Context, session *gocql.Session, userID, resourceID int) (bool, error) {
	var canManage bool
	var canView bool

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
	return canView, nil
}

// runCheckManageOrgAdmin runs the "manage via org admin" check benchmark.
func runCheckManageOrgAdmin(ctx context.Context, session *gocql.Session, data *benchDataset) {
	iterations := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)

	samplePairs := len(data.orgAdminPairs)
	if samplePairs == 0 {
		log.Printf("[scylladb] [check_manage_org_admin] SKIP: no orgAdminPairs in dataset")
		return
	}

	log.Printf("[scylladb] [check_manage_org_admin] iterations=%d samplePairs=%d", iterations, samplePairs)

	start := time.Now()
	allowed := 0

	for i := 0; i < iterations; i++ {
		p := data.orgAdminPairs[i%samplePairs]

		iterCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ok, err := checkManage(iterCtx, session, p.userID, p.resourceID)
		cancel()
		if err != nil {
			log.Fatalf("[scylladb] [check_manage_org_admin] query failed: %v", err)
		}
		if ok {
			allowed++
		}
	}

	total := time.Since(start)
	avg := total / time.Duration(iterations)

	log.Printf("[scylladb] [check_manage_org_admin] DONE: iters=%d allowed=%d avg=%s total=%s",
		iterations, allowed, avg, total)
}

// runCheckViewViaGroupMember runs the "view via group membership" check benchmark.
func runCheckViewViaGroupMember(ctx context.Context, session *gocql.Session, data *benchDataset) {
	iterations := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)

	samplePairs := len(data.groupViewPairs)
	if samplePairs == 0 {
		log.Printf("[scylladb] [check_view_via_group_member] SKIP: no groupViewPairs in dataset")
		return
	}

	log.Printf("[scylladb] [check_view_via_group_member] iterations=%d samplePairs=%d", iterations, samplePairs)

	start := time.Now()
	allowed := 0

	for i := 0; i < iterations; i++ {
		p := data.groupViewPairs[i%samplePairs]

		iterCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ok, err := checkView(iterCtx, session, p.userID, p.resourceID)
		cancel()
		if err != nil {
			log.Fatalf("[scylladb] [check_view_via_group_member] query failed: %v", err)
		}
		if ok {
			allowed++
		}
	}

	total := time.Since(start)
	avg := total / time.Duration(iterations)

	log.Printf("[scylladb] [check_view_via_group_member] DONE: iters=%d allowed=%d avg=%s total=%s",
		iterations, allowed, avg, total)
}

// runLookupResourcesManageHeavyUser runs a LookupResources-style benchmark
// for "manage" permission for a heavy user.
func runLookupResourcesManageHeavyUser(ctx context.Context, session *gocql.Session, data *benchDataset) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)
	userIDStr := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	if userIDStr == "" {
		userIDStr = data.heavyManageUser
	}
	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		log.Fatalf("[scylladb] [lookup_resources_manage_super] invalid user id %q: %v", userIDStr, err)
	}

	runLookupBenchManage(ctx, session, "lookup_resources_manage_super", "manage", userID, iters, 60*time.Second)
}

// runLookupBenchManage drives a list-style benchmark for resources where
// can_manage is true for a given user.
func runLookupBenchManage(ctx context.Context, session *gocql.Session, name, permission string, userID int, iters int, timeout time.Duration) {
	if userID == 0 {
		log.Printf("[scylladb] [%s] skipped: no user specified", name)
		return
	}

	log.Printf("[scylladb] [%s] iterations=%d user=%d", name, iters, userID)

	var total time.Duration
	var lastCount int

	for i := 0; i < iters; i++ {
		ctx2, cancel := context.WithTimeout(ctx, timeout)
		start := time.Now()

		count, err := listManageResources(ctx2, session, userID)
		cancel()
		if err != nil {
			log.Fatalf("[scylladb] [%s] query failed: %v", name, err)
		}

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[scylladb] [%s] iter=%d resources=%d duration=%s", name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[scylladb] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s", name, iters, lastCount, avg, total)
}

// listManageResources lists resources with can_manage=true for a user,
// with a client-side limit and paging to avoid scanning the entire partition.
func listManageResources(ctx context.Context, session *gocql.Session, userID int) (int, error) {
	iter := session.
		Query(
			"SELECT resource_id, can_manage FROM user_resource_perms_by_user WHERE user_id = ?",
			userID,
		).
		WithContext(ctx).
		Consistency(gocql.LocalOne).
		PageSize(500).
		Iter()

	count := 0
	var resID int
	var canManage bool
	for iter.Scan(&resID, &canManage) {
		if canManage {
			count++
			if count >= listLimit() {
				break
			}
		}
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}
	return count, nil
}

// ==========================================================
// RLS list benchmarks: which resources can a user view?
// ==========================================================

// listLimit returns how many rows we try to fetch per list query. Make
// it configurable so benchmarks can match other engines. Default is
// 1000 to match the larger defaults used elsewhere.
func listLimit() int { return utils.GetEnvInt("BENCH_LOOKUPRES_LIST_LIMIT", 1000) }

func runListViewHeavyUser(ctx context.Context, session *gocql.Session, data *benchDataset) {
	userID, err := strconv.Atoi(data.heavyManageUser)
	if err != nil {
		log.Fatalf("[scylladb] [list_view_heavy_user] invalid heavyManageUser=%q: %v", data.heavyManageUser, err)
	}
	runListViewUser(ctx, session, "list_view_heavy_user", userID)
}

func runListViewRegularUser(ctx context.Context, session *gocql.Session, data *benchDataset) {
	userIDStr := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if userIDStr == "" {
		userIDStr = data.regularViewUser
	}
	userID, err := strconv.Atoi(userIDStr)
	if err != nil {
		log.Fatalf("[scylladb] [list_view_regular_user] invalid regularViewUser=%q: %v", userIDStr, err)
	}
	runListViewUser(ctx, session, "list_view_regular_user", userID)
}

// runListViewUser runs a list-style benchmark for viewing permissions
// for a particular user.
func runListViewUser(ctx context.Context, session *gocql.Session, benchName string, userID int) {
	iterations := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)

	log.Printf("[scylladb] [%s] iterations=%d user_id=%d", benchName, iterations, userID)

	start := time.Now()
	totalLists := 0
	totalRows := 0

	for i := 0; i < iterations; i++ {
		iterStart := time.Now()
		iterCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		n, err := listViewResources(iterCtx, session, userID)
		cancel()
		iterDur := time.Since(iterStart)
		if err != nil {
			log.Fatalf("[scylladb] [%s] query failed: %v", benchName, err)
		}
		totalLists++
		totalRows += n

		// Per-iteration logging for detailed visibility and comparability
		// with other backends that emit per-iteration details.
		log.Printf("[scylladb] [%s] iter=%d resources=%d duration=%s limit=%d",
			benchName, i, n, iterDur.Truncate(time.Millisecond), listLimit())
	}

	total := time.Since(start)
	avg := total / time.Duration(iterations)
	avgRows := 0
	if totalLists > 0 {
		avgRows = totalRows / totalLists
	}

	log.Printf("[scylladb] [%s] DONE: iters=%d avg=%s total=%s avg_rows_per_list=%d",
		benchName, iterations, avg, total, avgRows)
}

// listViewResources lists resources with can_view=true for a user.
//
// It performs a single-partition scan by user_id and filters can_view
// client-side, with a page size and early exit once listLimit()
// resources are collected.
func listViewResources(ctx context.Context, session *gocql.Session, userID int) (int, error) {
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
			if count >= listLimit() {
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
