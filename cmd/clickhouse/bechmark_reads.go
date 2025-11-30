package clickhouse

import (
	"context"
	"database/sql"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"test-tls/infrastructure"
)

// benchPair couples a resource with a user that should have a given permission.
type benchPair struct {
	ResourceID uint32
	UserID     uint32
}

// benchDataset holds precomputed samples and heavy users for the benchmarks.
type benchDataset struct {
	directManagerPairs []benchPair // from resource_acl (subject_type='user', relation='manager')
	orgAdminPairs      []benchPair // resource + org admin user
	groupViewPairs     []benchPair // resource + user via viewer_group + group membership

	heavyManageUser string // user used for "manage" lookup benchmarks (string form)
	regularViewUser string // user used for "view" lookup benchmarks (string form)
}

// ClickhouseBenchmarkReads runs several read benchmarks against the ClickHouse dataset.
func ClickhouseBenchmarkReads() {
	ctx := context.Background()

	db, cleanup, err := infrastructure.NewClickhouseFromEnv(ctx)
	if err != nil {
		log.Fatalf("[clickhouse] failed to create ClickHouse connection: %v", err)
	}
	defer cleanup()

	log.Println("[clickhouse] == Building benchmark dataset from ClickHouse ==")
	data := loadBenchDataset(db)
	log.Println("[clickhouse] == Running ClickHouse read benchmarks on DB-backed dataset ==")

	runCheckManageDirectUser(db, data)   // direct manager_user ACL
	runCheckManageOrgAdmin(db, data)     // org->admin path
	runCheckViewViaGroupMember(db, data) // via viewer_group + group membership
	runLookupResourcesManageHeavyUser(db, data)
	runLookupResourcesViewRegularUser(db, data)

	log.Println("[clickhouse] == ClickHouse read benchmarks DONE ==")
}

// =========================
// Dataset loading from ClickHouse
// =========================

// loadBenchDataset mirrors the Authzed behavior:
//
//   - Build sample pairs for:
//   - direct manager (resource_acl: user + manager)
//   - org admin (resources + org_memberships[role=admin])
//   - view via group (resource_acl: group + viewer + group_members_expanded)
//   - Choose heavy/regular users from environment variables if present,
//     otherwise pick them randomly from the sampled pairs.
//
// It avoids global counting maps and does not reconstruct the full
// permission graph in memory.
func loadBenchDataset(db *sql.DB) *benchDataset {
	start := time.Now()
	ctx := context.Background()

	var directManagerPairs []benchPair
	var orgAdminPairs []benchPair
	var groupViewPairs []benchPair

	// For org admin sampling:
	//   - resource_id -> org_id
	//   - org_id -> []admin_user_id
	resourceOrg := make(map[uint32]uint32)
	var resourceIDs []uint32
	orgAdmins := make(map[uint32][]uint32)

	// For group viewer sampling we use round-robin index per group.
	groupSampleIndex := make(map[uint32]int)

	// 1) Load resource -> org mapping.
	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, org_id
			FROM resources
		`)
		if err != nil {
			log.Fatalf("[clickhouse] loadBenchDataset: query resources failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var resID, orgID uint32
			if err := rows.Scan(&resID, &orgID); err != nil {
				log.Fatalf("[clickhouse] loadBenchDataset: scan resources failed: %v", err)
			}
			resourceOrg[resID] = orgID
			resourceIDs = append(resourceIDs, resID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse] loadBenchDataset: resources rows err: %v", err)
		}
	}()

	// 2) Load org admin users (role='admin').
	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT org_id, user_id
			FROM org_memberships
			WHERE role = 'admin'
		`)
		if err != nil {
			log.Fatalf("[clickhouse] loadBenchDataset: query org_memberships failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var orgID, userID uint32
			if err := rows.Scan(&orgID, &userID); err != nil {
				log.Fatalf("[clickhouse] loadBenchDataset: scan org_memberships failed: %v", err)
			}
			orgAdmins[orgID] = append(orgAdmins[orgID], userID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse] loadBenchDataset: org_memberships rows err: %v", err)
		}
	}()

	// 3) Build directManagerPairs from resource_acl (user + manager).
	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, subject_id
			FROM resource_acl
			WHERE subject_type = 'user' AND relation = 'manager'
		`)
		if err != nil {
			log.Fatalf("[clickhouse] loadBenchDataset: query resource_acl (manager_user) failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var resID, userID uint32
			if err := rows.Scan(&resID, &userID); err != nil {
				log.Fatalf("[clickhouse] loadBenchDataset: scan resource_acl (manager_user) failed: %v", err)
			}
			directManagerPairs = append(directManagerPairs, benchPair{
				ResourceID: resID,
				UserID:     userID,
			})
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse] loadBenchDataset: resource_acl (manager_user) rows err: %v", err)
		}
	}()

	// 4) Build groupViewPairs from resource_acl (group + viewer),
	//    sampling one effective member (role='member') from group_members_expanded.
	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, subject_id
			FROM resource_acl
			WHERE subject_type = 'group' AND relation = 'viewer'
		`)
		if err != nil {
			log.Fatalf("[clickhouse] loadBenchDataset: query resource_acl (viewer_group) failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var resID, groupID uint32
			if err := rows.Scan(&resID, &groupID); err != nil {
				log.Fatalf("[clickhouse] loadBenchDataset: scan resource_acl (viewer_group) failed: %v", err)
			}

			// Fetch effective members for this group from group_members_expanded.
			// role='member' is enough for "view via group member".
			memberRows, err := db.QueryContext(ctx, `
				SELECT user_id
				FROM group_members_expanded
				WHERE group_id = ? AND role = 'member'
			`, groupID)
			if err != nil {
				log.Fatalf("[clickhouse] loadBenchDataset: query group_members_expanded failed: %v", err)
			}

			var members []uint32
			for memberRows.Next() {
				var u uint32
				if err := memberRows.Scan(&u); err != nil {
					memberRows.Close()
					log.Fatalf("[clickhouse] loadBenchDataset: scan group_members_expanded failed: %v", err)
				}
				members = append(members, u)
			}
			if err := memberRows.Err(); err != nil {
				memberRows.Close()
				log.Fatalf("[clickhouse] loadBenchDataset: group_members_expanded rows err: %v", err)
			}
			memberRows.Close()

			// Fallback: if expanded table is somehow empty, try direct group_memberships.
			if len(members) == 0 {
				fallbackRows, err := db.QueryContext(ctx, `
					SELECT user_id
					FROM group_memberships
					WHERE group_id = ?
				`, groupID)
				if err != nil {
					log.Fatalf("[clickhouse] loadBenchDataset: query group_memberships fallback failed: %v", err)
				}
				for fallbackRows.Next() {
					var u uint32
					if err := fallbackRows.Scan(&u); err != nil {
						fallbackRows.Close()
						log.Fatalf("[clickhouse] loadBenchDataset: scan group_memberships fallback failed: %v", err)
					}
					members = append(members, u)
				}
				if err := fallbackRows.Err(); err != nil {
					fallbackRows.Close()
					log.Fatalf("[clickhouse] loadBenchDataset: group_memberships fallback rows err: %v", err)
				}
				fallbackRows.Close()
			}

			if len(members) == 0 {
				// No members to sample for this group; skip.
				continue
			}

			idx := groupSampleIndex[groupID] % len(members)
			groupSampleIndex[groupID]++
			userID := members[idx]

			groupViewPairs = append(groupViewPairs, benchPair{
				ResourceID: resID,
				UserID:     userID,
			})
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse] loadBenchDataset: resource_acl (viewer_group) rows err: %v", err)
		}
	}()

	// 5) Build orgAdminPairs: one admin per resource (round-robin per org).
	adminRoundRobin := make(map[uint32]int)
	for _, resID := range resourceIDs {
		orgID := resourceOrg[resID]
		admins := orgAdmins[orgID]
		if len(admins) == 0 {
			continue
		}
		idx := adminRoundRobin[orgID] % len(admins)
		adminRoundRobin[orgID]++
		userID := admins[idx]

		orgAdminPairs = append(orgAdminPairs, benchPair{
			ResourceID: resID,
			UserID:     userID,
		})
	}

	// 6) Select heavyManageUser & regularViewUser.
	//
	//   - If env vars are set, they win.
	//   - Else choose random users from the sampled pairs.
	rand.Seed(time.Now().UnixNano())

	heavyManageUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	if heavyManageUser == "" && len(directManagerPairs) > 0 {
		idx := rand.Intn(len(directManagerPairs))
		heavyManageUser = strconv.FormatUint(uint64(directManagerPairs[idx].UserID), 10)
	}

	regularViewUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if regularViewUser == "" && len(groupViewPairs) > 0 {
		// Try to pick a different user from heavyManageUser if possible.
		var heavyID uint32
		if heavyManageUser != "" {
			if v, err := strconv.ParseUint(heavyManageUser, 10, 32); err == nil {
				heavyID = uint32(v)
			}
		}

		candidates := make([]uint32, 0, len(groupViewPairs))
		for _, p := range groupViewPairs {
			if heavyID != 0 && p.UserID == heavyID {
				continue
			}
			candidates = append(candidates, p.UserID)
		}
		if len(candidates) == 0 {
			// Fall back to any user from groupViewPairs.
			idx := rand.Intn(len(groupViewPairs))
			regularViewUser = strconv.FormatUint(uint64(groupViewPairs[idx].UserID), 10)
		} else {
			idx := rand.Intn(len(candidates))
			regularViewUser = strconv.FormatUint(uint64(candidates[idx]), 10)
		}
	}

	// Safety: if one of them is still empty but the other is set, reuse it.
	if heavyManageUser == "" && regularViewUser != "" {
		heavyManageUser = regularViewUser
	}
	if regularViewUser == "" && heavyManageUser != "" {
		regularViewUser = heavyManageUser
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[clickhouse] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
		elapsed, len(directManagerPairs), len(orgAdminPairs), len(groupViewPairs),
		heavyManageUser, regularViewUser)

	return &benchDataset{
		directManagerPairs: directManagerPairs,
		orgAdminPairs:      orgAdminPairs,
		groupViewPairs:     groupViewPairs,
		heavyManageUser:    heavyManageUser,
		regularViewUser:    regularViewUser,
	}
}

// ===============================
// Bench 1: Check "manage" via direct manager_user
// ===============================

func runCheckManageDirectUser(db *sql.DB, data *benchDataset) {
	pairs := data.directManagerPairs
	if len(pairs) == 0 {
		log.Printf("[clickhouse] [check_manage_direct_user] skipped: no direct manager_user ACL entries")
		return
	}

	iters := getEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)

	name := "check_manage_direct_user"
	log.Printf("[clickhouse] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]
		resID := pair.ResourceID
		userID := pair.UserID

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()

		row := db.QueryRowContext(ctx, `
			SELECT 1
			FROM user_resource_permissions
			WHERE resource_id = ? AND user_id = ? AND relation = 'manager'
			LIMIT 1
		`, resID, userID)

		var dummy int
		err := row.Scan(&dummy)
		cancel()
		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[clickhouse] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 2: Check "manage" via org admin (org->admin)
// ===============================

func runCheckManageOrgAdmin(db *sql.DB, data *benchDataset) {
	pairs := data.orgAdminPairs
	if len(pairs) == 0 {
		log.Printf("[clickhouse] [check_manage_org_admin] skipped: no org admin + resource pairs")
		return
	}

	iters := getEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)

	name := "check_manage_org_admin"
	log.Printf("[clickhouse] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]
		resID := pair.ResourceID
		userID := pair.UserID

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()

		// For this benchmark we still check via the compiled permissions table,
		// which already encodes org->admin into manager permissions.
		row := db.QueryRowContext(ctx, `
			SELECT 1
			FROM user_resource_permissions
			WHERE resource_id = ? AND user_id = ? AND relation = 'manager'
			LIMIT 1
		`, resID, userID)

		var dummy int
		err := row.Scan(&dummy)
		cancel()
		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[clickhouse] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 3: Check "view" via group membership
// ===============================

func runCheckViewViaGroupMember(db *sql.DB, data *benchDataset) {
	pairs := data.groupViewPairs
	if len(pairs) == 0 {
		log.Printf("[clickhouse] [check_view_via_group_member] skipped: no viewer_group-based sample pairs")
		return
	}

	iters := getEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)

	name := "check_view_via_group_member"
	log.Printf("[clickhouse] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]
		resID := pair.ResourceID
		userID := pair.UserID

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()

		row := db.QueryRowContext(ctx, `
			SELECT 1
			FROM user_resource_permissions
			WHERE resource_id = ? AND user_id = ? AND relation = 'viewer'
			LIMIT 1
		`, resID, userID)

		var dummy int
		err := row.Scan(&dummy)
		cancel()
		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[clickhouse] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 4: LookupResources "manage" for a heavy user
// ===============================

func runLookupResourcesManageHeavyUser(db *sql.DB, data *benchDataset) {
	iters := getEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)

	userIDStr := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	if userIDStr == "" {
		userIDStr = data.heavyManageUser
	}
	if userIDStr == "" {
		log.Printf("[clickhouse] [lookup_resources_manage_super] skipped: no heavyManageUser found")
		return
	}

	id, err := strconv.ParseUint(userIDStr, 10, 32)
	if err != nil {
		log.Fatalf("[clickhouse] [lookup_resources_manage_super] invalid user id %q: %v", userIDStr, err)
	}
	userID := uint32(id)

	name := "lookup_resources_manage_super"
	log.Printf("[clickhouse] [%s] iterations=%d user=%s", name, iters, userIDStr)

	var total time.Duration
	var lastCount int

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		rows, err := db.QueryContext(ctx, `
			SELECT DISTINCT resource_id
			FROM user_resource_permissions
			WHERE user_id = ? AND relation = 'manager'
		`, userID)
		if err != nil {
			cancel()
			log.Fatalf("[clickhouse] [%s] query failed: %v", name, err)
		}

		count := 0
		for rows.Next() {
			var resID uint32
			if err := rows.Scan(&resID); err != nil {
				rows.Close()
				cancel()
				log.Fatalf("[clickhouse] [%s] scan failed: %v", name, err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			cancel()
			log.Fatalf("[clickhouse] [%s] rows err: %v", name, err)
		}
		rows.Close()
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[clickhouse] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}

// ===============================
// Bench 5: LookupResources "view" for a regular-ish user
// ===============================

func runLookupResourcesViewRegularUser(db *sql.DB, data *benchDataset) {
	iters := getEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)

	userIDStr := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if userIDStr == "" {
		userIDStr = data.regularViewUser
	}
	if userIDStr == "" {
		log.Printf("[clickhouse] [lookup_resources_view_regular] skipped: no regularViewUser found")
		return
	}

	id, err := strconv.ParseUint(userIDStr, 10, 32)
	if err != nil {
		log.Fatalf("[clickhouse] [lookup_resources_view_regular] invalid user id %q: %v", userIDStr, err)
	}
	userID := uint32(id)

	name := "lookup_resources_view_regular"
	log.Printf("[clickhouse] [%s] iterations=%d user=%s", name, iters, userIDStr)

	var total time.Duration
	var lastCount int

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		// Include both viewer and manager as "viewable" resources.
		rows, err := db.QueryContext(ctx, `
			SELECT DISTINCT resource_id
			FROM user_resource_permissions
			WHERE user_id = ? AND relation IN ('viewer','manager')
		`, userID)
		if err != nil {
			cancel()
			log.Fatalf("[clickhouse] [%s] query failed: %v", name, err)
		}

		count := 0
		for rows.Next() {
			var resID uint32
			if err := rows.Scan(&resID); err != nil {
				rows.Close()
				cancel()
				log.Fatalf("[clickhouse] [%s] scan failed: %v", name, err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			cancel()
			log.Fatalf("[clickhouse] [%s] rows err: %v", name, err)
		}
		rows.Close()
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[clickhouse] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}

// getEnvInt reads an int from env, falling back to default if unset or invalid.
func getEnvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
