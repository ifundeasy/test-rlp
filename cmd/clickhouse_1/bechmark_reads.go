package clickhouse_1

import (
	"context"
	"database/sql"
	"log"
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
	directManagerPairs []benchPair // from resource_acl (subject_type=user, relation=manager)
	orgAdminPairs      []benchPair // resource + org admin user
	groupViewPairs     []benchPair // resource + user via viewer_group + group membership

	heavyManageUserID    uint32
	heavyManageUserIDStr string

	regularViewUserID    uint32
	regularViewUserIDStr string
}

// ClickhouseBenchmarkReads runs several read benchmarks against the ClickHouse dataset.
func ClickhouseBenchmarkReads() {
	ctx := context.Background()

	db, cleanup, err := infrastructure.NewClickhouseFromEnv(ctx)
	if err != nil {
		log.Fatalf("[clickhouse_1] failed to create ClickHouse connection: %v", err)
	}
	defer cleanup()

	log.Println("[clickhouse_1] == Building benchmark dataset from ClickHouse ==")
	data := loadBenchDataset(db)
	log.Println("[clickhouse_1] == Running ClickHouse read benchmarks on DB-backed dataset ==")

	runCheckManageDirectUser(db, data)   // direct manager_user ACL
	runCheckManageOrgAdmin(db, data)     // org->admin path
	runCheckViewViaGroupMember(db, data) // via viewer_group + group membership
	runLookupResourcesManageHeavyUser(db, data)
	runLookupResourcesViewRegularUser(db, data)

	log.Println("[clickhouse_1] == ClickHouse read benchmarks DONE ==")
}

// =========================
// Dataset loading from ClickHouse
// =========================

func loadBenchDataset(db *sql.DB) *benchDataset {
	start := time.Now()
	ctx := context.Background()

	// org-level
	orgAdmins := make(map[uint32][]uint32)  // org_id -> []user_id (admin)
	orgMembers := make(map[uint32][]uint32) // org_id -> []user_id (member+admin)

	// groups & memberships
	groupMembers := make(map[uint32][]uint32) // group_id -> []user_id
	groupOrg := make(map[uint32]uint32)       // group_id -> org_id

	// resources
	resOrg := make(map[uint32]uint32) // resource_id -> org_id
	orgResourceCount := make(map[uint32]int)
	var resourceIDs []uint32

	// user permission "weight"
	manageCount := make(map[uint32]int) // approx number of manageable resources
	viewCount := make(map[uint32]int)   // approx number of viewable resources

	// 1) organizations memberships: org_memberships (org_id,user_id,role)
	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT org_id, user_id, role
			FROM org_memberships
		`)
		if err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: query org_memberships failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var orgID, userID uint32
			var role string
			if err := rows.Scan(&orgID, &userID, &role); err != nil {
				log.Fatalf("[clickhouse_1] loadBenchDataset: scan org_memberships failed: %v", err)
			}
			orgMembers[orgID] = append(orgMembers[orgID], userID)
			if role == "admin" {
				orgAdmins[orgID] = append(orgAdmins[orgID], userID)
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: org_memberships rows err: %v", err)
		}
	}()

	// 2) groups: group_id -> org_id
	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT group_id, org_id
			FROM groups
		`)
		if err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: query groups failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var groupID, orgID uint32
			if err := rows.Scan(&groupID, &orgID); err != nil {
				log.Fatalf("[clickhouse_1] loadBenchDataset: scan groups failed: %v", err)
			}
			groupOrg[groupID] = orgID
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: groups rows err: %v", err)
		}
	}()

	// 3) group memberships: group_id -> []user_id
	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT group_id, user_id, role
			FROM group_memberships
		`)
		if err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: query group_memberships failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var groupID, userID uint32
			var role string
			if err := rows.Scan(&groupID, &userID, &role); err != nil {
				log.Fatalf("[clickhouse_1] loadBenchDataset: scan group_memberships failed: %v", err)
			}
			groupMembers[groupID] = append(groupMembers[groupID], userID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: group_memberships rows err: %v", err)
		}
	}()

	// 4) resources: resOrg + orgResourceCount + resourceIDs
	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, org_id
			FROM resources
		`)
		if err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: query resources failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var resID, orgID uint32
			if err := rows.Scan(&resID, &orgID); err != nil {
				log.Fatalf("[clickhouse_1] loadBenchDataset: scan resources failed: %v", err)
			}
			resOrg[resID] = orgID
			orgResourceCount[orgID]++
			resourceIDs = append(resourceIDs, resID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: resources rows err: %v", err)
		}
	}()

	// 5) org-level contributions:
	//    manage: org.admin
	//    view: org.member (including admins)
	for orgID, admins := range orgAdmins {
		cnt := orgResourceCount[orgID]
		if cnt == 0 {
			continue
		}
		for _, u := range admins {
			manageCount[u] += cnt
			viewCount[u] += cnt // admins are also members
		}
	}
	for orgID, members := range orgMembers {
		cnt := orgResourceCount[orgID]
		if cnt == 0 {
			continue
		}
		for _, u := range members {
			viewCount[u] += cnt
		}
	}
	// group -> org membership (similar to organization.member_group in Authzed)
	for groupID, members := range groupMembers {
		orgID, ok := groupOrg[groupID]
		if !ok {
			continue
		}
		cnt := orgResourceCount[orgID]
		if cnt == 0 {
			continue
		}
		for _, u := range members {
			viewCount[u] += cnt
		}
	}

	// 6) resource ACL contributions
	var directManagerPairs []benchPair
	var groupViewPairs []benchPair
	groupSampleIndex := make(map[uint32]int)

	func() {
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, subject_type, subject_id, relation
			FROM resource_acl
		`)
		if err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: query resource_acl failed: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var resID, subjID uint32
			var subjType, relation string
			if err := rows.Scan(&resID, &subjType, &subjID, &relation); err != nil {
				log.Fatalf("[clickhouse_1] loadBenchDataset: scan resource_acl failed: %v", err)
			}

			switch subjType {
			case "user":
				switch relation {
				case "manager":
					directManagerPairs = append(directManagerPairs, benchPair{
						ResourceID: resID,
						UserID:     subjID,
					})
					manageCount[subjID]++
				case "viewer":
					viewCount[subjID]++
				}
			case "group":
				members := groupMembers[subjID]
				if len(members) == 0 {
					continue
				}
				switch relation {
				case "manager":
					for _, u := range members {
						manageCount[u]++
					}
				case "viewer":
					for _, u := range members {
						viewCount[u]++
					}
					// sample one user per (group,resource) for the "view via group member" benchmark
					idx := groupSampleIndex[subjID] % len(members)
					groupSampleIndex[subjID]++
					userID := members[idx]
					groupViewPairs = append(groupViewPairs, benchPair{
						ResourceID: resID,
						UserID:     userID,
					})
				}
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[clickhouse_1] loadBenchDataset: resource_acl rows err: %v", err)
		}
	}()

	// 7) propagate manage -> view (schema: view includes manage)
	for userID, mc := range manageCount {
		viewCount[userID] += mc
	}

	// 8) Build orgAdminPairs: satu admin per resource (round-robin per org)
	var orgAdminPairs []benchPair
	adminRoundRobin := make(map[uint32]int)

	for _, resID := range resourceIDs {
		orgID := resOrg[resID]
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

	// 9) Pick heavyManageUser & regularViewUser
	var heavyManageUserID uint32
	maxManage := 0
	for userID, c := range manageCount {
		if c > maxManage {
			maxManage = c
			heavyManageUserID = userID
		}
	}

	var regularViewUserID uint32
	maxView := 0
	for userID, c := range viewCount {
		if userID == heavyManageUserID {
			continue
		}
		if c > maxView {
			maxView = c
			regularViewUserID = userID
		}
	}
	if regularViewUserID == 0 {
		regularViewUserID = heavyManageUserID
	}

	heavyStr := strconv.FormatUint(uint64(heavyManageUserID), 10)
	regStr := strconv.FormatUint(uint64(regularViewUserID), 10)

	// 10) Allow override via env (shared with lookup_resources_* benchmarks)
	if v := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER"); v != "" {
		if id, err := strconv.ParseUint(v, 10, 32); err == nil {
			heavyManageUserID = uint32(id)
			heavyStr = v
		}
	}
	if v := os.Getenv("BENCH_LOOKUPRES_VIEW_USER"); v != "" {
		if id, err := strconv.ParseUint(v, 10, 32); err == nil {
			regularViewUserID = uint32(id)
			regStr = v
		}
	}

	elapsed := time.Since(start).Truncate(time.Second)
	log.Printf("[clickhouse_1] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
		elapsed, len(directManagerPairs), len(orgAdminPairs), len(groupViewPairs),
		heavyStr, regStr)

	return &benchDataset{
		directManagerPairs:   directManagerPairs,
		orgAdminPairs:        orgAdminPairs,
		groupViewPairs:       groupViewPairs,
		heavyManageUserID:    heavyManageUserID,
		heavyManageUserIDStr: heavyStr,
		regularViewUserID:    regularViewUserID,
		regularViewUserIDStr: regStr,
	}
}

// ===============================
// Bench 1: Check "manage" via direct manager_user
// ===============================

func runCheckManageDirectUser(db *sql.DB, data *benchDataset) {
	pairs := data.directManagerPairs
	if len(pairs) == 0 {
		log.Printf("[clickhouse_1] [check_manage_direct_user] skipped: no direct manager_user ACL entries")
		return
	}

	iters := getEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)

	name := "check_manage_direct_user"
	log.Printf("[clickhouse_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

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
			FROM resource_acl
			WHERE resource_id = ? AND subject_type = 'user' AND subject_id = ? AND relation = 'manager'
			LIMIT 1
		`, resID, userID)

		var dummy int
		err := row.Scan(&dummy)
		cancel()
		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[clickhouse_1] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 2: Check "manage" via org admin (org->admin)
// ===============================

func runCheckManageOrgAdmin(db *sql.DB, data *benchDataset) {
	pairs := data.orgAdminPairs
	if len(pairs) == 0 {
		log.Printf("[clickhouse_1] [check_manage_org_admin] skipped: no org admin + resource pairs")
		return
	}

	iters := getEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)

	name := "check_manage_org_admin"
	log.Printf("[clickhouse_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

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
			FROM resources r
			JOIN org_memberships om
				ON om.org_id = r.org_id
				AND om.user_id = ?
				AND om.role = 'admin'
			WHERE r.resource_id = ?
			LIMIT 1
		`, userID, resID)

		var dummy int
		err := row.Scan(&dummy)
		cancel()
		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[clickhouse_1] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 3: Check "view" via group membership
// ===============================

func runCheckViewViaGroupMember(db *sql.DB, data *benchDataset) {
	pairs := data.groupViewPairs
	if len(pairs) == 0 {
		log.Printf("[clickhouse_1] [check_view_via_group_member] skipped: no viewer_group-based sample pairs")
		return
	}

	iters := getEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)

	name := "check_view_via_group_member"
	log.Printf("[clickhouse_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

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
			FROM resource_acl ra
			JOIN group_memberships gm
				ON gm.group_id = ra.subject_id
			WHERE ra.resource_id = ?
			  AND ra.subject_type = 'group'
			  AND ra.relation = 'viewer'
			  AND gm.user_id = ?
			LIMIT 1
		`, resID, userID)

		var dummy int
		err := row.Scan(&dummy)
		cancel()
		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[clickhouse_1] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 4: LookupResources "manage" for a heavy user
// ===============================

func runLookupResourcesManageHeavyUser(db *sql.DB, data *benchDataset) {
	iters := getEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)

	// Env override (string) -> parse; else use dataset heavy
	var userID uint32
	userIDStr := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	if userIDStr != "" {
		id, err := strconv.ParseUint(userIDStr, 10, 32)
		if err != nil {
			log.Fatalf("[clickhouse_1] [lookup_resources_manage_super] invalid BENCH_LOOKUPRES_MANAGE_USER=%q: %v", userIDStr, err)
		}
		userID = uint32(id)
	} else {
		userID = data.heavyManageUserID
		userIDStr = data.heavyManageUserIDStr
	}
	if userID == 0 {
		log.Printf("[clickhouse_1] [lookup_resources_manage_super] skipped: no heavyManageUser found")
		return
	}

	name := "lookup_resources_manage_super"
	log.Printf("[clickhouse_1] [%s] iterations=%d user=%s", name, iters, userIDStr)

	var total time.Duration
	var lastCount int

	query := `
		SELECT DISTINCT resource_id
		FROM (
			-- manage via direct user
			SELECT ra.resource_id
			FROM resource_acl ra
			WHERE ra.subject_type = 'user'
			  AND ra.relation = 'manager'
			  AND ra.subject_id = ?

			UNION ALL

			-- manage via group
			SELECT ra.resource_id
			FROM resource_acl ra
			JOIN group_memberships gm
				ON gm.group_id = ra.subject_id
			WHERE ra.subject_type = 'group'
			  AND ra.relation = 'manager'
			  AND gm.user_id = ?

			UNION ALL

			-- manage via org admin
			SELECT r.resource_id
			FROM resources r
			JOIN org_memberships om
				ON om.org_id = r.org_id
			WHERE om.user_id = ?
			  AND om.role = 'admin'
		)
	`

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		rows, err := db.QueryContext(ctx, query, userID, userID, userID)
		if err != nil {
			cancel()
			log.Fatalf("[clickhouse_1] [%s] query failed: %v", name, err)
		}

		count := 0
		for rows.Next() {
			var resID uint32
			if err := rows.Scan(&resID); err != nil {
				rows.Close()
				cancel()
				log.Fatalf("[clickhouse_1] [%s] scan failed: %v", name, err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			cancel()
			log.Fatalf("[clickhouse_1] [%s] rows err: %v", name, err)
		}
		rows.Close()
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[clickhouse_1] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse_1] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}

// ===============================
// Bench 5: LookupResources "view" for a regular-ish user
// ===============================

func runLookupResourcesViewRegularUser(db *sql.DB, data *benchDataset) {
	iters := getEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)

	var userID uint32
	userIDStr := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if userIDStr != "" {
		id, err := strconv.ParseUint(userIDStr, 10, 32)
		if err != nil {
			log.Fatalf("[clickhouse_1] [lookup_resources_view_regular] invalid BENCH_LOOKUPRES_VIEW_USER=%q: %v", userIDStr, err)
		}
		userID = uint32(id)
	} else {
		userID = data.regularViewUserID
		userIDStr = data.regularViewUserIDStr
	}
	if userID == 0 {
		log.Printf("[clickhouse_1] [lookup_resources_view_regular] skipped: no regularViewUser found")
		return
	}

	name := "lookup_resources_view_regular"
	log.Printf("[clickhouse_1] [%s] iterations=%d user=%s", name, iters, userIDStr)

	var total time.Duration
	var lastCount int

	query := `
		SELECT DISTINCT resource_id
		FROM (
			-- view via manage (direct user)
			SELECT ra.resource_id
			FROM resource_acl ra
			WHERE ra.subject_type = 'user'
			  AND ra.relation = 'manager'
			  AND ra.subject_id = ?

			UNION ALL

			-- view via manage (group)
			SELECT ra.resource_id
			FROM resource_acl ra
			JOIN group_memberships gm
				ON gm.group_id = ra.subject_id
			WHERE ra.subject_type = 'group'
			  AND ra.relation = 'manager'
			  AND gm.user_id = ?

			UNION ALL

			-- view via manage (org admin)
			SELECT r.resource_id
			FROM resources r
			JOIN org_memberships om
				ON om.org_id = r.org_id
			WHERE om.user_id = ?
			  AND om.role = 'admin'

			UNION ALL

			-- view via org membership (member or admin)
			SELECT r.resource_id
			FROM resources r
			JOIN org_memberships om
				ON om.org_id = r.org_id
			WHERE om.user_id = ?

			UNION ALL

			-- view via viewer_user ACL
			SELECT ra.resource_id
			FROM resource_acl ra
			WHERE ra.subject_type = 'user'
			  AND ra.relation = 'viewer'
			  AND ra.subject_id = ?

			UNION ALL

			-- view via viewer_group ACL
			SELECT ra.resource_id
			FROM resource_acl ra
			JOIN group_memberships gm
				ON gm.group_id = ra.subject_id
			WHERE ra.subject_type = 'group'
			  AND ra.relation = 'viewer'
			  AND gm.user_id = ?
		)
	`

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		rows, err := db.QueryContext(ctx, query,
			userID, userID, userID, userID, userID, userID,
		)
		if err != nil {
			cancel()
			log.Fatalf("[clickhouse_1] [%s] query failed: %v", name, err)
		}

		count := 0
		for rows.Next() {
			var resID uint32
			if err := rows.Scan(&resID); err != nil {
				rows.Close()
				cancel()
				log.Fatalf("[clickhouse_1] [%s] scan failed: %v", name, err)
			}
			count++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			cancel()
			log.Fatalf("[clickhouse_1] [%s] rows err: %v", name, err)
		}
		rows.Close()
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[clickhouse_1] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse_1] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
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
