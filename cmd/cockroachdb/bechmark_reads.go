package cockroachdb

import (
	"context"
	"database/sql"
	"log"
	"math/rand"
	"os"
	"time"

	"test-tls/infrastructure"
	"test-tls/utils"
)

// benchPair couples a resource with a user that should have a given permission.
type benchPair struct {
	ResourceID string
	UserID     string
}

// benchDataset holds precomputed samples and heavy users for the benchmarks.
type benchDataset struct {
	directManagerPairs []benchPair // from resource_acl: subject_type=user, relation=manager
	orgAdminPairs      []benchPair // resource + org admin user
	groupViewPairs     []benchPair // resource + user via viewer_group + group_membership

	heavyManageUser string // user used for "manage" lookup benchmarks
	regularViewUser string // user used for "view" lookup benchmarks
}

// CockroachdbBenchmarkReads runs several read benchmarks against the current dataset.
func CockroachdbBenchmarkReads() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, cleanup, err := infrastructure.NewCockroachDBFromEnv(ctx)
	if err != nil {
		log.Fatalf("[cockroachdb] benchmark: connect failed: %v", err)
	}
	defer cleanup()

	log.Println("[cockroachdb] == Building benchmark dataset from Cockroachdb ==")
	data := loadBenchDataset(db)
	log.Println("[cockroachdb] == Running Cockroachdb read benchmarks on DB-backed dataset ==")

	runCheckManageDirectUser(db, data)   // direct manager ACL
	runCheckManageOrgAdmin(db, data)     // org admin path
	runCheckViewViaGroupMember(db, data) // via viewer_group + group membership
	runLookupResourcesManageHeavyUser(db, data)
	runLookupResourcesViewRegularUser(db, data)

	log.Println("[cockroachdb] == Cockroachdb read benchmarks DONE ==")
}

///////////////////////////////
// Dataset loading from DB   //
///////////////////////////////

// loadBenchDataset builds the benchmark dataset for CockroachDB.
//
// It mirrors the Authzed benchmark behavior:
//
//   - build sample pairs for direct manager, org admin, and group-based view
//   - choose heavy/regular users from environment variables, or fall back
//     to random users drawn from the sampled pairs
//
// It intentionally avoids global "weight" maps and does not attempt to
// reconstruct the full permission graph in memory.
func loadBenchDataset(db *sql.DB) *benchDataset {
	start := time.Now()
	ctx := context.Background()

	var directManagerPairs []benchPair
	var orgAdminPairs []benchPair
	var groupViewPairs []benchPair

	// For org admin sampling we need:
	//   - resource_id -> org_id
	//   - org_id -> []admin_user_id
	resourceOrg := make(map[string]string)
	var resourceIDs []string
	orgAdmins := make(map[string][]string)

	// For group-based view sampling we need:
	//   - group_id -> []user_id
	groupMembers := make(map[string][]string)
	groupSampleIndex := make(map[string]int)

	// 1) Load resource -> org mapping.
	{
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, org_id
			FROM resources
		`)
		if err != nil {
			log.Fatalf("[cockroachdb] loadBenchDataset: query resources: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var resID, orgID string
			if err := rows.Scan(&resID, &orgID); err != nil {
				log.Fatalf("[cockroachdb] loadBenchDataset: scan resources: %v", err)
			}
			resourceOrg[resID] = orgID
			resourceIDs = append(resourceIDs, resID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[cockroachdb] loadBenchDataset: rows resources: %v", err)
		}
	}

	// 2) Load org admin users (role='admin').
	{
		rows, err := db.QueryContext(ctx, `
			SELECT org_id, user_id
			FROM org_memberships
			WHERE role = 'admin'
		`)
		if err != nil {
			log.Fatalf("[cockroachdb] loadBenchDataset: query org_memberships: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var orgID, userID string
			if err := rows.Scan(&orgID, &userID); err != nil {
				log.Fatalf("[cockroachdb] loadBenchDataset: scan org_memberships: %v", err)
			}
			orgAdmins[orgID] = append(orgAdmins[orgID], userID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[cockroachdb] loadBenchDataset: rows org_memberships: %v", err)
		}
	}

	// 3) Load group memberships (role is not needed for sampling).
	{
		rows, err := db.QueryContext(ctx, `
			SELECT group_id, user_id
			FROM group_memberships
		`)
		if err != nil {
			log.Fatalf("[cockroachdb] loadBenchDataset: query group_memberships: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var groupID, userID string
			if err := rows.Scan(&groupID, &userID); err != nil {
				log.Fatalf("[cockroachdb] loadBenchDataset: scan group_memberships: %v", err)
			}
			groupMembers[groupID] = append(groupMembers[groupID], userID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[cockroachdb] loadBenchDataset: rows group_memberships: %v", err)
		}
	}

	// 4) Build direct manager and group view pairs from resource_acl.
	{
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, subject_type, subject_id, relation
			FROM resource_acl
		`)
		if err != nil {
			log.Fatalf("[cockroachdb] loadBenchDataset: query resource_acl: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var resID, subjectType, subjectID, relation string
			if err := rows.Scan(&resID, &subjectType, &subjectID, &relation); err != nil {
				log.Fatalf("[cockroachdb] loadBenchDataset: scan resource_acl: %v", err)
			}

			switch subjectType {
			case "user":
				// Direct manager edges become directManagerPairs.
				if relation == "manager" {
					directManagerPairs = append(directManagerPairs, benchPair{
						ResourceID: resID,
						UserID:     subjectID,
					})
				}
				// viewer_user edges are not needed for sampling; they are
				// still covered by the lookup queries themselves.

			case "group":
				// For viewer_group, we sample one member from the group.
				if relation == "viewer" {
					members := groupMembers[subjectID]
					if len(members) == 0 {
						continue
					}
					idx := groupSampleIndex[subjectID] % len(members)
					groupSampleIndex[subjectID]++
					userID := members[idx]

					groupViewPairs = append(groupViewPairs, benchPair{
						ResourceID: resID,
						UserID:     userID,
					})
				}
				// manager_group edges are handled in lookup queries, not needed here.

			default:
				// ignore unknown subject_type
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[cockroachdb] loadBenchDataset: rows resource_acl: %v", err)
		}
	}

	// 5) Build orgAdminPairs: one admin per resource (round-robin per org).
	adminRoundRobin := make(map[string]int)
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
	//    Behavior:
	//      - If env vars are set, they win.
	//      - Else choose random users from the sampled pairs.
	rand.Seed(time.Now().UnixNano())

	heavyManageUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	if heavyManageUser == "" && len(directManagerPairs) > 0 {
		idx := rand.Intn(len(directManagerPairs))
		heavyManageUser = directManagerPairs[idx].UserID
	}

	regularViewUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if regularViewUser == "" && len(groupViewPairs) > 0 {
		// Try to pick a different user from heavyManageUser if possible.
		candidates := make([]string, 0, len(groupViewPairs))
		for _, p := range groupViewPairs {
			if p.UserID == heavyManageUser {
				continue
			}
			candidates = append(candidates, p.UserID)
		}
		if len(candidates) == 0 {
			// Fall back to any user from groupViewPairs.
			idx := rand.Intn(len(groupViewPairs))
			regularViewUser = groupViewPairs[idx].UserID
		} else {
			idx := rand.Intn(len(candidates))
			regularViewUser = candidates[idx]
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
	log.Printf("[cockroachdb] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
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

///////////////////////////////
// Bench 1: direct manage   //
///////////////////////////////

func runCheckManageDirectUser(db *sql.DB, data *benchDataset) {
	pairs := data.directManagerPairs
	iters := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)
	// Use materialized view for fast permission checks (canonical relation 'manager')
	query := `
			SELECT 1
			FROM user_resource_permissions
			WHERE resource_id = $1
			  AND user_id = $2
			  AND relation = 'manager'
			LIMIT 1
		`
	runCheckBenchSQL(db, "check_manage_direct_user", query, pairs, iters, 2*time.Second)
}

// runCheckBenchSQL executes a prepared statement that checks a permission
// for many (resource,user) pairs and reports timing and allowed counts.
func runCheckBenchSQL(db *sql.DB, name, query string, pairs []benchPair, iters int, timeout time.Duration) {
	if len(pairs) == 0 {
		log.Printf("[cockroachdb] [%s] skipped: no sample pairs", name)
		return
	}

	log.Printf("[cockroachdb] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	prepCtx := context.Background()
	stmt, err := db.PrepareContext(prepCtx, query)
	if err != nil {
		log.Fatalf("[cockroachdb] [%s] prepare failed: %v", name, err)
	}
	defer stmt.Close()

	var total time.Duration
	allowedCount := 0

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()

		var dummy int
		err := stmt.QueryRowContext(ctx, pair.ResourceID, pair.UserID).Scan(&dummy)
		cancel()

		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[cockroachdb] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[cockroachdb] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

/////////////////////////////////////////
// Bench 2: manage via org admin      //
/////////////////////////////////////////

func runCheckManageOrgAdmin(db *sql.DB, data *benchDataset) {
	pairs := data.orgAdminPairs
	iters := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)
	query := `
			SELECT 1
			FROM resources r
			JOIN org_memberships om
			  ON r.org_id = om.org_id
			WHERE r.resource_id = $1
			  AND om.user_id = $2
			  AND om.role = 'admin'
			LIMIT 1
		`
	runCheckBenchSQL(db, "check_manage_org_admin", query, pairs, iters, 2*time.Second)
}

/////////////////////////////////////////
// Bench 3: view via group membership //
/////////////////////////////////////////

func runCheckViewViaGroupMember(db *sql.DB, data *benchDataset) {
	pairs := data.groupViewPairs
	iters := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)
	// Use materialized view for canonical viewer relation (includes group expansion)
	query := `
			SELECT 1
			FROM user_resource_permissions
			WHERE resource_id = $1
			  AND user_id = $2
			  AND relation = 'viewer'
			LIMIT 1
		`
	runCheckBenchSQL(db, "check_view_via_group_member", query, pairs, iters, 2*time.Second)
}

//////////////////////////////////////////////
// Bench 4: Lookup manage for heavy user    //
//////////////////////////////////////////////

func runLookupResourcesManageHeavyUser(db *sql.DB, data *benchDataset) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)

	userID := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	if userID == "" {
		userID = data.heavyManageUser
	}
	if userID == "" {
		log.Printf("[cockroachdb] [lookup_resources_manage_super] skipped: no heavyManageUser found")
		return
	}

	name := "lookup_resources_manage_super"
	// Use COUNT(DISTINCT ...) so DB does the aggregation and we avoid
	// transferring potentially large result sets back to the client.
	query := `
				SELECT COUNT(DISTINCT resource_id) FROM (
						-- org->admin (org admins manage all org resources)
						SELECT r.resource_id
						FROM resources r
						JOIN org_memberships om
							ON r.org_id = om.org_id
						WHERE om.user_id = $1
							AND om.role = 'admin'

						UNION

						-- manager_user
						SELECT ra.resource_id
						FROM resource_acl ra
						WHERE ra.subject_type = 'user'
							AND ra.subject_id = $1
							AND ra.relation = 'manager'

						UNION

						-- manager_group via group_memberships
						SELECT ra.resource_id
						FROM resource_acl ra
						JOIN group_memberships gm
							ON ra.subject_type = 'group'
						 AND ra.subject_id = gm.group_id
						WHERE gm.user_id = $1
							AND ra.relation = 'manager'
				) AS t;
		`

	runLookupBenchSQL(db, name, query, userID, iters)
}

//////////////////////////////////////////////
// Bench 5: Lookup view for regular user    //
//////////////////////////////////////////////

func runLookupResourcesViewRegularUser(db *sql.DB, data *benchDataset) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)

	userID := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if userID == "" {
		userID = data.regularViewUser
	}
	if userID == "" {
		log.Printf("[cockroachdb] [lookup_resources_view_regular] skipped: no regularViewUser found")
		return
	}

	name := "lookup_resources_view_regular"
	// Use COUNT(DISTINCT ...) to avoid transferring large result sets.
	query := `
				SELECT COUNT(DISTINCT resource_id) FROM (
						-- org->member (member_user + member_group + admin)
						--  a) direct org_memberships
						SELECT r.resource_id
						FROM resources r
						JOIN org_memberships om
							ON r.org_id = om.org_id
						WHERE om.user_id = $1

						UNION

						--  b) member_group: group memberships + groups(org_id)
						SELECT r.resource_id
						FROM resources r
						JOIN groups g
							ON g.org_id = r.org_id
						JOIN group_memberships gm
							ON gm.group_id = g.group_id
						WHERE gm.user_id = $1

						UNION

						-- viewer_user
						SELECT ra.resource_id
						FROM resource_acl ra
						WHERE ra.subject_type = 'user'
							AND ra.subject_id = $1
							AND ra.relation = 'viewer'

						UNION

						-- viewer_group via group_memberships
						SELECT ra.resource_id
						FROM resource_acl ra
						JOIN group_memberships gm
							ON ra.subject_type = 'group'
						 AND ra.subject_id = gm.group_id
						WHERE gm.user_id = $1
							AND ra.relation = 'viewer'

						UNION

						-- manage path: org->admin
						SELECT r.resource_id
						FROM resources r
						JOIN org_memberships om
							ON r.org_id = om.org_id
						WHERE om.user_id = $1
							AND om.role = 'admin'

						UNION

						-- manage path: manager_user
						SELECT ra.resource_id
						FROM resource_acl ra
						WHERE ra.subject_type = 'user'
							AND ra.subject_id = $1
							AND ra.relation = 'manager'

						UNION

						-- manage path: manager_group via group_memberships
						SELECT ra.resource_id
						FROM resource_acl ra
						JOIN group_memberships gm
							ON ra.subject_type = 'group'
						 AND ra.subject_id = gm.group_id
						WHERE gm.user_id = $1
							AND ra.relation = 'manager'
				) AS t;
		`

	runLookupBenchSQL(db, name, query, userID, iters)
}

// runLookupBenchSQL prepares and runs a COUNT(*) style lookup query for a user
// across multiple iterations, logging per-iteration durations and overall stats.
func runLookupBenchSQL(db *sql.DB, name, query, userID string, iters int) {
	log.Printf("[cockroachdb] [%s] iterations=%d user=%s", name, iters, userID)

	prepCtx := context.Background()
	stmt, err := db.PrepareContext(prepCtx, query)
	if err != nil {
		log.Fatalf("[cockroachdb] [%s] prepare count query failed: %v", name, err)
	}
	defer stmt.Close()

	var total time.Duration
	var lastCount int

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		var count int
		if err := stmt.QueryRowContext(ctx, userID).Scan(&count); err != nil {
			cancel()
			log.Fatalf("[cockroachdb] [%s] count query failed: %v", name, err)
		}
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[cockroachdb] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[cockroachdb] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}
