package postgres_1

import (
	"context"
	"database/sql"
	"log"
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

	heavyManageUser string // user with many "manage" resources
	regularViewUser string // user with many "view" resources (preferably not heavyManageUser)
}

// PostgresBenchmarkReads runs several read benchmarks against the current dataset.
func PostgresBenchmarkReads() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	db, cleanup, err := infrastructure.NewPostgresFromEnv(ctx)
	if err != nil {
		log.Fatalf("[postgres_1] benchmark: connect failed: %v", err)
	}
	defer cleanup()

	log.Println("[postgres_1] == Building benchmark dataset from Postgres ==")
	data := loadBenchDataset(db)
	log.Println("[postgres_1] == Running Postgres read benchmarks on DB-backed dataset ==")

	runCheckManageDirectUser(db, data)   // direct manager ACL
	runCheckManageOrgAdmin(db, data)     // org admin path
	runCheckViewViaGroupMember(db, data) // via viewer_group + group membership
	runLookupResourcesManageHeavyUser(db, data)
	runLookupResourcesViewRegularUser(db, data)

	log.Println("[postgres_1] == Postgres read benchmarks DONE ==")
}

///////////////////////////////
// Dataset loading from DB   //
///////////////////////////////

func loadBenchDataset(db *sql.DB) *benchDataset {
	start := time.Now()
	ctx := context.Background()

	// org-level
	orgAdmins := make(map[string][]string)  // org_id -> []user_id (role=admin)
	orgMembers := make(map[string][]string) // org_id -> []user_id (all roles)

	// groups & memberships
	groupMembers := make(map[string][]string) // group_id -> []user_id
	groupOrg := make(map[string]string)       // group_id -> org_id

	// resources
	resOrg := make(map[string]string) // resource_id -> org_id
	orgResourceCount := make(map[string]int)
	var resourceIDs []string

	// user permission "weight"
	manageCount := make(map[string]int) // how many resources user can manage
	viewCount := make(map[string]int)   // how many resources user can view

	// 1) org_memberships: org_id,user_id,role
	{
		rows, err := db.QueryContext(ctx, `
			SELECT org_id, user_id, role
			FROM org_memberships
		`)
		if err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: query org_memberships: %v", err)
		}
		for rows.Next() {
			var orgID, userID, role string
			if err := rows.Scan(&orgID, &userID, &role); err != nil {
				rows.Close()
				log.Fatalf("[postgres_1] loadBenchDataset: scan org_memberships: %v", err)
			}
			orgMembers[orgID] = append(orgMembers[orgID], userID)
			if role == "admin" {
				orgAdmins[orgID] = append(orgAdmins[orgID], userID)
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: rows org_memberships: %v", err)
		}
		rows.Close()
	}

	// 2) group_memberships: group_id,user_id,role
	{
		rows, err := db.QueryContext(ctx, `
			SELECT group_id, user_id, role
			FROM group_memberships
		`)
		if err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: query group_memberships: %v", err)
		}
		for rows.Next() {
			var groupID, userID, role string
			if err := rows.Scan(&groupID, &userID, &role); err != nil {
				rows.Close()
				log.Fatalf("[postgres_1] loadBenchDataset: scan group_memberships: %v", err)
			}
			_ = role // member/admin -> both counted via group membership
			groupMembers[groupID] = append(groupMembers[groupID], userID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: rows group_memberships: %v", err)
		}
		rows.Close()
	}

	// 3) groups: group_id,org_id -> org.member_group (in schema) via usergroup#member
	{
		rows, err := db.QueryContext(ctx, `
			SELECT group_id, org_id
			FROM groups
		`)
		if err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: query groups: %v", err)
		}
		for rows.Next() {
			var groupID, orgID string
			if err := rows.Scan(&groupID, &orgID); err != nil {
				rows.Close()
				log.Fatalf("[postgres_1] loadBenchDataset: scan groups: %v", err)
			}
			groupOrg[groupID] = orgID
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: rows groups: %v", err)
		}
		rows.Close()
	}

	// 4) resources: resource_id,org_id
	{
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, org_id
			FROM resources
		`)
		if err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: query resources: %v", err)
		}
		for rows.Next() {
			var resID, orgID string
			if err := rows.Scan(&resID, &orgID); err != nil {
				rows.Close()
				log.Fatalf("[postgres_1] loadBenchDataset: scan resources: %v", err)
			}
			resOrg[resID] = orgID
			orgResourceCount[orgID]++
			resourceIDs = append(resourceIDs, resID)
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: rows resources: %v", err)
		}
		rows.Close()
	}

	// 5) org->admin contributes to manage (manage += all resources in org)
	for orgID, admins := range orgAdmins {
		cnt := orgResourceCount[orgID]
		if cnt == 0 {
			continue
		}
		for _, u := range admins {
			manageCount[u] += cnt
		}
	}

	// 6) org->member contributes to view:
	// org.member = member_user + member_group + admin
	// - member_user/admin_user: org_members
	// - member_group: groupOrg + groupMembers
	for orgID, members := range orgMembers {
		cnt := orgResourceCount[orgID]
		if cnt == 0 {
			continue
		}
		for _, u := range members {
			viewCount[u] += cnt
		}
	}
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

	// 7) resource_acl -> manage & view & sample pairs
	var directManagerPairs []benchPair
	var groupViewPairs []benchPair
	groupSampleIndex := make(map[string]int)

	{
		rows, err := db.QueryContext(ctx, `
			SELECT resource_id, subject_type, subject_id, relation
			FROM resource_acl
		`)
		if err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: query resource_acl: %v", err)
		}
		for rows.Next() {
			var resID, subjectType, subjectID, relation string
			if err := rows.Scan(&resID, &subjectType, &subjectID, &relation); err != nil {
				rows.Close()
				log.Fatalf("[postgres_1] loadBenchDataset: scan resource_acl: %v", err)
			}

			switch subjectType {
			case "user":
				switch relation {
				case "manager":
					// resource.manager_user
					directManagerPairs = append(directManagerPairs, benchPair{
						ResourceID: resID,
						UserID:     subjectID,
					})
					manageCount[subjectID]++
				case "viewer":
					// resource.viewer_user
					viewCount[subjectID]++
				default:
					// ignore
				}

			case "group":
				members := groupMembers[subjectID]
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
					// one sample per (group,resource) for bench view-via-group
					idx := groupSampleIndex[subjectID] % len(members)
					groupSampleIndex[subjectID]++
					userID := members[idx]
					groupViewPairs = append(groupViewPairs, benchPair{
						ResourceID: resID,
						UserID:     userID,
					})
				default:
					// ignore
				}
			default:
				// ignore
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatalf("[postgres_1] loadBenchDataset: rows resource_acl: %v", err)
		}
		rows.Close()
	}

	// 8) propagate manage -> view (schema: view includes manage)
	for userID, mc := range manageCount {
		viewCount[userID] += mc
	}

	// 9) Build orgAdminPairs: one admin per resource
	var orgAdminPairs []benchPair
	adminRoundRobin := make(map[string]int)

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

	// 10) heavyManageUser & regularViewUser
	var heavyManageUser string
	maxManage := 0
	for userID, c := range manageCount {
		if c > maxManage {
			maxManage = c
			heavyManageUser = userID
		}
	}

	var regularViewUser string
	maxView := 0
	for userID, c := range viewCount {
		if userID == heavyManageUser {
			continue
		}
		if c > maxView {
			maxView = c
			regularViewUser = userID
		}
	}
	if regularViewUser == "" {
		regularViewUser = heavyManageUser
	}

	// Allow override via env
	if v := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER"); v != "" {
		heavyManageUser = v
	}
	if v := os.Getenv("BENCH_LOOKUPRES_VIEW_USER"); v != "" {
		regularViewUser = v
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[postgres_1] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
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
	if len(pairs) == 0 {
		log.Printf("[postgres_1] [check_manage_direct_user] skipped: no direct manager ACL entries")
		return
	}

	iters := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)

	name := "check_manage_direct_user"
	log.Printf("[postgres_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	// prepare statement once to avoid repeated parse/plan overhead
	prepCtx := context.Background()
	stmt, err := db.PrepareContext(prepCtx, `
			SELECT 1
			FROM resource_acl
			WHERE resource_id = $1
			  AND subject_type = 'user'
			  AND subject_id = $2
			  AND relation = 'manager'
			LIMIT 1
		`)
	if err != nil {
		log.Fatalf("[postgres_1] [%s] prepare failed: %v", name, err)
	}
	defer stmt.Close()

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()

		var dummy int
		err := stmt.QueryRowContext(ctx, pair.ResourceID, pair.UserID).Scan(&dummy)
		cancel()

		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[postgres_1] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[postgres_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

/////////////////////////////////////////
// Bench 2: manage via org admin      //
/////////////////////////////////////////

func runCheckManageOrgAdmin(db *sql.DB, data *benchDataset) {
	pairs := data.orgAdminPairs
	if len(pairs) == 0 {
		log.Printf("[postgres_1] [check_manage_org_admin] skipped: no org admin + resource pairs")
		return
	}

	iters := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)

	name := "check_manage_org_admin"
	log.Printf("[postgres_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	// prepare once
	prepCtx := context.Background()
	stmt, err := db.PrepareContext(prepCtx, `
			SELECT 1
			FROM resources r
			JOIN org_memberships om
			  ON r.org_id = om.org_id
			WHERE r.resource_id = $1
			  AND om.user_id = $2
			  AND om.role = 'admin'
			LIMIT 1
		`)
	if err != nil {
		log.Fatalf("[postgres_1] [%s] prepare failed: %v", name, err)
	}
	defer stmt.Close()

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()

		var dummy int
		err := stmt.QueryRowContext(ctx, pair.ResourceID, pair.UserID).Scan(&dummy)
		cancel()

		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[postgres_1] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[postgres_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

/////////////////////////////////////////
// Bench 3: view via group membership //
/////////////////////////////////////////

func runCheckViewViaGroupMember(db *sql.DB, data *benchDataset) {
	pairs := data.groupViewPairs
	if len(pairs) == 0 {
		log.Printf("[postgres_1] [check_view_via_group_member] skipped: no group-based viewer pairs")
		return
	}

	iters := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)

	name := "check_view_via_group_member"
	log.Printf("[postgres_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	// prepare once
	prepCtx := context.Background()
	stmt, err := db.PrepareContext(prepCtx, `
			SELECT 1
			FROM resource_acl ra
			JOIN group_memberships gm
			  ON ra.subject_type = 'group'
			 AND ra.subject_id = gm.group_id
			WHERE ra.resource_id = $1
			  AND ra.relation = 'viewer'
			  AND gm.user_id = $2
			LIMIT 1
		`)
	if err != nil {
		log.Fatalf("[postgres_1] [%s] prepare failed: %v", name, err)
	}
	defer stmt.Close()

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()

		var dummy int
		err := stmt.QueryRowContext(ctx, pair.ResourceID, pair.UserID).Scan(&dummy)
		cancel()

		if err != nil && err != sql.ErrNoRows {
			log.Fatalf("[postgres_1] [%s] query failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if err == nil {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[postgres_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
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
		log.Printf("[postgres_1] [lookup_resources_manage_super] skipped: no heavyManageUser found")
		return
	}

	name := "lookup_resources_manage_super"
	log.Printf("[postgres_1] [%s] iterations=%d user=%s", name, iters, userID)

	var total time.Duration
	var lastCount int

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

	prepCtx := context.Background()
	stmt, err := db.PrepareContext(prepCtx, query)
	if err != nil {
		log.Fatalf("[postgres_1] [%s] prepare count query failed: %v", name, err)
	}
	defer stmt.Close()

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		var count int
		if err := stmt.QueryRowContext(ctx, userID).Scan(&count); err != nil {
			cancel()
			log.Fatalf("[postgres_1] [%s] count query failed: %v", name, err)
		}
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[postgres_1] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[postgres_1] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
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
		log.Printf("[postgres_1] [lookup_resources_view_regular] skipped: no regularViewUser found")
		return
	}

	name := "lookup_resources_view_regular"
	log.Printf("[postgres_1] [%s] iterations=%d user=%s", name, iters, userID)

	var total time.Duration
	var lastCount int

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

	prepCtx := context.Background()
	stmt, err := db.PrepareContext(prepCtx, query)
	if err != nil {
		log.Fatalf("[postgres_1] [%s] prepare count query failed: %v", name, err)
	}
	defer stmt.Close()

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		var count int
		if err := stmt.QueryRowContext(ctx, userID).Scan(&count); err != nil {
			cancel()
			log.Fatalf("[postgres_1] [%s] count query failed: %v", name, err)
		}
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[postgres_1] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[postgres_1] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}
