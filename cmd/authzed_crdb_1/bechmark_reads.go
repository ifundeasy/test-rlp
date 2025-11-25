package authzed_crdb_1

import (
	"context"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	authzed "github.com/authzed/authzed-go/v1"

	"test-tls/infrastructure"
)

// benchPair couples a resource with a user that should have a given permission.
type benchPair struct {
	ResourceID string
	UserID     string
}

// benchDataset holds precomputed samples and heavy users for the benchmarks.
type benchDataset struct {
	directManagerPairs []benchPair // from resource.manager_user@user
	orgAdminPairs      []benchPair // resource + org admin user
	groupViewPairs     []benchPair // resource + user via viewer_group + group membership

	heavyManageUser string // user with many "manage" resources
	regularViewUser string // user with many "view" resources (preferably not heavyManageUser)
}

// AuthzedBenchmarkReads runs several read benchmarks against the current dataset.
func AuthzedBenchmarkReads() {
	client, _, cancel, err := infrastructure.NewAuthzedCrdbClientFromEnv(context.Background())
	if err != nil {
		log.Fatalf("[authzed_crdb_1] failed to create authzed client: %v", err)
	}
	defer cancel()
	defer client.Close()

	log.Println("[authzed_crdb_1] == Building benchmark dataset from SpiceDB ==")
	data := loadBenchDataset(client)
	log.Println("[authzed_crdb_1] == Running Authzed read benchmarks on SpiceDB dataset ==")

	runCheckManageDirectUser(client, data)   // direct manager_user in resource_acl
	runCheckManageOrgAdmin(client, data)     // org->admin path
	runCheckViewViaGroupMember(client, data) // via viewer_group + group membership
	runLookupResourcesManageHeavyUser(client, data)
	runLookupResourcesViewRegularUser(client, data)

	log.Println("[authzed_crdb_1] == Authzed read benchmarks DONE ==")
}

// =========================
// Dataset loading from SpiceDB
// =========================

func loadBenchDataset(client *authzed.Client) *benchDataset {
	start := time.Now()
	ctx := context.Background()

	// org-level
	orgAdmins := make(map[string][]string)  // org_id -> []user_id (admin_user)
	orgMembers := make(map[string][]string) // org_id -> []user_id (member_user + admin_user)

	// groups & memberships
	groupMembers := make(map[string][]string) // group_id -> []user_id (member+admin)
	groupOrg := make(map[string]string)       // group_id -> org_id (via organization#member_group)

	// resources
	resOrg := make(map[string]string) // resource_id -> org_id
	orgResourceCount := make(map[string]int)
	var resourceIDs []string

	// user permission "weight"
	manageCount := make(map[string]int) // approx number of manageable resources
	viewCount := make(map[string]int)   // approx number of viewable resources

	// 1) organization#admin_user@user  -> orgAdmins + orgMembers
	readRels := func(filter *v1.RelationshipFilter, handle func(rel *v1.Relationship)) {
		stream, err := client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
			RelationshipFilter: filter,
		})
		if err != nil {
			log.Fatalf("[authzed_crdb_1] ReadRelationships failed: %v", err)
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[authzed_crdb_1] ReadRelationships stream recv failed: %v", err)
			}
			handle(resp.Relationship)
		}
	}

	// org admin_user
	readRels(&v1.RelationshipFilter{
		ResourceType:     "organization",
		OptionalRelation: "admin_user",
	}, func(rel *v1.Relationship) {
		orgID := rel.Resource.ObjectId
		userID := rel.Subject.Object.ObjectId
		orgAdmins[orgID] = append(orgAdmins[orgID], userID)
		orgMembers[orgID] = append(orgMembers[orgID], userID)
	})

	// org member_user
	readRels(&v1.RelationshipFilter{
		ResourceType:     "organization",
		OptionalRelation: "member_user",
	}, func(rel *v1.Relationship) {
		orgID := rel.Resource.ObjectId
		userID := rel.Subject.Object.ObjectId
		orgMembers[orgID] = append(orgMembers[orgID], userID)
	})

	// 2) usergroup memberships: usergroup.{member_user,admin_user}@user  -> groupMembers
	readRels(&v1.RelationshipFilter{
		ResourceType:     "usergroup",
		OptionalRelation: "member_user",
	}, func(rel *v1.Relationship) {
		groupID := rel.Resource.ObjectId
		userID := rel.Subject.Object.ObjectId
		groupMembers[groupID] = append(groupMembers[groupID], userID)
	})
	readRels(&v1.RelationshipFilter{
		ResourceType:     "usergroup",
		OptionalRelation: "admin_user",
	}, func(rel *v1.Relationship) {
		groupID := rel.Resource.ObjectId
		userID := rel.Subject.Object.ObjectId
		groupMembers[groupID] = append(groupMembers[groupID], userID)
	})

	// 3) organization.member_group@usergroup#member -> groupOrg
	readRels(&v1.RelationshipFilter{
		ResourceType:     "organization",
		OptionalRelation: "member_group",
	}, func(rel *v1.Relationship) {
		orgID := rel.Resource.ObjectId
		groupID := rel.Subject.Object.ObjectId
		groupOrg[groupID] = orgID
	})

	// 4) resource.org@organization -> resOrg + orgResourceCount + resourceIDs
	readRels(&v1.RelationshipFilter{
		ResourceType:     "resource",
		OptionalRelation: "org",
	}, func(rel *v1.Relationship) {
		resID := rel.Resource.ObjectId
		orgID := rel.Subject.Object.ObjectId
		resOrg[resID] = orgID
		orgResourceCount[orgID]++
		resourceIDs = append(resourceIDs, resID)
	})

	// 5) org-level contributions:
	//    manage: org.admin
	//    view: org.member (member_user + member_group + admin)
	for orgID, admins := range orgAdmins {
		cnt := orgResourceCount[orgID]
		if cnt == 0 {
			continue
		}
		for _, u := range admins {
			manageCount[u] += cnt
			viewCount[u] += cnt // admins are also members -> view via org.member
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
	for groupID, members := range groupMembers {
		orgID, ok := groupOrg[groupID]
		if !ok {
			continue
		}
		cnt := orgResourceCount[orgID]
		if cnt == 0 {
			continue
		}
		// org.member_group: all members of the group become organization members
		for _, u := range members {
			viewCount[u] += cnt
		}
	}

	// 6) resource ACL contributions
	var directManagerPairs []benchPair
	var groupViewPairs []benchPair
	groupSampleIndex := make(map[string]int)

	// resource.manager_user@user
	readRels(&v1.RelationshipFilter{
		ResourceType:     "resource",
		OptionalRelation: "manager_user",
	}, func(rel *v1.Relationship) {
		resID := rel.Resource.ObjectId
		userID := rel.Subject.Object.ObjectId

		directManagerPairs = append(directManagerPairs, benchPair{
			ResourceID: resID,
			UserID:     userID,
		})
		manageCount[userID]++
	})

	// resource.viewer_user@user
	readRels(&v1.RelationshipFilter{
		ResourceType:     "resource",
		OptionalRelation: "viewer_user",
	}, func(rel *v1.Relationship) {
		userID := rel.Subject.Object.ObjectId
		viewCount[userID]++
	})

	// resource.manager_group@usergroup#admin -> all members of the group manage
	readRels(&v1.RelationshipFilter{
		ResourceType:     "resource",
		OptionalRelation: "manager_group",
	}, func(rel *v1.Relationship) {
		resID := rel.Resource.ObjectId
		_ = resID // we only need per-user manageCount
		groupID := rel.Subject.Object.ObjectId

		members := groupMembers[groupID]
		for _, u := range members {
			manageCount[u]++
		}
	})

	// resource.viewer_group@usergroup#member -> all members of the group view
	readRels(&v1.RelationshipFilter{
		ResourceType:     "resource",
		OptionalRelation: "viewer_group",
	}, func(rel *v1.Relationship) {
		resID := rel.Resource.ObjectId
		groupID := rel.Subject.Object.ObjectId

		members := groupMembers[groupID]
		if len(members) == 0 {
			return
		}
		for _, u := range members {
			viewCount[u]++
		}

		// sample one user per (group,resource) for the "view via group member" benchmark
		idx := groupSampleIndex[groupID] % len(members)
		groupSampleIndex[groupID]++
		userID := members[idx]

		groupViewPairs = append(groupViewPairs, benchPair{
			ResourceID: resID,
			UserID:     userID,
		})
	})

	// 7) propagate manage -> view (schema: view includes manage)
	for userID, mc := range manageCount {
		viewCount[userID] += mc
	}

	// 8) Build orgAdminPairs: satu admin per resource (round-robin per org)
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

	// 9) Pick heavyManageUser & regularViewUser
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

	// 10) Allow override via env (shared with lookup_resources_* benchmarks)
	if v := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER"); v != "" {
		heavyManageUser = v
	}
	if v := os.Getenv("BENCH_LOOKUPRES_VIEW_USER"); v != "" {
		regularViewUser = v
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[authzed_crdb_1] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
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

func runCheckManageDirectUser(client *authzed.Client, data *benchDataset) {
	pairs := data.directManagerPairs
	if len(pairs) == 0 {
		log.Printf("[authzed_crdb_1] [check_manage_direct_user] skipped: no direct manager_user ACL entries")
		return
	}

	iters := getEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)

	name := "check_manage_direct_user"
	log.Printf("[authzed_crdb_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]
		resID := pair.ResourceID
		userID := pair.UserID

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		resp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
			Resource: &v1.ObjectReference{
				ObjectType: "resource",
				ObjectId:   resID,
			},
			Permission: "manage",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: "user",
					ObjectId:   userID,
				},
			},
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{
					FullyConsistent: true,
				},
			},
		})
		cancel()
		if err != nil {
			log.Fatalf("[authzed_crdb_1] [%s] CheckPermission failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if resp.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[authzed_crdb_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 2: Check "manage" via org admin (org->admin)
// ===============================

func runCheckManageOrgAdmin(client *authzed.Client, data *benchDataset) {
	pairs := data.orgAdminPairs
	if len(pairs) == 0 {
		log.Printf("[authzed_crdb_1] [check_manage_org_admin] skipped: no org admin + resource pairs")
		return
	}

	iters := getEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)

	name := "check_manage_org_admin"
	log.Printf("[authzed_crdb_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]
		resID := pair.ResourceID
		userID := pair.UserID

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		resp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
			Resource: &v1.ObjectReference{
				ObjectType: "resource",
				ObjectId:   resID,
			},
			Permission: "manage",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: "user",
					ObjectId:   userID,
				},
			},
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{
					FullyConsistent: true,
				},
			},
		})
		cancel()
		if err != nil {
			log.Fatalf("[authzed_crdb_1] [%s] CheckPermission failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if resp.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[authzed_crdb_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 3: Check "view" via group membership
// ===============================

func runCheckViewViaGroupMember(client *authzed.Client, data *benchDataset) {
	pairs := data.groupViewPairs
	if len(pairs) == 0 {
		log.Printf("[authzed_crdb_1] [check_view_via_group_member] skipped: no viewer_group-based sample pairs")
		return
	}

	iters := getEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)

	name := "check_view_via_group_member"
	log.Printf("[authzed_crdb_1] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]
		resID := pair.ResourceID
		userID := pair.UserID

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		resp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
			Resource: &v1.ObjectReference{
				ObjectType: "resource",
				ObjectId:   resID,
			},
			Permission: "view",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: "user",
					ObjectId:   userID,
				},
			},
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{
					FullyConsistent: true,
				},
			},
		})
		cancel()
		if err != nil {
			log.Fatalf("[authzed_crdb_1] [%s] CheckPermission failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if resp.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[authzed_crdb_1] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// ===============================
// Bench 4: LookupResources "manage" for a heavy user
// ===============================

func runLookupResourcesManageHeavyUser(client *authzed.Client, data *benchDataset) {
	iters := getEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)

	userID := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	if userID == "" {
		userID = data.heavyManageUser
	}
	if userID == "" {
		log.Printf("[authzed_crdb_1] [lookup_resources_manage_super] skipped: no heavyManageUser found")
		return
	}

	name := "lookup_resources_manage_super"
	log.Printf("[authzed_crdb_1] [%s] iterations=%d user=%s", name, iters, userID)

	var total time.Duration
	var lastCount int

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
			ResourceObjectType: "resource",
			Permission:         "manage",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: "user",
					ObjectId:   userID,
				},
			},
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{
					FullyConsistent: true,
				},
			},
		})
		if err != nil {
			cancel()
			log.Fatalf("[authzed_crdb_1] [%s] LookupResources failed: %v", name, err)
		}

		count := 0
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cancel()
				log.Fatalf("[authzed_crdb_1] [%s] stream Recv failed: %v", name, err)
			}
			_ = res.ResourceObjectId
			count++
		}
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[authzed_crdb_1] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[authzed_crdb_1] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}

// ===============================
// Bench 5: LookupResources "view" for a regular-ish user
// ===============================

func runLookupResourcesViewRegularUser(client *authzed.Client, data *benchDataset) {
	iters := getEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)

	userID := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if userID == "" {
		userID = data.regularViewUser
	}
	if userID == "" {
		log.Printf("[authzed_crdb_1] [lookup_resources_view_regular] skipped: no regularViewUser found")
		return
	}

	name := "lookup_resources_view_regular"
	log.Printf("[authzed_crdb_1] [%s] iterations=%d user=%s", name, iters, userID)

	var total time.Duration
	var lastCount int

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		start := time.Now()

		stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
			ResourceObjectType: "resource",
			Permission:         "view",
			Subject: &v1.SubjectReference{
				Object: &v1.ObjectReference{
					ObjectType: "user",
					ObjectId:   userID,
				},
			},
			Consistency: &v1.Consistency{
				Requirement: &v1.Consistency_FullyConsistent{
					FullyConsistent: true,
				},
			},
		})
		if err != nil {
			cancel()
			log.Fatalf("[authzed_crdb_1] [%s] LookupResources failed: %v", name, err)
		}

		count := 0
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cancel()
				log.Fatalf("[authzed_crdb_1] [%s] stream Recv failed: %v", name, err)
			}
			_ = res.ResourceObjectId
			count++
		}
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[authzed_crdb_1] [%s] iter=%d resources=%d duration=%s",
			name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[authzed_crdb_1] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
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
