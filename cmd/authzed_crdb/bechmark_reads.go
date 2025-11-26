package authzed_crdb

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
		log.Fatalf("[authzed_crdb] failed to create authzed client: %v", err)
	}
	defer cancel()
	defer client.Close()

	log.Println("[authzed_crdb] == Building benchmark dataset from SpiceDB ==")
	data := loadBenchDataset(client)
	log.Println("[authzed_crdb] == Running Authzed read benchmarks on SpiceDB dataset ==")

	runCheckManageDirectUser(client, data)   // direct manager_user in resource_acl
	runCheckManageOrgAdmin(client, data)     // org->admin path
	runCheckViewViaGroupMember(client, data) // via viewer_group + group membership
	runLookupResourcesManageHeavyUser(client, data)
	runLookupResourcesViewRegularUser(client, data)

	log.Println("[authzed_crdb] == Authzed read benchmarks DONE ==")
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
			log.Fatalf("[authzed_crdb] ReadRelationships failed: %v", err)
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[authzed_crdb] ReadRelationships stream recv failed: %v", err)
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
		OptionalRelation: "direct_member_user",
	}, func(rel *v1.Relationship) {
		groupID := rel.Resource.ObjectId
		userID := rel.Subject.Object.ObjectId
		groupMembers[groupID] = append(groupMembers[groupID], userID)
	})
	readRels(&v1.RelationshipFilter{
		ResourceType:     "usergroup",
		OptionalRelation: "direct_manager_user",
	}, func(rel *v1.Relationship) {
		groupID := rel.Resource.ObjectId
		userID := rel.Subject.Object.ObjectId
		groupMembers[groupID] = append(groupMembers[groupID], userID)
	})

	// 2b) group hierarchy for Schema #3: usergroup.{member_group,manager_group}@usergroup
	// build parent->child maps so we can compute transitive (effective) group membership
	memberChildren := make(map[string][]string) // parent -> []child
	managerChildren := make(map[string][]string)
	readRels(&v1.RelationshipFilter{
		ResourceType:     "usergroup",
		OptionalRelation: "member_group",
	}, func(rel *v1.Relationship) {
		parent := rel.Resource.ObjectId
		child := rel.Subject.Object.ObjectId
		memberChildren[parent] = append(memberChildren[parent], child)
	})
	readRels(&v1.RelationshipFilter{
		ResourceType:     "usergroup",
		OptionalRelation: "manager_group",
	}, func(rel *v1.Relationship) {
		parent := rel.Resource.ObjectId
		child := rel.Subject.Object.ObjectId
		managerChildren[parent] = append(managerChildren[parent], child)
	})

	// compute effective members for each group (direct members + transitive children members)
	computeEffectiveMembers := func(direct map[string][]string, children map[string][]string) map[string][]string {
		eff := make(map[string][]string)
		// memoization to avoid recomputing
		visited := make(map[string]map[string]struct{})

		var dfs func(g string) map[string]struct{}
		dfs = func(g string) map[string]struct{} {
			if s, ok := visited[g]; ok {
				return s
			}
			set := make(map[string]struct{})
			// add direct members
			for _, u := range direct[g] {
				set[u] = struct{}{}
			}
			// traverse children
			for _, c := range children[g] {
				for u := range dfs(c) {
					set[u] = struct{}{}
				}
			}
			visited[g] = set
			return set
		}

		// run for all known groups
		for g := range direct {
			set := dfs(g)
			if len(set) == 0 {
				eff[g] = nil
				continue
			}
			list := make([]string, 0, len(set))
			for u := range set {
				list = append(list, u)
			}
			eff[g] = list
		}
		// also ensure parents that only appear in children map are included
		for g := range children {
			if _, ok := eff[g]; ok {
				continue
			}
			set := dfs(g)
			list := make([]string, 0, len(set))
			for u := range set {
				list = append(list, u)
			}
			eff[g] = list
		}

		return eff
	}

	groupEffectiveMembers := computeEffectiveMembers(groupMembers, memberChildren)
	groupEffectiveManagers := computeEffectiveMembers(groupMembers, managerChildren)

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
	for groupID := range groupOrg {
		orgID := groupOrg[groupID]
		cnt := orgResourceCount[orgID]
		if cnt == 0 {
			continue
		}
		members := groupEffectiveMembers[groupID]
		if len(members) == 0 {
			members = groupMembers[groupID]
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

	// resource.manager_group@usergroup -> all effective managers of the group manage
	readRels(&v1.RelationshipFilter{
		ResourceType:     "resource",
		OptionalRelation: "manager_group",
	}, func(rel *v1.Relationship) {
		resID := rel.Resource.ObjectId
		_ = resID // we only need per-user manageCount
		groupID := rel.Subject.Object.ObjectId

		// prefer effective managers (transitive), fall back to direct members
		members := groupEffectiveManagers[groupID]
		if len(members) == 0 {
			members = groupMembers[groupID]
		}
		for _, u := range members {
			manageCount[u]++
		}
	})

	// resource.viewer_group@usergroup -> all effective members of the group view
	readRels(&v1.RelationshipFilter{
		ResourceType:     "resource",
		OptionalRelation: "viewer_group",
	}, func(rel *v1.Relationship) {
		resID := rel.Resource.ObjectId
		groupID := rel.Subject.Object.ObjectId
		members := groupEffectiveMembers[groupID]
		if len(members) == 0 {
			members = groupMembers[groupID]
		}
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
	log.Printf("[authzed_crdb] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
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

// runCheckBench is a small harness that executes many CheckPermission calls
// against the given sample pairs and reports simple statistics.
func runCheckBench(client *authzed.Client, name, permission string, pairs []benchPair, iters int, timeout time.Duration) {
	if len(pairs) == 0 {
		log.Printf("[authzed_crdb] [%s] skipped: no sample pairs", name)
		return
	}

	log.Printf("[authzed_crdb] [%s] iterations=%d samplePairs=%d", name, iters, len(pairs))

	var total time.Duration
	allowedCount := 0

	for i := 0; i < iters; i++ {
		pair := pairs[i%len(pairs)]
		resID := pair.ResourceID
		userID := pair.UserID

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()
		resp, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
			Resource: &v1.ObjectReference{
				ObjectType: "resource",
				ObjectId:   resID,
			},
			Permission: permission,
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
			log.Fatalf("[authzed_crdb] [%s] CheckPermission failed: %v", name, err)
		}
		dur := time.Since(start)
		total += dur

		if resp.Permissionship == v1.CheckPermissionResponse_PERMISSIONSHIP_HAS_PERMISSION {
			allowedCount++
		}
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[authzed_crdb] [%s] DONE: iters=%d allowed=%d avg=%s total=%s",
		name, iters, allowedCount, avg, total)
}

// runLookupBench runs LookupResources for the given user and permission,
// counting returned resources and reporting simple timing metrics.
func runLookupBench(client *authzed.Client, name, permission, userID string, iters int, timeout time.Duration) {
	if userID == "" {
		log.Printf("[authzed_crdb] [%s] skipped: no user specified", name)
		return
	}

	log.Printf("[authzed_crdb] [%s] iterations=%d user=%s", name, iters, userID)

	var total time.Duration
	var lastCount int

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()

		stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
			ResourceObjectType: "resource",
			Permission:         permission,
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
			log.Fatalf("[authzed_crdb] [%s] LookupResources failed: %v", name, err)
		}

		count := 0
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cancel()
				log.Fatalf("[authzed_crdb] [%s] stream Recv failed: %v", name, err)
			}
			count++
		}
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[authzed_crdb] [%s] iter=%d resources=%d duration=%s", name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[authzed_crdb] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}

func runCheckManageDirectUser(client *authzed.Client, data *benchDataset) {
	pairs := data.directManagerPairs
	iters := getEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)
	runCheckBench(client, "check_manage_direct_user", "manage", pairs, iters, 2*time.Second)
}

// ===============================
// Bench 2: Check "manage" via org admin (org->admin)
// ===============================

func runCheckManageOrgAdmin(client *authzed.Client, data *benchDataset) {
	pairs := data.orgAdminPairs
	iters := getEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)
	runCheckBench(client, "check_manage_org_admin", "manage", pairs, iters, 2*time.Second)
}

// ===============================
// Bench 3: Check "view" via group membership
// ===============================

func runCheckViewViaGroupMember(client *authzed.Client, data *benchDataset) {
	pairs := data.groupViewPairs
	iters := getEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)
	runCheckBench(client, "check_view_via_group_member", "view", pairs, iters, 2*time.Second)
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
	runLookupBench(client, "lookup_resources_manage_super", "manage", userID, iters, 60*time.Second)
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
	runLookupBench(client, "lookup_resources_view_regular", "view", userID, iters, 60*time.Second)
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
