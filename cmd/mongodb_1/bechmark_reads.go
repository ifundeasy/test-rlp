package mongodb_1

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"test-tls/infrastructure"
	"test-tls/utils"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// benchPair holds one (resource, user) pair for sampling.
type benchPair struct {
	ResourceID string
	UserID     string
}

// benchDataset holds all precomputed data for the benchmarks.
type benchDataset struct {
	directManagerPairs []benchPair
	orgAdminPairs      []benchPair
	groupViewPairs     []benchPair

	heavyManageUser string
	regularViewUser string
}

// MongoBenchmarkReads is the entry point: called from cmd/main.go
// when you run `go run cmd/main.go mongodb_1 benchmark`.
func MongodbBenchmarkReads() {
	ctx := context.Background()

	_, db, cleanup, err := infrastructure.NewMongoFromEnv(ctx)
	if err != nil {
		log.Fatalf("[mongodb_1] create mongo client: %v", err)
	}
	defer cleanup()

	log.Printf("[mongodb_1] == Building benchmark dataset from MongoDB ==")
	data, err := buildBenchDatasetFromMongo(ctx, db)
	if err != nil {
		log.Fatalf("[mongodb_1] build benchmark dataset: %v", err)
	}

	log.Printf("[mongodb_1] == Running MongoDB read benchmarks on DB-backed dataset ==")
	runCheckManageDirectUser(ctx, db, data)
	runCheckManageOrgAdmin(ctx, db, data)
	runCheckViewViaGroupMember(ctx, db, data)
	runLookupResourcesManageSuper(ctx, db, data)
	runLookupResourcesViewRegular(ctx, db, data)
	log.Printf("[mongodb_1] == MongoDB read benchmarks DONE ==")
}

// ==============================
// Dataset builder from MongoDB
// ==============================

type orgMembershipDoc struct {
	OrgID  string `bson:"org_id"`
	UserID string `bson:"user_id"`
	Role   string `bson:"role"`
}

type groupMembershipDoc struct {
	GroupID string `bson:"group_id"`
	UserID  string `bson:"user_id"`
	Role    string `bson:"role"`
}

type resourceDoc struct {
	ResourceID string `bson:"_id"` // FIX: use _id, not resource_id
	OrgID      string `bson:"org_id"`
}

type resourceACLDoc struct {
	ResourceID  string `bson:"resource_id"`
	SubjectType string `bson:"subject_type"` // "user" or "group"
	SubjectID   string `bson:"subject_id"`
	Relation    string `bson:"relation"` // "manager" or "viewer"
}

func buildBenchDatasetFromMongo(ctx context.Context, db *mongo.Database) (*benchDataset, error) {
	start := time.Now()

	// ---- 1) Load org admins ----
	orgAdmins := make(map[string][]string) // org_id -> []admin user_id

	omCur, err := db.Collection("org_memberships").Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer omCur.Close(ctx)

	for omCur.Next(ctx) {
		var om orgMembershipDoc
		if err := omCur.Decode(&om); err != nil {
			return nil, err
		}
		if om.Role == "admin" {
			orgAdmins[om.OrgID] = append(orgAdmins[om.OrgID], om.UserID)
		}
	}
	if err := omCur.Err(); err != nil {
		return nil, err
	}

	// ---- 2) Load group memberships ----
	groupMembers := make(map[string][]string) // group_id -> []user_id

	gmCur, err := db.Collection("group_memberships").Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer gmCur.Close(ctx)

	for gmCur.Next(ctx) {
		var gm groupMembershipDoc
		if err := gmCur.Decode(&gm); err != nil {
			return nil, err
		}
		groupMembers[gm.GroupID] = append(groupMembers[gm.GroupID], gm.UserID)
	}
	if err := gmCur.Err(); err != nil {
		return nil, err
	}

	// ---- 3) Load resources (for orgAdminPairs) ----
	resourceIDs := make([]string, 0, 1024)
	resOrg := make(map[string]string) // resource_id -> org_id

	resCur, err := db.Collection("resources").Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer resCur.Close(ctx)

	for resCur.Next(ctx) {
		var r resourceDoc
		if err := resCur.Decode(&r); err != nil {
			return nil, err
		}
		resourceIDs = append(resourceIDs, r.ResourceID)
		resOrg[r.ResourceID] = r.OrgID
	}
	if err := resCur.Err(); err != nil {
		return nil, err
	}

	// ---- 4) Scan resource_acl to build pairs + counts ----
	directManagerPairs := make([]benchPair, 0, 1024)
	groupViewPairs := make([]benchPair, 0, 1024)

	manageCount := make(map[string]int) // user_id -> #manageable resources (all paths)
	viewCount := make(map[string]int)   // user_id -> #viewable resources (all paths)

	groupSampleIndex := make(map[string]int) // group_id -> round-robin index

	raclCur, err := db.Collection("resource_acl").Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer raclCur.Close(ctx)

	for raclCur.Next(ctx) {
		var ra resourceACLDoc
		if err := raclCur.Decode(&ra); err != nil {
			return nil, err
		}
		switch ra.SubjectType {
		case "user":
			switch ra.Relation {
			case "manager":
				directManagerPairs = append(directManagerPairs, benchPair{
					ResourceID: ra.ResourceID,
					UserID:     ra.SubjectID,
				})
				manageCount[ra.SubjectID]++
			case "viewer":
				viewCount[ra.SubjectID]++
			default:
				// ignore unknown relation
			}

		case "group":
			members := groupMembers[ra.SubjectID]
			if len(members) == 0 {
				continue
			}
			switch ra.Relation {
			case "manager":
				for _, uid := range members {
					manageCount[uid]++
				}
			case "viewer":
				for _, uid := range members {
					viewCount[uid]++
				}
				// For group view benchmark, sample one user per (resource, group) via round-robin.
				idx := groupSampleIndex[ra.SubjectID] % len(members)
				groupSampleIndex[ra.SubjectID]++
				userID := members[idx]
				groupViewPairs = append(groupViewPairs, benchPair{
					ResourceID: ra.ResourceID,
					UserID:     userID,
				})
			default:
				// ignore
			}
		default:
			// ignore unknown subject_type
		}
	}
	if err := raclCur.Err(); err != nil {
		return nil, err
	}

	// ---- 5) Build orgAdminPairs (one admin per resource via round-robin) ----
	orgAdminPairs := make([]benchPair, 0, len(resourceIDs))
	adminIdx := make(map[string]int) // org_id -> round-robin index

	for _, resID := range resourceIDs {
		orgID := resOrg[resID]
		admins := orgAdmins[orgID]
		if len(admins) == 0 {
			continue
		}
		idx := adminIdx[orgID] % len(admins)
		adminIdx[orgID]++
		uid := admins[idx]

		orgAdminPairs = append(orgAdminPairs, benchPair{
			ResourceID: resID,
			UserID:     uid,
		})
		// org admin can manage resources -> count as manage
		manageCount[uid]++
	}

	// ---- 6) Determine heavyManageUser & regularViewUser (with env override) ----
	heavy, regular := pickLookupUsers(manageCount, viewCount)

	elapsed := time.Since(start).Truncate(time.Second)
	log.Printf("[mongodb_1] Benchmark dataset loaded in %s: directManagerPairs=%d orgAdminPairs=%d groupViewPairs=%d heavyManageUser=%q regularViewUser=%q",
		elapsed, len(directManagerPairs), len(orgAdminPairs), len(groupViewPairs), heavy, regular)

	return &benchDataset{
		directManagerPairs: directManagerPairs,
		orgAdminPairs:      orgAdminPairs,
		groupViewPairs:     groupViewPairs,
		heavyManageUser:    heavy,
		regularViewUser:    regular,
	}, nil
}

func pickLookupUsers(manageCount, viewCount map[string]int) (string, string) {
	// Heavy manage user
	envManage := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	if envManage == "" {
		envManage = maxCountKey(manageCount, "")
	}
	// Regular view user
	envView := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if envView == "" {
		envView = maxCountKey(viewCount, envManage)
	}
	if envView == "" {
		envView = envManage
	}
	return envManage, envView
}

func maxCountKey(counts map[string]int, exclude string) string {
	var bestKey string
	bestVal := -1
	for k, v := range counts {
		if k == exclude {
			continue
		}
		if v > bestVal {
			bestVal = v
			bestKey = k
		}
	}
	return bestKey
}

// =====================
// Bench 1: direct user
// =====================

func runCheckManageDirectUser(parent context.Context, db *mongo.Database, data *benchDataset) {
	iters := envInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)
	pairs := data.directManagerPairs
	if len(pairs) == 0 {
		log.Printf("[mongodb_1] [check_manage_direct_user] SKIP: no directManagerPairs in dataset")
		return
	}

	log.Printf("[mongodb_1] [check_manage_direct_user] iterations=%d samplePairs=%d", iters, len(pairs))

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	allowed := 0

	coll := db.Collection("resource_acl")

	for i := 0; i < iters; i++ {
		p := pairs[rnd.Intn(len(pairs))]

		ctx, cancel := context.WithTimeout(parent, 2*time.Second)
		count, err := coll.CountDocuments(ctx, bson.M{
			"resource_id":  p.ResourceID,
			"subject_type": "user",
			"subject_id":   p.UserID,
			"relation":     "manager",
		})
		cancel()
		if err != nil {
			log.Fatalf("[mongodb_1] [check_manage_direct_user] query error: %v", err)
		}
		if count == 0 {
			log.Fatalf("[mongodb_1] [check_manage_direct_user] unexpected deny for pair (resource=%s,user=%s)", p.ResourceID, p.UserID)
		}
		allowed++
	}

	total := time.Since(start)
	avg := total / time.Duration(iters)
	log.Printf("[mongodb_1] [check_manage_direct_user] DONE: iters=%d allowed=%d avg=%s total=%s",
		iters, allowed, avg, total)
}

// ==========================
// Bench 2: org admin manage
// ==========================

func runCheckManageOrgAdmin(parent context.Context, db *mongo.Database, data *benchDataset) {
	iters := envInt("BENCH_CHECK_ORGADMIN_ITER", 1000)
	pairs := data.orgAdminPairs
	if len(pairs) == 0 {
		log.Printf("[mongodb_1] [check_manage_org_admin] SKIP: no orgAdminPairs in dataset")
		return
	}

	log.Printf("[mongodb_1] [check_manage_org_admin] iterations=%d samplePairs=%d", iters, len(pairs))

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	allowed := 0

	resColl := db.Collection("resources")
	orgMemberships := db.Collection("org_memberships")

	for i := 0; i < iters; i++ {
		p := pairs[rnd.Intn(len(pairs))]

		ctx, cancel := context.WithTimeout(parent, 2*time.Second)

		// 1) Fetch org_id of resource
		var r resourceDoc
		// err := resColl.FindOne(ctx, bson.M{"resource_id": p.ResourceID}).Decode(&r)
		err := resColl.FindOne(ctx, bson.M{"_id": p.ResourceID}).Decode(&r)
		if err != nil {
			cancel()
			if err == mongo.ErrNoDocuments {
				log.Fatalf("[mongodb_1] [check_manage_org_admin] resource not found: %s", p.ResourceID)
			}
			log.Fatalf("[mongodb_1] [check_manage_org_admin] find resource error: %v", err)
		}

		// 2) Check if user is admin of that org
		count, err := orgMemberships.CountDocuments(ctx, bson.M{
			"org_id":  r.OrgID,
			"user_id": p.UserID,
			"role":    "admin",
		})
		cancel()
		if err != nil {
			log.Fatalf("[mongodb_1] [check_manage_org_admin] query error: %v", err)
		}
		if count == 0 {
			log.Fatalf("[mongodb_1] [check_manage_org_admin] unexpected deny for pair (resource=%s,user=%s,org=%s)", p.ResourceID, p.UserID, r.OrgID)
		}
		allowed++
	}

	total := time.Since(start)
	avg := total / time.Duration(iters)
	log.Printf("[mongodb_1] [check_manage_org_admin] DONE: iters=%d allowed=%d avg=%s total=%s",
		iters, allowed, avg, total)
}

// ======================================
// Bench 3: view via group membership
// ======================================

func runCheckViewViaGroupMember(parent context.Context, db *mongo.Database, data *benchDataset) {
	iters := envInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)
	pairs := data.groupViewPairs
	if len(pairs) == 0 {
		log.Printf("[mongodb_1] [check_view_via_group_member] SKIP: no groupViewPairs in dataset")
		return
	}

	log.Printf("[mongodb_1] [check_view_via_group_member] iterations=%d samplePairs=%d", iters, len(pairs))

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	start := time.Now()
	allowed := 0

	groupMemberships := db.Collection("group_memberships")
	resourceACL := db.Collection("resource_acl")

	for i := 0; i < iters; i++ {
		p := pairs[rnd.Intn(len(pairs))]

		ctx, cancel := context.WithTimeout(parent, 2*time.Second)

		// 1) Find groups where user is a member
		gCur, err := groupMemberships.Find(ctx, bson.M{"user_id": p.UserID})
		if err != nil {
			cancel()
			log.Fatalf("[mongodb_1] [check_view_via_group_member] find groups error: %v", err)
		}

		groupIDs := make([]string, 0, 8)
		for gCur.Next(ctx) {
			var gm groupMembershipDoc
			if err := gCur.Decode(&gm); err != nil {
				gCur.Close(ctx)
				cancel()
				log.Fatalf("[mongodb_1] [check_view_via_group_member] decode group_membership error: %v", err)
			}
			groupIDs = append(groupIDs, gm.GroupID)
		}
		gCur.Close(ctx)
		if err := gCur.Err(); err != nil {
			cancel()
			log.Fatalf("[mongodb_1] [check_view_via_group_member] cursor error: %v", err)
		}

		if len(groupIDs) == 0 {
			cancel()
			log.Fatalf("[mongodb_1] [check_view_via_group_member] user=%s has no groups (expected at least one)", p.UserID)
		}

		// 2) Check if any of those groups has viewer ACL on the resource
		count, err := resourceACL.CountDocuments(ctx, bson.M{
			"resource_id":  p.ResourceID,
			"subject_type": "group",
			"relation":     "viewer",
			"subject_id":   bson.M{"$in": groupIDs},
		})
		cancel()
		if err != nil {
			log.Fatalf("[mongodb_1] [check_view_via_group_member] query error: %v", err)
		}
		if count == 0 {
			log.Fatalf("[mongodb_1] [check_view_via_group_member] unexpected deny for pair (resource=%s,user=%s)", p.ResourceID, p.UserID)
		}
		allowed++
	}

	total := time.Since(start)
	avg := total / time.Duration(iters)
	log.Printf("[mongodb_1] [check_view_via_group_member] DONE: iters=%d allowed=%d avg=%s total=%s",
		iters, allowed, avg, total)
}

// ==================================
// Bench 4: lookup manage resources
// ==================================

func runLookupResourcesManageSuper(parent context.Context, db *mongo.Database, data *benchDataset) {
	userID := data.heavyManageUser
	iters := envInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)

	log.Printf("[mongodb_1] [lookup_resources_manage_super] iterations=%d user=%s", iters, userID)

	totalDur := time.Duration(0)
	lastCount := 0

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(parent, 10*time.Second)
		resIDs, err := mongoLookupManageResourcesForUser(ctx, db, userID)
		cancel()
		if err != nil {
			log.Fatalf("[mongodb_1] [lookup_resources_manage_super] lookup error: %v", err)
		}
		lastCount = len(resIDs)
		dur := time.Duration(0)
		if len(resIDs) > 0 {
			// dur is basically the time between context creation and cancel.
			// measure by re-running with a timer:
			// but we already have measure above; so just approximate via totalDur.
		}
		// To measure a real duration, do the lookup inside the timing window:
		start := time.Now()
		ctx2, cancel2 := context.WithTimeout(parent, 10*time.Second)
		_, err = mongoLookupManageResourcesForUser(ctx2, db, userID)
		cancel2()
		if err != nil {
			log.Fatalf("[mongodb_1] [lookup_resources_manage_super] timing lookup error: %v", err)
		}
		dur = time.Since(start)

		totalDur += dur
		log.Printf("[mongodb_1] [lookup_resources_manage_super] iter=%d resources=%d duration=%s", i, lastCount, dur)
	}

	avg := time.Duration(0)
	if iters > 0 {
		avg = totalDur / time.Duration(iters)
	}
	log.Printf("[mongodb_1] [lookup_resources_manage_super] DONE: iters=%d lastCount=%d avg=%s total=%s",
		iters, lastCount, avg, totalDur)
}

func mongoLookupManageResourcesForUser(ctx context.Context, db *mongo.Database, userID string) ([]string, error) {
	resourceACL := db.Collection("resource_acl")
	orgMemberships := db.Collection("org_memberships")
	groupMemberships := db.Collection("group_memberships")
	resources := db.Collection("resources")

	resultSet := make(map[string]struct{})

	// 1) Direct manager ACLs
	rCur, err := resourceACL.Find(ctx, bson.M{
		"subject_type": "user",
		"subject_id":   userID,
		"relation":     "manager",
	})
	if err != nil {
		return nil, err
	}
	for rCur.Next(ctx) {
		var ra resourceACLDoc
		if err := rCur.Decode(&ra); err != nil {
			rCur.Close(ctx)
			return nil, err
		}
		resultSet[ra.ResourceID] = struct{}{}
	}
	rCur.Close(ctx)
	if err := rCur.Err(); err != nil {
		return nil, err
	}

	// 2) Group-based manager ACLs
	gCur, err := groupMemberships.Find(ctx, bson.M{"user_id": userID})
	if err != nil {
		return nil, err
	}
	groupIDs := make([]string, 0, 8)
	for gCur.Next(ctx) {
		var gm groupMembershipDoc
		if err := gCur.Decode(&gm); err != nil {
			gCur.Close(ctx)
			return nil, err
		}
		groupIDs = append(groupIDs, gm.GroupID)
	}
	gCur.Close(ctx)
	if err := gCur.Err(); err != nil {
		return nil, err
	}
	if len(groupIDs) > 0 {
		rCur2, err := resourceACL.Find(ctx, bson.M{
			"subject_type": "group",
			"relation":     "manager",
			"subject_id":   bson.M{"$in": groupIDs},
		})
		if err != nil {
			return nil, err
		}
		for rCur2.Next(ctx) {
			var ra resourceACLDoc
			if err := rCur2.Decode(&ra); err != nil {
				rCur2.Close(ctx)
				return nil, err
			}
			resultSet[ra.ResourceID] = struct{}{}
		}
		rCur2.Close(ctx)
		if err := rCur2.Err(); err != nil {
			return nil, err
		}
	}

	// 3) Org admin path
	omCur, err := orgMemberships.Find(ctx, bson.M{
		"user_id": userID,
		"role":    "admin",
	})
	if err != nil {
		return nil, err
	}
	adminOrgs := make([]string, 0, 4)
	for omCur.Next(ctx) {
		var om orgMembershipDoc
		if err := omCur.Decode(&om); err != nil {
			omCur.Close(ctx)
			return nil, err
		}
		adminOrgs = append(adminOrgs, om.OrgID)
	}
	omCur.Close(ctx)
	if err := omCur.Err(); err != nil {
		return nil, err
	}
	if len(adminOrgs) > 0 {
		resCur, err := resources.Find(ctx, bson.M{
			"org_id": bson.M{"$in": adminOrgs},
		})
		if err != nil {
			return nil, err
		}
		for resCur.Next(ctx) {
			var r resourceDoc
			if err := resCur.Decode(&r); err != nil {
				resCur.Close(ctx)
				return nil, err
			}
			resultSet[r.ResourceID] = struct{}{}
		}
		resCur.Close(ctx)
		if err := resCur.Err(); err != nil {
			return nil, err
		}
	}

	out := make([]string, 0, len(resultSet))
	for rid := range resultSet {
		out = append(out, rid)
	}
	return out, nil
}

// ================================
// Bench 5: lookup view resources
// ================================

// distinctStrings returns a slice of strings from the result of a Distinct call on a field.
func distinctStrings(ctx context.Context, coll *mongo.Collection, field string, filter interface{}) ([]string, error) {
	values, err := coll.Distinct(ctx, field, filter)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(values))
	for _, v := range values {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out, nil
}

// ------------------------------
// 5) Lookup "view" for a regular user
// ------------------------------
//
// Semantics aligned with Postgres:
//   - direct viewers:      resources.viewers contains the userID
//   - direct managers:     resources.managers contains the userID
//   - org member/admin:    org_memberships (org_id,user_id,role) ->
//     all resources with that org_id are viewable
//   - group member/admin:  group_memberships (group_id,user_id,role) +
//     resources.viewer_groups / manager_groups
//
// The result should be roughly 1:1 with Postgres (same resource count for the same user).
func runLookupResourcesViewRegular(parent context.Context, db *mongo.Database, data *benchDataset) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)

	// Use env if present, otherwise use the sampled dataset result.
	userID := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	if userID == "" {
		userID = data.regularViewUser
	}

	log.Printf("[mongodb_1] [lookup_resources_view_regular] iterations=%d user=%s", iters, userID)

	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()

	orgMemberships := db.Collection("org_memberships")
	groupMemberships := db.Collection("group_memberships")
	resourcesColl := db.Collection("resources")

	// 1) Find all orgs where this user is a member/admin.
	orgIDs, err := distinctStrings(ctx, orgMemberships, "org_id", bson.M{
		"user_id": userID,
	})
	if err != nil {
		log.Fatalf("[mongodb_1] [lookup_resources_view_regular] load orgIDs: %v", err)
	}

	// 2) Find all groups where this user is a member/admin.
	groupIDs, err := distinctStrings(ctx, groupMemberships, "group_id", bson.M{
		"user_id": userID,
	})
	if err != nil {
		log.Fatalf("[mongodb_1] [lookup_resources_view_regular] load groupIDs: %v", err)
	}

	// 3) Build a resources filter that matches the Postgres semantics.
	orClauses := []bson.M{
		// Direct viewer
		{"viewers": userID},
		// Direct manager (manager ⇒ can view)
		{"managers": userID},
	}

	if len(orgIDs) > 0 {
		orClauses = append(orClauses, bson.M{
			"org_id": bson.M{"$in": orgIDs},
		})
	}

	if len(groupIDs) > 0 {
		orClauses = append(orClauses,
			bson.M{"viewer_groups": bson.M{"$in": groupIDs}},
			bson.M{"manager_groups": bson.M{"$in": groupIDs}},
		)
	}

	filter := bson.M{"$or": orClauses}

	total := time.Duration(0)
	lastCount := 0

	for i := 0; i < iters; i++ {
		iterCtx, iterCancel := context.WithTimeout(parent, 15*time.Second)
		start := time.Now()

		cur, err := resourcesColl.Find(
			iterCtx,
			filter,
			options.Find().SetProjection(bson.M{"_id": 1}),
		)
		if err != nil {
			iterCancel()
			log.Fatalf("[mongodb_1] [lookup_resources_view_regular] Find error: %v", err)
		}

		count := 0
		for cur.Next(iterCtx) {
			count++
		}
		if err := cur.Err(); err != nil {
			iterCancel()
			log.Fatalf("[mongodb_1] [lookup_resources_view_regular] cursor error: %v", err)
		}
		cur.Close(iterCtx)
		iterCancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[mongodb_1] [lookup_resources_view_regular] iter=%d resources=%d duration=%s",
			i, count, dur.Truncate(time.Microsecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[mongodb_1] [lookup_resources_view_regular] DONE: iters=%d lastCount=%d avg=%s total=%s",
		iters, lastCount, avg, total)
}

func mongoLookupViewResourcesForUser(ctx context.Context, db *mongo.Database, userID string) ([]string, error) {
	resourceACL := db.Collection("resource_acl")
	groupMemberships := db.Collection("group_memberships")

	resultSet := make(map[string]struct{})

	// 1) Direct viewer ACLs
	rCur, err := resourceACL.Find(ctx, bson.M{
		"subject_type": "user",
		"subject_id":   userID,
		"relation":     "viewer",
	})
	if err != nil {
		return nil, err
	}
	for rCur.Next(ctx) {
		var ra resourceACLDoc
		if err := rCur.Decode(&ra); err != nil {
			rCur.Close(ctx)
			return nil, err
		}
		resultSet[ra.ResourceID] = struct{}{}
	}
	rCur.Close(ctx)
	if err := rCur.Err(); err != nil {
		return nil, err
	}

	// 2) Viewer via groups
	gCur, err := groupMemberships.Find(ctx, bson.M{"user_id": userID})
	if err != nil {
		return nil, err
	}
	groupIDs := make([]string, 0, 8)
	for gCur.Next(ctx) {
		var gm groupMembershipDoc
		if err := gCur.Decode(&gm); err != nil {
			gCur.Close(ctx)
			return nil, err
		}
		groupIDs = append(groupIDs, gm.GroupID)
	}
	gCur.Close(ctx)
	if err := gCur.Err(); err != nil {
		return nil, err
	}

	if len(groupIDs) > 0 {
		rCur2, err := resourceACL.Find(ctx, bson.M{
			"subject_type": "group",
			"relation":     "viewer",
			"subject_id":   bson.M{"$in": groupIDs},
		})
		if err != nil {
			return nil, err
		}
		for rCur2.Next(ctx) {
			var ra resourceACLDoc
			if err := rCur2.Decode(&ra); err != nil {
				rCur2.Close(ctx)
				return nil, err
			}
			resultSet[ra.ResourceID] = struct{}{}
		}
		rCur2.Close(ctx)
		if err := rCur2.Err(); err != nil {
			return nil, err
		}
	}

	out := make([]string, 0, len(resultSet))
	for rid := range resultSet {
		out = append(out, rid)
	}
	return out, nil
}

// ====================
// Env helper
// ====================

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("[mongodb_1] invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}
