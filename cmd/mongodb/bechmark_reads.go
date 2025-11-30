package mongodb

import (
	"context"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"test-tls/infrastructure"
	"test-tls/utils"
)

// Streaming-only benchmarks for MongoDB using denormalized collections defined
// in create_schemas.go. No in-memory accumulation; all checks use cursors.
func MongodbBenchmarkReads() {
	client, db, cleanup, err := infrastructure.NewMongoFromEnv(context.Background())
	if err != nil {
		log.Fatalf("[mongodb] failed to create mongo client: %v", err)
	}
	defer cleanup()

	start := time.Now()
	heavyManageUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	regularViewUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[mongodb] Running in streaming-only mode (no precollection). elapsed=%s heavyManageUser=%q regularViewUser=%q",
		elapsed, heavyManageUser, regularViewUser)

	runCheckManageDirectUser(db)
	runCheckManageOrgAdmin(db)
	runCheckViewViaGroupMember(db)
	runLookupResourcesManageHeavyUser(db)
	runLookupResourcesViewRegularUser(db)

	log.Println("[mongodb] == Mongo read benchmarks DONE ==")
	_ = client
}

// === Helpers ===
func findOneString(ctx context.Context, coll *mongo.Collection, filter interface{}, field string) (string, error) {
	// Project single field to avoid large docs
	opts := options.FindOne().SetProjection(bson.D{{Key: field, Value: 1}})
	var doc bson.M
	err := coll.FindOne(ctx, filter, opts).Decode(&doc)
	if err != nil {
		return "", err
	}
	v, _ := doc[field].(string)
	return v, nil
}

func streamCursor(ctx context.Context, cur *mongo.Cursor, handle func(bson.M)) error {
	for cur.Next(ctx) {
		var m bson.M
		if err := cur.Decode(&m); err != nil {
			return err
		}
		handle(m)
	}
	return cur.Err()
}

// === Scenarios ===

// Direct manager_user relationship checks: stream resources with manager_user_ids entries
func runCheckManageDirectUser(db *mongo.Database) {
	iters := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)
	log.Printf("[mongodb] [check_manage_direct_user] streaming mode. iterations=%d", iters)

	coll := db.Collection("resources")
	ctx := context.Background()
	done := 0

	// Stream resources having at least one manager_user_ids element
	cur, err := coll.Find(ctx, bson.D{{Key: "manager_user_ids", Value: bson.D{{Key: "$exists", Value: true}}}}, options.Find().SetProjection(bson.D{{Key: "resource_id", Value: 1}, {Key: "manager_user_ids", Value: 1}}))
	if err != nil {
		log.Fatalf("[mongodb] [check_manage_direct_user] query failed: %v", err)
	}
	defer cur.Close(ctx)

	_ = streamCursor(ctx, cur, func(m bson.M) {
		if done >= iters {
			return
		}
		resID, _ := m["resource_id"].(string)
		users, _ := m["manager_user_ids"].(bson.A)
		if len(users) == 0 {
			return
		}
		userID, _ := users[0].(string)

		// Simulate CheckPermission: existence check for (resID, userID)
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		findErr := db.Collection("resources").FindOne(cctx, bson.D{{Key: "resource_id", Value: resID}, {Key: "manager_user_ids", Value: userID}}).Err()
		cancel()
		if findErr != nil {
			// Not found implies permission false; keep streaming
			return
		}
		dur := time.Since(start)
		if done%100 == 0 {
			log.Printf("[mongodb] [check_manage_direct_user] iter=%d resource=%s user=%s dur=%s", done, resID, userID, dur)
		}
		done++
	})

	log.Printf("[mongodb] [check_manage_direct_user] DONE: iters=%d", iters)
}

// Manage via org admin: stream resources' org_id and pick an admin
func runCheckManageOrgAdmin(db *mongo.Database) {
	iters := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)
	log.Printf("[mongodb] [check_manage_org_admin] streaming mode. iterations=%d", iters)

	rcoll := db.Collection("resources")
	ocoll := db.Collection("organizations")
	ctx := context.Background()
	done := 0

	cur, err := rcoll.Find(ctx, bson.D{}, options.Find().SetProjection(bson.D{{Key: "resource_id", Value: 1}, {Key: "org_id", Value: 1}}))
	if err != nil {
		log.Fatalf("[mongodb] [check_manage_org_admin] query failed: %v", err)
	}
	defer cur.Close(ctx)

	_ = streamCursor(ctx, cur, func(m bson.M) {
		if done >= iters {
			return
		}
		resID, _ := m["resource_id"].(string)
		orgID, _ := m["org_id"].(string)

		// Find an admin user for the org without caching
		aCur, err := ocoll.Find(ctx, bson.D{{Key: "org_id", Value: orgID}, {Key: "admin_user_ids", Value: bson.D{{Key: "$exists", Value: true}}}}, options.Find().SetProjection(bson.D{{Key: "admin_user_ids", Value: 1}}))
		if err != nil {
			return
		}
		defer aCur.Close(ctx)
		var adminUser string
		for aCur.Next(ctx) {
			var om bson.M
			if err := aCur.Decode(&om); err != nil {
				break
			}
			arr, _ := om["admin_user_ids"].(bson.A)
			if len(arr) > 0 {
				adminUser, _ = arr[0].(string)
				break
			}
		}
		if adminUser == "" {
			return
		}

		// Simulate CheckPermission via org admin path: resource.org matches org where user is admin
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		err = rcoll.FindOne(cctx, bson.D{{Key: "resource_id", Value: resID}, {Key: "org_id", Value: orgID}}).Err()
		cancel()
		if err != nil {
			return
		}
		dur := time.Since(start)
		if done%100 == 0 {
			log.Printf("[mongodb] [check_manage_org_admin] iter=%d resource=%s org=%s admin=%s dur=%s", done, resID, orgID, adminUser, dur)
		}
		done++
	})

	log.Printf("[mongodb] [check_manage_org_admin] DONE: iters=%d", iters)
}

// View via viewer_group and group membership
func runCheckViewViaGroupMember(db *mongo.Database) {
	iters := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)
	log.Printf("[mongodb] [check_view_via_group_member] streaming mode. iterations=%d", iters)

	rcoll := db.Collection("resources")
	gcoll := db.Collection("groups")
	ctx := context.Background()
	done := 0

	// Stream resources that reference some viewer_group_ids
	cur, err := rcoll.Find(ctx, bson.D{{Key: "viewer_group_ids", Value: bson.D{{Key: "$exists", Value: true}}}}, options.Find().SetProjection(bson.D{{Key: "resource_id", Value: 1}, {Key: "viewer_group_ids", Value: 1}}))
	if err != nil {
		log.Fatalf("[mongodb] [check_view_via_group_member] query failed: %v", err)
	}
	defer cur.Close(ctx)

	_ = streamCursor(ctx, cur, func(m bson.M) {
		if done >= iters {
			return
		}
		resID, _ := m["resource_id"].(string)
		groups, _ := m["viewer_group_ids"].(bson.A)
		if len(groups) == 0 {
			return
		}
		groupID, _ := groups[0].(string)

		// Pick a direct member; fallback to manager
		var pickedUser string
		gCur, err := gcoll.Find(ctx, bson.D{{Key: "group_id", Value: groupID}}, options.Find().SetProjection(bson.D{{Key: "direct_member_user_ids", Value: 1}, {Key: "direct_manager_user_ids", Value: 1}}))
		if err != nil {
			return
		}
		defer gCur.Close(ctx)
		for gCur.Next(ctx) {
			var gm bson.M
			if err := gCur.Decode(&gm); err != nil {
				break
			}
			arr, _ := gm["direct_member_user_ids"].(bson.A)
			if len(arr) > 0 {
				pickedUser, _ = arr[0].(string)
				break
			}
			arr2, _ := gm["direct_manager_user_ids"].(bson.A)
			if pickedUser == "" && len(arr2) > 0 {
				pickedUser, _ = arr2[0].(string)
			}
		}
		if pickedUser == "" {
			return
		}

		// Simulate CheckPermission: ensure resource has group and group contains user
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		start := time.Now()
		// Check resource references the group
		if err := rcoll.FindOne(cctx, bson.D{{Key: "resource_id", Value: resID}, {Key: "viewer_group_ids", Value: groupID}}).Err(); err != nil {
			cancel()
			return
		}
		// Check group membership
		if err := gcoll.FindOne(cctx, bson.D{{Key: "group_id", Value: groupID}, {Key: "$or", Value: bson.A{
			bson.D{{Key: "direct_member_user_ids", Value: pickedUser}},
			bson.D{{Key: "direct_manager_user_ids", Value: pickedUser}},
		}}}).Err(); err != nil {
			cancel()
			return
		}
		cancel()
		dur := time.Since(start)
		if done%100 == 0 {
			log.Printf("[mongodb] [check_view_via_group_member] iter=%d resource=%s group=%s user=%s dur=%s", done, resID, groupID, pickedUser, dur)
		}
		done++
	})

	log.Printf("[mongodb] [check_view_via_group_member] DONE: iters=%d", iters)
}

// Lookup resources for manage for a heavy user
func runLookupResourcesManageHeavyUser(db *mongo.Database) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	runLookupBench(db, "lookup_resources_manage_super", "manage", userID, iters, 60*time.Second)
}

// Lookup resources for view for a regular user
func runLookupResourcesViewRegularUser(db *mongo.Database) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	runLookupBench(db, "lookup_resources_view_regular", "view", userID, iters, 60*time.Second)
}

// runLookupBench streams matching resources for a user and counts them.
func runLookupBench(db *mongo.Database, name, permission, userID string, iters int, timeout time.Duration) {
	if userID == "" {
		log.Printf("[mongodb] [%s] skipped: no user specified", name)
		return
	}
	log.Printf("[mongodb] [%s] iterations=%d user=%s", name, iters, userID)

	rcoll := db.Collection("resources")
	gcoll := db.Collection("groups")
	ocoll := db.Collection("organizations")

	var total time.Duration
	var lastCount int

	for i := 0; i < iters; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()

		// Stream resources by combining direct and derived paths without precollecting:
		// manage: direct user, org admin, manager group
		// view: direct user, viewer group (member/manager), or manage
		var filter bson.D
		if permission == "manage" {
			filter = bson.D{{Key: "$or", Value: bson.A{
				bson.D{{Key: "manager_user_ids", Value: userID}},
				// org admin path handled by streaming: match later inside loop
				// manager group: resources referencing any group where user is manager
			}}}
		} else {
			filter = bson.D{{Key: "$or", Value: bson.A{
				bson.D{{Key: "viewer_user_ids", Value: userID}},
				// viewer_group handled by group membership streaming
			}}}
		}

		cur, err := rcoll.Find(ctx, filter, options.Find().SetProjection(bson.D{{Key: "resource_id", Value: 1}, {Key: "org_id", Value: 1}, {Key: "manager_group_ids", Value: 1}, {Key: "viewer_group_ids", Value: 1}}))
		if err != nil {
			cancel()
			log.Fatalf("[mongodb] [%s] query failed: %v", name, err)
		}

		count := 0
		for cur.Next(ctx) {
			var m bson.M
			if err := cur.Decode(&m); err != nil {
				break
			}
			resID, _ := m["resource_id"].(string)
			orgID, _ := m["org_id"].(string)

			// Derived paths
			match := false
			if permission == "manage" {
				// org admin path
				if err := ocoll.FindOne(ctx, bson.D{{Key: "org_id", Value: orgID}, {Key: "admin_user_ids", Value: userID}}).Err(); err == nil {
					match = true
				}
				// manager group path
				if !match {
					groups, _ := m["manager_group_ids"].(bson.A)
					for _, g := range groups {
						gid, _ := g.(string)
						if gid == "" {
							continue
						}
						if err := gcoll.FindOne(ctx, bson.D{{Key: "group_id", Value: gid}, {Key: "direct_manager_user_ids", Value: userID}}).Err(); err == nil {
							match = true
							break
						}
					}
				}
			} else { // view
				// viewer_group path via member or manager
				groups, _ := m["viewer_group_ids"].(bson.A)
				for _, g := range groups {
					gid, _ := g.(string)
					if gid == "" {
						continue
					}
					if err := gcoll.FindOne(ctx, bson.D{{Key: "group_id", Value: gid}, {Key: "$or", Value: bson.A{
						bson.D{{Key: "direct_member_user_ids", Value: userID}},
						bson.D{{Key: "direct_manager_user_ids", Value: userID}},
					}}}).Err(); err == nil {
						match = true
						break
					}
				}
				// manage implies view
				if !match {
					if err := rcoll.FindOne(ctx, bson.D{{Key: "resource_id", Value: resID}, {Key: "manager_user_ids", Value: userID}}).Err(); err == nil {
						match = true
					}
				}
				if !match {
					if err := ocoll.FindOne(ctx, bson.D{{Key: "org_id", Value: orgID}, {Key: "admin_user_ids", Value: userID}}).Err(); err == nil {
						match = true
					}
				}
			}

			if match {
				count++
			}
		}
		cur.Close(ctx)
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count
		log.Printf("[mongodb] [%s] iter=%d resources=%d duration=%s", name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[mongodb] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s", name, iters, lastCount, avg, total)
}
