package authzed_pgdb

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	authzed "github.com/authzed/authzed-go/v1"

	"test-tls/infrastructure"
	"test-tls/utils"
)

// This file runs read benchmarks against SpiceDB (Authzed) in streaming-only
// mode. It does not precompute or retain relationships in memory — all
// relationships are streamed and checked on-demand to avoid any in-memory
// collection. LookupResources benchmarks require explicit user IDs via env
// variables `BENCH_LOOKUPRES_MANAGE_USER` and `BENCH_LOOKUPRES_VIEW_USER`.

// AuthzedBenchmarkReads runs a comprehensive suite of read benchmarks against the current dataset.
// It tests various permission check patterns including direct relationships, organizational hierarchies,
// group memberships, and resource lookups. All benchmarks operate in streaming mode without
// precomputing or caching relationships.
func AuthzedBenchmarkReads() {
	client, _, cancel, err := infrastructure.NewAuthzedPgdbClientFromEnv(context.Background())
	if err != nil {
		log.Fatalf("[authzed_pgdb] failed to create authzed client: %v", err)
	}
	defer cancel()
	defer client.Close()
	// Log startup summary including any env-overridden lookup users.
	start := time.Now()
	heavyManageUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	regularViewUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[authzed_pgdb] Running in streaming-only mode (no precollection). elapsed=%s heavyManageUser=%q regularViewUser=%q",
		elapsed, heavyManageUser, regularViewUser)

	// Run individual benchmark scenarios
	runCheckManageDirectUser(client)          // Test direct manager_user relationships in resource_acl
	runCheckManageOrgAdmin(client)            // Test org->admin permission paths
	runCheckViewViaGroupMember(client)        // Test permissions via viewer_group and group membership
	runLookupResourcesManageHeavyUser(client) // Test resource lookup for users with many manage permissions
	runLookupResourcesViewRegularUser(client) // Test resource lookup for users with regular view permissions

	log.Println("[authzed_pgdb] == Authzed read benchmarks DONE ==")
}

// streamReadRels streams relationships matching the given filter and invokes the handle
// callback for each relationship. This helper avoids collecting results into memory,
// making it suitable for processing large datasets without memory overhead.
func streamReadRels(ctx context.Context, client *authzed.Client, filter *v1.RelationshipFilter, handle func(rel *v1.Relationship)) error {
	stream, err := client.ReadRelationships(ctx, &v1.ReadRelationshipsRequest{
		RelationshipFilter: filter,
	})
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		handle(resp.Relationship)
	}
	return nil
}

// runLookupBench runs LookupResources calls for a given user and permission,
// counting the number of resources returned and reporting timing metrics.
// Each iteration streams all accessible resources and counts them.
//
// Parameters:
//   - client: The Authzed client for making API calls
//   - name: Benchmark identifier for logging
//   - permission: The permission to lookup (e.g., "manage", "view")
//   - userID: The user ID to lookup resources for
//   - iters: Number of iterations to run
//   - timeout: Maximum time allowed per LookupResources call
func runLookupBench(client *authzed.Client, name, permission, userID string, iters int, timeout time.Duration) {
	if userID == "" {
		log.Printf("[authzed_pgdb] [%s] skipped: no user specified", name)
		return
	}

	log.Printf("[authzed_pgdb] [%s] iterations=%d user=%s", name, iters, userID)

	var total time.Duration
	var lastCount int

	for i := range iters {
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
			log.Fatalf("[authzed_pgdb] [%s] LookupResources failed: %v", name, err)
		}

		// Count resources returned in the stream
		count := 0
		for {
			_, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				cancel()
				log.Fatalf("[authzed_pgdb] [%s] stream Recv failed: %v", name, err)
			}
			count++
		}
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[authzed_pgdb] [%s] iter=%d resources=%d duration=%s", name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[authzed_pgdb] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}

// runCheckManageDirectUser benchmarks CheckPermission calls for "manage" permission
// where users are directly assigned as manager_user on resources. This tests the
// simplest permission path without organizational or group hierarchies.
// The number of iterations is controlled by BENCH_CHECK_DIRECT_SUPER_ITER env variable.
func runCheckManageDirectUser(client *authzed.Client) {
	iters := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)

	log.Printf("[authzed_pgdb] [check_manage_direct_user] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_MANAGE_USER is set, prefer LookupResources for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// Stream LookupResources for the specified user and permission "manage"
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "manage",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{ObjectType: "user", ObjectId: lookupUser},
				},
				Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
			})
			if err != nil {
				cancel()
				log.Fatalf("[authzed_pgdb] [check_manage_direct_user] LookupResources failed: %v", err)
			}

			streamed := 0
			for {
				if done >= iters || streamed >= sampleLimit {
					break
				}
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					cancel()
					log.Fatalf("[authzed_pgdb] [check_manage_direct_user] Lookup stream Recv failed: %v", err)
				}
				streamed++
				resID := resp.GetResourceObjectId()

				// Call CheckPermission for each resource as it arrives (no buffering)
				cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				_, err = client.CheckPermission(cctx, &v1.CheckPermissionRequest{
					Resource:    &v1.ObjectReference{ObjectType: "resource", ObjectId: resID},
					Permission:  "manage",
					Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: lookupUser}},
					Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
				})
				ccancel()
				if err != nil {
					cancel()
					log.Fatalf("[authzed_pgdb] [check_manage_direct_user] CheckPermission failed: %v", err)
				}
				dur := time.Since(start)
				if done%100 == 0 {
					log.Printf("[authzed_pgdb] [check_manage_direct_user] lookup iter=%d resource=%s user=%s dur=%s", done, resID, lookupUser, dur)
				}
				done++
			}
			cancel()
			if streamed == 0 {
				log.Printf("[authzed_pgdb] [check_manage_direct_user] lookup-mode: no resources returned for user=%s", lookupUser)
				// fallback to relationship streaming once to exercise other paths
				lookupUser = ""
			}
			continue
		}

		// Stream all resource.manager_user relationships and check each one
		err := streamReadRels(context.Background(), client, &v1.RelationshipFilter{
			ResourceType:     "resource",
			OptionalRelation: "manager_user",
		}, func(rel *v1.Relationship) {
			if done >= iters {
				return
			}
			resID := rel.Resource.ObjectId
			userID := rel.Subject.Object.ObjectId

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			start := time.Now()
			_, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
				Resource:    &v1.ObjectReference{ObjectType: "resource", ObjectId: resID},
				Permission:  "manage",
				Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: userID}},
				Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
			})
			cancel()
			if err != nil {
				log.Fatalf("[authzed_pgdb] [check_manage_direct_user] CheckPermission failed: %v", err)
			}
			dur := time.Since(start)
			// Log every 100th iteration to avoid excessive output
			if done%100 == 0 {
				log.Printf("[authzed_pgdb] [check_manage_direct_user] iter=%d resource=%s user=%s dur=%s", done, resID, userID, dur)
			}
			done++
		})
		if err != nil {
			log.Fatalf("[authzed_pgdb] [check_manage_direct_user] streamReadRels failed: %v", err)
		}
	}
	log.Printf("[authzed_pgdb] [check_manage_direct_user] DONE: iters=%d", iters)
}

// runCheckManageOrgAdmin benchmarks CheckPermission calls for "manage" permission
// where access is granted through organizational admin relationships (org->admin path).
// This tests permission inheritance through organizational hierarchies.
// The number of iterations is controlled by BENCH_CHECK_ORGADMIN_ITER env variable.
func runCheckManageOrgAdmin(client *authzed.Client) {
	iters := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)

	log.Printf("[authzed_pgdb] [check_manage_org_admin] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_MANAGE_USER is set, prefer LookupResources for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// LookupResources for the user with permission "manage"
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "manage",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{ObjectType: "user", ObjectId: lookupUser},
				},
				Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
			})
			if err != nil {
				cancel()
				log.Fatalf("[authzed_pgdb] [check_manage_org_admin] LookupResources failed: %v", err)
			}

			streamed := 0
			for {
				if done >= iters || streamed >= sampleLimit {
					break
				}
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					cancel()
					log.Fatalf("[authzed_pgdb] [check_manage_org_admin] Lookup stream Recv failed: %v", err)
				}
				streamed++
				resID := resp.GetResourceObjectId()

				// CheckPermission for returned resource
				cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				_, err = client.CheckPermission(cctx, &v1.CheckPermissionRequest{
					Resource:    &v1.ObjectReference{ObjectType: "resource", ObjectId: resID},
					Permission:  "manage",
					Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: lookupUser}},
					Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
				})
				ccancel()
				if err != nil {
					cancel()
					log.Fatalf("[authzed_pgdb] [check_manage_org_admin] CheckPermission failed: %v", err)
				}
				dur := time.Since(start)
				if done%100 == 0 {
					log.Printf("[authzed_pgdb] [check_manage_org_admin] lookup iter=%d resource=%s user=%s dur=%s", done, resID, lookupUser, dur)
				}
				done++
			}
			cancel()
			if streamed == 0 {
				log.Printf("[authzed_pgdb] [check_manage_org_admin] lookup-mode: no resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			continue
		}

		// Stream all resource.org relationships and check admin permissions
		err := streamReadRels(context.Background(), client, &v1.RelationshipFilter{
			ResourceType:     "resource",
			OptionalRelation: "org",
		}, func(rel *v1.Relationship) {
			if done >= iters {
				return
			}
			resID := rel.Resource.ObjectId
			orgID := rel.Subject.Object.ObjectId

			// Find the first admin for this organization on-demand (no caching)
			var adminUser string
			_ = streamReadRels(context.Background(), client, &v1.RelationshipFilter{
				ResourceType:     "organization",
				OptionalRelation: "admin_user",
			}, func(orel *v1.Relationship) {
				if orel.Resource.ObjectId != orgID {
					return
				}
				if adminUser == "" {
					adminUser = orel.Subject.Object.ObjectId
				}
			})

			if adminUser == "" {
				// Skip if no admin found for this organization
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			start := time.Now()
			_, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
				Resource:    &v1.ObjectReference{ObjectType: "resource", ObjectId: resID},
				Permission:  "manage",
				Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: adminUser}},
				Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
			})
			cancel()
			if err != nil {
				log.Fatalf("[authzed_pgdb] [check_manage_org_admin] CheckPermission failed: %v", err)
			}
			dur := time.Since(start)
			// Log every 100th iteration to avoid excessive output
			if done%100 == 0 {
				log.Printf("[authzed_pgdb] [check_manage_org_admin] iter=%d resource=%s org=%s admin=%s dur=%s", done, resID, orgID, adminUser, dur)
			}
			done++
		})
		if err != nil {
			log.Fatalf("[authzed_pgdb] [check_manage_org_admin] streamReadRels failed: %v", err)
		}
	}
	log.Printf("[authzed_pgdb] [check_manage_org_admin] DONE: iters=%d", iters)
}

// runCheckViewViaGroupMember benchmarks CheckPermission calls for "view" permission
// where access is granted through user group membership (viewer_group + group membership path).
// This tests permission inheritance through group hierarchies without transitive expansion.
// The number of iterations is controlled by BENCH_CHECK_VIEW_GROUP_ITER env variable.
func runCheckViewViaGroupMember(client *authzed.Client) {
	iters := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)

	log.Printf("[authzed_pgdb] [check_view_via_group_member] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_VIEW_USER is set, prefer LookupResources for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// LookupResources for permission "view"
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			stream, err := client.LookupResources(ctx, &v1.LookupResourcesRequest{
				ResourceObjectType: "resource",
				Permission:         "view",
				Subject: &v1.SubjectReference{
					Object: &v1.ObjectReference{ObjectType: "user", ObjectId: lookupUser},
				},
				Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
			})
			if err != nil {
				cancel()
				log.Fatalf("[authzed_pgdb] [check_view_via_group_member] LookupResources failed: %v", err)
			}

			streamed := 0
			for {
				if done >= iters || streamed >= sampleLimit {
					break
				}
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					cancel()
					log.Fatalf("[authzed_pgdb] [check_view_via_group_member] Lookup stream Recv failed: %v", err)
				}
				streamed++
				resID := resp.GetResourceObjectId()

				// Call CheckPermission for each resource
				cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				_, err = client.CheckPermission(cctx, &v1.CheckPermissionRequest{
					Resource:    &v1.ObjectReference{ObjectType: "resource", ObjectId: resID},
					Permission:  "view",
					Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: lookupUser}},
					Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
				})
				ccancel()
				if err != nil {
					cancel()
					log.Fatalf("[authzed_pgdb] [check_view_via_group_member] CheckPermission failed: %v", err)
				}
				dur := time.Since(start)
				if done%100 == 0 {
					log.Printf("[authzed_pgdb] [check_view_via_group_member] lookup iter=%d resource=%s user=%s dur=%s", done, resID, lookupUser, dur)
				}
				done++
			}
			cancel()
			if streamed == 0 {
				log.Printf("[authzed_pgdb] [check_view_via_group_member] lookup-mode: no resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			continue
		}

		// Stream resource.viewer_group relations and check permissions for group members
		err := streamReadRels(context.Background(), client, &v1.RelationshipFilter{
			ResourceType:     "resource",
			OptionalRelation: "viewer_group",
		}, func(rel *v1.Relationship) {
			if done >= iters {
				return
			}
			resID := rel.Resource.ObjectId
			groupID := rel.Subject.Object.ObjectId

			// Find a direct member of this group (no transitive expansion)
			var pickedUser string
			_ = streamReadRels(context.Background(), client, &v1.RelationshipFilter{
				ResourceType:     "usergroup",
				OptionalRelation: "direct_member_user",
			}, func(grel *v1.Relationship) {
				if grel.Resource.ObjectId != groupID {
					return
				}
				if pickedUser == "" {
					pickedUser = grel.Subject.Object.ObjectId
				}
			})

			if pickedUser == "" {
				// Try manager_user as fallback if no direct member found
				_ = streamReadRels(context.Background(), client, &v1.RelationshipFilter{
					ResourceType:     "usergroup",
					OptionalRelation: "direct_manager_user",
				}, func(grel *v1.Relationship) {
					if grel.Resource.ObjectId != groupID {
						return
					}
					if pickedUser == "" {
						pickedUser = grel.Subject.Object.ObjectId
					}
				})
			}

			if pickedUser == "" {
				// Skip if no group member found
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			start := time.Now()
			_, err := client.CheckPermission(ctx, &v1.CheckPermissionRequest{
				Resource:    &v1.ObjectReference{ObjectType: "resource", ObjectId: resID},
				Permission:  "view",
				Subject:     &v1.SubjectReference{Object: &v1.ObjectReference{ObjectType: "user", ObjectId: pickedUser}},
				Consistency: &v1.Consistency{Requirement: &v1.Consistency_FullyConsistent{FullyConsistent: true}},
			})
			cancel()
			if err != nil {
				log.Fatalf("[authzed_pgdb] [check_view_via_group_member] CheckPermission failed: %v", err)
			}
			dur := time.Since(start)
			// Log every 100th iteration to avoid excessive output
			if done%100 == 0 {
				log.Printf("[authzed_pgdb] [check_view_via_group_member] iter=%d resource=%s group=%s user=%s dur=%s", done, resID, groupID, pickedUser, dur)
			}
			done++
		})
		if err != nil {
			log.Fatalf("[authzed_pgdb] [check_view_via_group_member] streamReadRels failed: %v", err)
		}
	}
	log.Printf("[authzed_pgdb] [check_view_via_group_member] DONE: iters=%d", iters)
}

// runLookupResourcesManageHeavyUser benchmarks LookupResources for "manage" permission
// for a user with many manage permissions (heavy user scenario). This tests the performance
// of resource enumeration for users with extensive access rights.
// User ID is specified via BENCH_LOOKUPRES_MANAGE_USER env variable.
// Iterations are controlled by BENCH_LOOKUPRES_MANAGE_ITER env variable (default: 10).
func runLookupResourcesManageHeavyUser(client *authzed.Client) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	runLookupBench(client, "lookup_resources_manage_super", "manage", userID, iters, 60*time.Second)
}

// runLookupResourcesViewRegularUser benchmarks LookupResources for "view" permission
// for a user with typical view permissions (regular user scenario). This tests the performance
// of resource enumeration for users with normal access patterns.
// User ID is specified via BENCH_LOOKUPRES_VIEW_USER env variable.
// Iterations are controlled by BENCH_LOOKUPRES_VIEW_ITER env variable (default: 10).
func runLookupResourcesViewRegularUser(client *authzed.Client) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	runLookupBench(client, "lookup_resources_view_regular", "view", userID, iters, 60*time.Second)
}
