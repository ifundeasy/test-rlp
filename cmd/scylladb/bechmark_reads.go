package scylladb

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/gocql/gocql"

	"test-tls/infrastructure"
	"test-tls/utils"
)

// This file runs read benchmarks against ScyllaDB in streaming-only
// mode. It does not precompute or retain relationships in memory — all
// relationships are streamed from queries without buffering. Benchmarks
// require database access and mirror Authzed streaming patterns.

// ScylladbBenchmarkReads runs a comprehensive suite of read benchmarks against the current dataset.
// It tests various permission check patterns including direct relationships, organizational hierarchies,
// group memberships, and resource lookups. All benchmarks operate in streaming mode without
// precomputing or caching relationships.
func ScylladbBenchmarkReads() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	session, cleanup, err := infrastructure.NewScyllaFromEnv(ctx)
	cancel()
	if err != nil {
		log.Fatalf("[scylladb] failed to create session: %v", err)
	}
	defer cleanup()

	// Log startup summary including any env-overridden lookup users.
	start := time.Now()
	heavyManageUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	regularViewUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[scylladb] Running in streaming-only mode (no precollection). elapsed=%s heavyManageUser=%q regularViewUser=%q",
		elapsed, heavyManageUser, regularViewUser)

	// Run individual benchmark scenarios
	runCheckManageDirectUser(session)          // Test direct manager_user relationships in resource_acl
	runCheckManageOrgAdmin(session)            // Test org->admin permission paths
	runCheckViewViaGroupMember(session)        // Test permissions via viewer_group and group membership
	runLookupResourcesManageHeavyUser(session) // Test resource lookup for users with many manage permissions
	runLookupResourcesViewRegularUser(session) // Test resource lookup for users with regular view permissions

	log.Println("[scylladb] == ScyllaDB read benchmarks DONE ==")
}

// streamQuery streams rows from a CQL query and invokes handle for each row.
// This helper avoids collecting results into memory, making it suitable for
// processing large datasets without memory overhead.
func streamQuery(ctx context.Context, session *gocql.Session, query string, args []any, handle func(iter *gocql.Iter) error) error {
	q := session.Query(query, args...)
	iter := q.WithContext(ctx).Iter()
	defer iter.Close()

	// Process each row as it arrives
	if err := handle(iter); err != nil {
		return err
	}
	return iter.Close()
}

// runLookupBench runs resource lookup queries for a given user and permission,
// counting the number of resources returned and reporting timing metrics.
// Each iteration streams all accessible resources and counts them.
//
// Parameters:
//   - session: ScyllaDB session
//   - name: Benchmark identifier for logging
//   - permission: The permission to lookup (e.g., "manager_user", "viewer_user")
//   - userID: The user ID to lookup resources for
//   - iters: Number of iterations to run
//   - timeout: Maximum time allowed per lookup query
func runLookupBench(session *gocql.Session, name, permission, userID string, iters int, timeout time.Duration) {
	if userID == "" {
		log.Printf("[scylladb] [%s] skipped: no user specified", name)
		return
	}

	log.Printf("[scylladb] [%s] iterations=%d user=%s", name, iters, userID)

	var total time.Duration
	var lastCount int

	for i := range iters {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()

		// Query resources where the user has the specified permission via resource_acl_by_subject
		query := `SELECT resource_id FROM resource_acl_by_subject
			WHERE subject_type = 'user' AND subject_id = ? AND relation = ?`

		count := 0
		err := streamQuery(ctx, session, query, []any{userID, permission}, func(iter *gocql.Iter) error {
			for {
				var resID int
				if !iter.Scan(&resID) {
					break
				}
				count++
			}
			return nil
		})
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
	log.Printf("[scylladb] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}

// runCheckManageDirectUser benchmarks permission checks for "manage" permission
// where users are directly assigned as manager_user on resources. This tests the
// simplest permission path without organizational or group hierarchies.
// The number of iterations is controlled by BENCH_CHECK_DIRECT_SUPER_ITER env variable.
func runCheckManageDirectUser(session *gocql.Session) {
	iters := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)

	log.Printf("[scylladb] [check_manage_direct_user] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_MANAGE_USER is set, prefer lookup for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// Query resources where the user has manager_user permission via resource_acl_by_subject
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			query := `SELECT resource_id FROM resource_acl_by_subject
				WHERE subject_type = 'user' AND subject_id = ? AND relation = 'manager_user'`

			streamed := 0
			err := streamQuery(ctx, session, query, []any{lookupUser}, func(iter *gocql.Iter) error {
				for {
					if done >= iters || streamed >= sampleLimit {
						break
					}
					var resID int
					if !iter.Scan(&resID) {
						break
					}
					streamed++

					// Check permission existence for returned resource
					cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
					start := time.Now()
					checkQuery := `SELECT COUNT(*) FROM resource_acl_by_resource
						WHERE resource_id = ? AND relation = 'manager_user' AND subject_type = 'user' AND subject_id = ?`
					var exists int
					err := session.Query(checkQuery, resID, lookupUser).WithContext(cctx).Scan(&exists)
					ccancel()
					if err != nil {
						log.Fatalf("[scylladb] [check_manage_direct_user] permission check failed: %v", err)
					}
					dur := time.Since(start)
					if done%100 == 0 {
						log.Printf("[scylladb] [check_manage_direct_user] lookup iter=%d resource=%d user=%s dur=%s", done, resID, lookupUser, dur)
					}
					done++
				}
				return nil
			})
			cancel()
			if err != nil {
				log.Fatalf("[scylladb] [check_manage_direct_user] query failed: %v", err)
			}
			if streamed == 0 {
				log.Printf("[scylladb] [check_manage_direct_user] lookup-mode: no resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			continue
		}

		// Stream all resource.manager_user relationships and check each one via resource_acl_by_resource
		ctx := context.Background()
		query := `SELECT resource_id, subject_id FROM resource_acl_by_resource
			WHERE relation = 'manager_user' AND subject_type = 'user'`

		err := streamQuery(ctx, session, query, nil, func(iter *gocql.Iter) error {
			for {
				if done >= iters {
					break
				}
				var resID, userID int
				if !iter.Scan(&resID, &userID) {
					break
				}

				cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				checkQuery := `SELECT COUNT(*) FROM resource_acl_by_resource
					WHERE resource_id = ? AND relation = 'manager_user' AND subject_type = 'user' AND subject_id = ?`
				var exists int
				queryErr := session.Query(checkQuery, resID, userID).WithContext(cctx).Scan(&exists)
				cancel()
				if queryErr != nil {
					log.Fatalf("[scylladb] [check_manage_direct_user] permission check failed: %v", queryErr)
				}
				dur := time.Since(start)
				// Log every 100th iteration to avoid excessive output
				if done%100 == 0 {
					log.Printf("[scylladb] [check_manage_direct_user] iter=%d resource=%d user=%d dur=%s", done, resID, userID, dur)
				}
				done++
			}
			return nil
		})
		if err != nil {
			log.Fatalf("[scylladb] [check_manage_direct_user] streaming failed: %v", err)
		}
	}
	log.Printf("[scylladb] [check_manage_direct_user] DONE: iters=%d", iters)
}

// runCheckManageOrgAdmin benchmarks permission checks for "manage" permission
// where access is granted through organizational admin relationships (org->admin path).
// This tests permission inheritance through organizational hierarchies.
// The number of iterations is controlled by BENCH_CHECK_ORGADMIN_ITER env variable.
func runCheckManageOrgAdmin(session *gocql.Session) {
	iters := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)

	log.Printf("[scylladb] [check_manage_org_admin] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_MANAGE_USER is set, prefer lookup for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// Query resources where the user has manager_user permission via resource_acl_by_subject
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			query := `SELECT resource_id FROM resource_acl_by_subject
				WHERE subject_type = 'user' AND subject_id = ? AND relation = 'manager_user'`

			streamed := 0
			err := streamQuery(ctx, session, query, []any{lookupUser}, func(iter *gocql.Iter) error {
				for {
					if done >= iters || streamed >= sampleLimit {
						break
					}
					var resID int
					if !iter.Scan(&resID) {
						break
					}
					streamed++

					// Check permission existence
					cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
					start := time.Now()
					checkQuery := `SELECT COUNT(*) FROM resource_acl_by_resource
						WHERE resource_id = ? AND relation = 'manager_user' AND subject_type = 'user' AND subject_id = ?`
					var exists int
					err := session.Query(checkQuery, resID, lookupUser).WithContext(cctx).Scan(&exists)
					ccancel()
					if err != nil {
						log.Fatalf("[scylladb] [check_manage_org_admin] permission check failed: %v", err)
					}
					dur := time.Since(start)
					if done%100 == 0 {
						log.Printf("[scylladb] [check_manage_org_admin] lookup iter=%d resource=%d user=%s dur=%s", done, resID, lookupUser, dur)
					}
					done++
				}
				return nil
			})
			cancel()
			if err != nil {
				log.Fatalf("[scylladb] [check_manage_org_admin] query failed: %v", err)
			}
			if streamed == 0 {
				log.Printf("[scylladb] [check_manage_org_admin] lookup-mode: no resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			continue
		}

		// Stream all resource.manager_user relationships and check admin permissions
		ctx := context.Background()
		query := `SELECT resource_id, subject_id FROM resource_acl_by_resource
			WHERE relation = 'manager_user' AND subject_type = 'user'`

		err := streamQuery(ctx, session, query, nil, func(iter *gocql.Iter) error {
			for {
				if done >= iters {
					break
				}
				var resID, userID int
				if !iter.Scan(&resID, &userID) {
					break
				}

				cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				checkQuery := `SELECT COUNT(*) FROM resource_acl_by_resource
					WHERE resource_id = ? AND relation = 'manager_user' AND subject_type = 'user' AND subject_id = ?`
				var exists int
				queryErr := session.Query(checkQuery, resID, userID).WithContext(cctx).Scan(&exists)
				cancel()
				if queryErr != nil {
					log.Fatalf("[scylladb] [check_manage_org_admin] permission check failed: %v", queryErr)
				}
				dur := time.Since(start)
				if done%100 == 0 {
					log.Printf("[scylladb] [check_manage_org_admin] iter=%d resource=%d user=%d dur=%s", done, resID, userID, dur)
				}
				done++
			}
			return nil
		})
		if err != nil {
			log.Fatalf("[scylladb] [check_manage_org_admin] streaming failed: %v", err)
		}
	}
	log.Printf("[scylladb] [check_manage_org_admin] DONE: iters=%d", iters)
}

// runCheckViewViaGroupMember benchmarks permission checks for "view" permission
// where access is granted through user group membership (viewer_group + group membership path).
// This tests permission inheritance through group hierarchies without transitive expansion.
// The number of iterations is controlled by BENCH_CHECK_VIEW_GROUP_ITER env variable.
func runCheckViewViaGroupMember(session *gocql.Session) {
	iters := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)

	log.Printf("[scylladb] [check_view_via_group_member] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_VIEW_USER is set, prefer lookup for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// Query resources where the user has viewer_user permission via resource_acl_by_subject
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			query := `SELECT resource_id FROM resource_acl_by_subject
				WHERE subject_type = 'user' AND subject_id = ? AND relation = 'viewer_user'`

			streamed := 0
			err := streamQuery(ctx, session, query, []any{lookupUser}, func(iter *gocql.Iter) error {
				for {
					if done >= iters || streamed >= sampleLimit {
						break
					}
					var resID int
					if !iter.Scan(&resID) {
						break
					}
					streamed++

					// Check permission existence
					cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
					start := time.Now()
					checkQuery := `SELECT COUNT(*) FROM resource_acl_by_resource
						WHERE resource_id = ? AND relation = 'viewer_user' AND subject_type = 'user' AND subject_id = ?`
					var exists int
					err := session.Query(checkQuery, resID, lookupUser).WithContext(cctx).Scan(&exists)
					ccancel()
					if err != nil {
						log.Fatalf("[scylladb] [check_view_via_group_member] permission check failed: %v", err)
					}
					dur := time.Since(start)
					if done%100 == 0 {
						log.Printf("[scylladb] [check_view_via_group_member] lookup iter=%d resource=%d user=%s dur=%s", done, resID, lookupUser, dur)
					}
					done++
				}
				return nil
			})
			cancel()
			if err != nil {
				log.Fatalf("[scylladb] [check_view_via_group_member] query failed: %v", err)
			}
			if streamed == 0 {
				log.Printf("[scylladb] [check_view_via_group_member] lookup-mode: no resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			continue
		}

		// Stream resource.viewer_group relations and check permissions for group members
		ctx := context.Background()
		query := `SELECT resource_id, subject_id FROM resource_acl_by_resource
			WHERE relation = 'viewer_group' AND subject_type = 'group'`

		err := streamQuery(ctx, session, query, nil, func(iter *gocql.Iter) error {
			for {
				if done >= iters {
					break
				}
				var resID, groupID int
				if !iter.Scan(&resID, &groupID) {
					break
				}

				// Find a direct member of this group from group_members_expanded
				memberQuery := `SELECT user_id FROM group_members_expanded
					WHERE group_id = ? AND role = 'member'
					LIMIT 1`
				var pickedUser int
				err := session.Query(memberQuery, groupID).Scan(&pickedUser)
				if err == gocql.ErrNotFound {
					// No member found, skip
					continue
				}
				if err != nil {
					log.Fatalf("[scylladb] [check_view_via_group_member] find member failed: %v", err)
				}

				cctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				checkQuery := `SELECT COUNT(*) FROM resource_acl_by_resource
					WHERE resource_id = ? AND relation = 'viewer_user' AND subject_type = 'user' AND subject_id = ?`
				var exists int
				queryErr := session.Query(checkQuery, resID, pickedUser).WithContext(cctx).Scan(&exists)
				cancel()
				if queryErr != nil {
					log.Fatalf("[scylladb] [check_view_via_group_member] permission check failed: %v", queryErr)
				}
				dur := time.Since(start)
				if done%100 == 0 {
					log.Printf("[scylladb] [check_view_via_group_member] iter=%d resource=%d group=%d user=%d dur=%s", done, resID, groupID, pickedUser, dur)
				}
				done++
			}
			return nil
		})
		if err != nil {
			log.Fatalf("[scylladb] [check_view_via_group_member] streaming failed: %v", err)
		}
	}
	log.Printf("[scylladb] [check_view_via_group_member] DONE: iters=%d", iters)
}

// runLookupResourcesManageHeavyUser benchmarks resource lookup for "manage" permission
// for a user with many manage permissions (heavy user scenario). This tests the performance
// of resource enumeration for users with extensive access rights.
// User ID is specified via BENCH_LOOKUPRES_MANAGE_USER env variable.
// Iterations are controlled by BENCH_LOOKUPRES_MANAGE_ITER env variable (default: 10).
func runLookupResourcesManageHeavyUser(session *gocql.Session) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	runLookupBench(session, "lookup_resources_manage_super", "manager_user", userID, iters, 60*time.Second)
}

// runLookupResourcesViewRegularUser benchmarks resource lookup for "view" permission
// for a user with typical view permissions (regular user scenario). This tests the performance
// of resource enumeration for users with normal access patterns.
// User ID is specified via BENCH_LOOKUPRES_VIEW_USER env variable.
// Iterations are controlled by BENCH_LOOKUPRES_VIEW_ITER env variable (default: 10).
func runLookupResourcesViewRegularUser(session *gocql.Session) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	runLookupBench(session, "lookup_resources_view_regular", "viewer_user", userID, iters, 60*time.Second)
}
