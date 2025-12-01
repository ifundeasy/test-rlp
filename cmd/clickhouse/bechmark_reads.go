package clickhouse

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"test-tls/infrastructure"
	"test-tls/utils"
)

// This file runs read benchmarks against ClickHouse in streaming-only
// mode. It does not precompute or retain relationships in memory — all
// queries are streamed and processed on-demand to avoid any in-memory
// collection. The benchmarks mirror the Authzed patterns but use SQL
// queries against the ClickHouse schema with resource_acl and related tables.

// ClickhouseBenchmarkReads runs a comprehensive suite of read benchmarks against the current ClickHouse dataset.
// It tests various permission check patterns including direct relationships, organizational hierarchies,
// group memberships, and resource lookups. All benchmarks operate in streaming mode without
// precomputing or caching relationships.
func ClickhouseBenchmarkReads() {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	db, cleanup, err := infrastructure.NewClickhouseFromEnv(ctx)
	if err != nil {
		log.Fatalf("[clickhouse] failed to create clickhouse client: %v", err)
	}
	defer cleanup()

	// Log startup summary including any env-overridden lookup users.
	start := time.Now()
	heavyManageUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	regularViewUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[clickhouse] Running in streaming-only mode (no precollection). elapsed=%s heavyManageUser=%q regularViewUser=%q",
		elapsed, heavyManageUser, regularViewUser)

	// Run individual benchmark scenarios
	runCheckManageDirectUser(db)          // Test direct manager_user relationships in resource_acl
	runCheckManageOrgAdmin(db)            // Test org->admin permission paths
	runCheckViewViaGroupMember(db)        // Test permissions via group members and group membership
	runLookupResourcesManageHeavyUser(db) // Test resource lookup for users with many manage permissions
	runLookupResourcesViewRegularUser(db) // Test resource lookup for users with regular view permissions

	log.Println("[clickhouse] == ClickHouse read benchmarks DONE ==")
}

// streamQuery executes a query and invokes the handle callback for each row.
// This helper avoids collecting results into memory, making it suitable for
// processing large datasets without memory overhead.
func streamQuery(ctx context.Context, db *sql.DB, query string, args []any, handle func(*sql.Rows) error) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err := handle(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// runLookupBench runs SELECT COUNT(*) queries for a given user and relation,
// counting the number of resources returned and reporting timing metrics.
// Each iteration queries all accessible resources and counts them.
//
// Parameters:
//   - db: The ClickHouse database connection
//   - name: Benchmark identifier for logging
//   - relation: The relation type to lookup (e.g., "manager", "viewer")
//   - userID: The user ID to lookup resources for
//   - iters: Number of iterations to run
func runLookupBench(db *sql.DB, name, relation, userID string, iters int) {
	if userID == "" {
		log.Printf("[clickhouse] [%s] skipped: no user specified", name)
		return
	}

	log.Printf("[clickhouse] [%s] iterations=%d user=%s", name, iters, userID)

	var total time.Duration
	var lastCount int

	for i := range iters {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		start := time.Now()

		// Query for all resources where the user has the specified relation
		query := `
		SELECT COUNT(DISTINCT resource_id)
		FROM resource_acl
		WHERE subject_type = 'user' AND subject_id = ? AND relation = ?
		`
		var count int
		err := db.QueryRowContext(ctx, query, userID, relation).Scan(&count)
		cancel()
		if err != nil {
			log.Fatalf("[clickhouse] [%s] query failed: %v", name, err)
		}

		dur := time.Since(start)
		total += dur
		lastCount = count

		log.Printf("[clickhouse] [%s] iter=%d resources=%d duration=%s", name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(int64(total) / int64(iters))
	log.Printf("[clickhouse] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s",
		name, iters, lastCount, avg, total)
}

// runCheckManageDirectUser benchmarks queries for "manager" relation
// where users are directly assigned as manager_user on resources. This tests the
// simplest permission path without organizational or group hierarchies.
// The number of iterations is controlled by BENCH_CHECK_DIRECT_SUPER_ITER env variable.
func runCheckManageDirectUser(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)

	log.Printf("[clickhouse] [check_manage_direct_user] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_MANAGE_USER is set, prefer LookupResources for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// Stream all resources where the lookup user has "manager" relation
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			query := `
			SELECT DISTINCT resource_id
			FROM resource_acl
			WHERE subject_type = 'user' AND subject_id = ? AND relation = 'manager'
			LIMIT ?
			`
			err := streamQuery(ctx, db, query, []any{lookupUser, sampleLimit}, func(rows *sql.Rows) error {
				if done >= iters {
					return nil
				}
				var resourceID uint32
				if err := rows.Scan(&resourceID); err != nil {
					return err
				}

				// For each resource, verify the permission exists (mimics CheckPermission)
				cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				checkQuery := `
				SELECT 1
				FROM resource_acl
				WHERE resource_id = ? AND subject_type = 'user' AND subject_id = ? AND relation = 'manager'
				LIMIT 1
				`
				var exists int
				err := db.QueryRowContext(cctx, checkQuery, resourceID, lookupUser).Scan(&exists)
				ccancel()
				if err != nil && err != sql.ErrNoRows {
					return err
				}

				dur := time.Since(start)
				if done%100 == 0 {
					log.Printf("[clickhouse] [check_manage_direct_user] lookup iter=%d resource=%d user=%s dur=%s", done, resourceID, lookupUser, dur)
				}
				done++
				return nil
			})
			cancel()

			if err != nil && err != context.DeadlineExceeded {
				log.Fatalf("[clickhouse] [check_manage_direct_user] streamQuery failed: %v", err)
			}

			if done < sampleLimit {
				log.Printf("[clickhouse] [check_manage_direct_user] lookup-mode: insufficient resources returned for user=%s", lookupUser)
				// fallback to relationship streaming once to exercise other paths
				lookupUser = ""
			}
			if done >= iters {
				break
			}
			continue
		}

		// Stream all resource_acl rows with manager relation and check each permission
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		query := `
		SELECT resource_id, subject_id
		FROM resource_acl
		WHERE subject_type = 'user' AND relation = 'manager'
		`
		err := streamQuery(ctx, db, query, []any{}, func(rows *sql.Rows) error {
			if done >= iters {
				return nil
			}
			var resourceID uint32
			var userID uint32
			if err := rows.Scan(&resourceID, &userID); err != nil {
				return err
			}

			// Verify the permission exists
			cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
			start := time.Now()
			checkQuery := `
			SELECT 1
			FROM resource_acl
			WHERE resource_id = ? AND subject_type = 'user' AND subject_id = ? AND relation = 'manager'
			LIMIT 1
			`
			var exists int
			err := db.QueryRowContext(cctx, checkQuery, resourceID, userID).Scan(&exists)
			ccancel()
			if err != nil && err != sql.ErrNoRows {
				return err
			}

			dur := time.Since(start)
			// Log every 100th iteration to avoid excessive output
			if done%100 == 0 {
				log.Printf("[clickhouse] [check_manage_direct_user] iter=%d resource=%d user=%d dur=%s", done, resourceID, userID, dur)
			}
			done++
			return nil
		})
		cancel()

		if err != nil && err != context.DeadlineExceeded {
			log.Fatalf("[clickhouse] [check_manage_direct_user] streamQuery failed: %v", err)
		}
		break
	}
	log.Printf("[clickhouse] [check_manage_direct_user] DONE: iters=%d", iters)
}

// runCheckManageOrgAdmin benchmarks queries for "manager" permission
// where access is granted through organizational admin relationships (org->admin path).
// This tests permission inheritance through organizational hierarchies.
// The number of iterations is controlled by BENCH_CHECK_ORGADMIN_ITER env variable.
func runCheckManageOrgAdmin(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)

	log.Printf("[clickhouse] [check_manage_org_admin] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_MANAGE_USER is set, prefer LookupResources for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// Stream all resources where the lookup user has "manager" relation via org admin
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			query := `
			SELECT DISTINCT r.resource_id
			FROM resources r
			JOIN org_memberships om ON om.org_id = r.org_id
			WHERE om.user_id = ? AND om.role = 'admin'
			LIMIT ?
			`
			err := streamQuery(ctx, db, query, []any{lookupUser, sampleLimit}, func(rows *sql.Rows) error {
				if done >= iters {
					return nil
				}
				var resourceID uint32
				if err := rows.Scan(&resourceID); err != nil {
					return err
				}

				// Verify permission via user_resource_permissions materialized view
				cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				checkQuery := `
				SELECT 1
				FROM user_resource_permissions
				WHERE resource_id = ? AND user_id = ? AND relation = 'manager'
				LIMIT 1
				`
				var exists int
				err := db.QueryRowContext(cctx, checkQuery, resourceID, lookupUser).Scan(&exists)
				ccancel()
				if err != nil && err != sql.ErrNoRows {
					return err
				}

				dur := time.Since(start)
				if done%100 == 0 {
					log.Printf("[clickhouse] [check_manage_org_admin] lookup iter=%d resource=%d user=%s dur=%s", done, resourceID, lookupUser, dur)
				}
				done++
				return nil
			})
			cancel()

			if err != nil && err != context.DeadlineExceeded {
				log.Fatalf("[clickhouse] [check_manage_org_admin] streamQuery failed: %v", err)
			}

			if done < sampleLimit {
				log.Printf("[clickhouse] [check_manage_org_admin] lookup-mode: insufficient resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			if done >= iters {
				break
			}
			continue
		}

		// Stream all resources with an organization and check manager permissions for org admins
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		query := `
		SELECT r.resource_id, r.org_id
		FROM resources r
		`
		err := streamQuery(ctx, db, query, []any{}, func(rows *sql.Rows) error {
			if done >= iters {
				return nil
			}
			var resourceID uint32
			var orgID uint32
			if err := rows.Scan(&resourceID, &orgID); err != nil {
				return err
			}

			// Find the first admin for this organization on-demand (no caching)
			var adminUser uint32
			aCtx, aCancel := context.WithTimeout(context.Background(), 5*time.Second)
			adminQuery := `
			SELECT user_id
			FROM org_memberships
			WHERE org_id = ? AND role = 'admin'
			LIMIT 1
			`
			_ = db.QueryRowContext(aCtx, adminQuery, orgID).Scan(&adminUser)
			aCancel()

			if adminUser == 0 {
				// Skip if no admin found for this organization
				return nil
			}

			cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
			start := time.Now()
			checkQuery := `
			SELECT 1
			FROM user_resource_permissions
			WHERE resource_id = ? AND user_id = ? AND relation = 'manager'
			LIMIT 1
			`
			var exists int
			err := db.QueryRowContext(cctx, checkQuery, resourceID, adminUser).Scan(&exists)
			ccancel()
			if err != nil && err != sql.ErrNoRows {
				return err
			}

			dur := time.Since(start)
			// Log every 100th iteration to avoid excessive output
			if done%100 == 0 {
				log.Printf("[clickhouse] [check_manage_org_admin] iter=%d resource=%d org=%d admin=%d dur=%s", done, resourceID, orgID, adminUser, dur)
			}
			done++
			return nil
		})
		cancel()

		if err != nil && err != context.DeadlineExceeded {
			log.Fatalf("[clickhouse] [check_manage_org_admin] streamQuery failed: %v", err)
		}
		break
	}
	log.Printf("[clickhouse] [check_manage_org_admin] DONE: iters=%d", iters)
}

// runCheckViewViaGroupMember benchmarks queries for "viewer" permission
// where access is granted through user group membership (viewer_group + group membership path).
// This tests permission inheritance through group hierarchies without transitive expansion.
// The number of iterations is controlled by BENCH_CHECK_VIEW_GROUP_ITER env variable.
func runCheckViewViaGroupMember(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)

	log.Printf("[clickhouse] [check_view_via_group_member] streaming mode. iterations=%d", iters)
	done := 0
	// Hybrid behavior: if BENCH_LOOKUPRES_VIEW_USER is set, prefer LookupResources for that user
	lookupUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)

	for done < iters {
		if lookupUser != "" {
			// Stream all resources where the lookup user has "viewer" relation via groups
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			query := `
			SELECT DISTINCT urp.resource_id
			FROM user_resource_permissions urp
			WHERE urp.user_id = ? AND urp.relation = 'viewer'
			LIMIT ?
			`
			err := streamQuery(ctx, db, query, []any{lookupUser, sampleLimit}, func(rows *sql.Rows) error {
				if done >= iters {
					return nil
				}
				var resourceID uint32
				if err := rows.Scan(&resourceID); err != nil {
					return err
				}

				// Verify permission via materialized view
				cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
				start := time.Now()
				checkQuery := `
				SELECT 1
				FROM user_resource_permissions
				WHERE resource_id = ? AND user_id = ? AND relation = 'viewer'
				LIMIT 1
				`
				var exists int
				err := db.QueryRowContext(cctx, checkQuery, resourceID, lookupUser).Scan(&exists)
				ccancel()
				if err != nil && err != sql.ErrNoRows {
					return err
				}

				dur := time.Since(start)
				if done%100 == 0 {
					log.Printf("[clickhouse] [check_view_via_group_member] lookup iter=%d resource=%d user=%s dur=%s", done, resourceID, lookupUser, dur)
				}
				done++
				return nil
			})
			cancel()

			if err != nil && err != context.DeadlineExceeded {
				log.Fatalf("[clickhouse] [check_view_via_group_member] streamQuery failed: %v", err)
			}

			if done < sampleLimit {
				log.Printf("[clickhouse] [check_view_via_group_member] lookup-mode: insufficient resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			if done >= iters {
				break
			}
			continue
		}

		// Stream resource_acl rows with viewer_group relation and check permissions for group members
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		query := `
		SELECT resource_id, subject_id
		FROM resource_acl
		WHERE subject_type = 'group' AND relation = 'viewer'
		`
		err := streamQuery(ctx, db, query, []any{}, func(rows *sql.Rows) error {
			if done >= iters {
				return nil
			}
			var resourceID uint32
			var groupID uint32
			if err := rows.Scan(&resourceID, &groupID); err != nil {
				return err
			}

			// Find a direct member of this group (no transitive expansion) via group_members_expanded
			var pickedUser uint32
			gCtx, gCancel := context.WithTimeout(context.Background(), 5*time.Second)
			memberQuery := `
			SELECT user_id
			FROM group_members_expanded
			WHERE group_id = ?
			LIMIT 1
			`
			_ = db.QueryRowContext(gCtx, memberQuery, groupID).Scan(&pickedUser)
			gCancel()

			if pickedUser == 0 {
				// Skip if no group member found
				return nil
			}

			cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
			start := time.Now()
			checkQuery := `
			SELECT 1
			FROM user_resource_permissions
			WHERE resource_id = ? AND user_id = ? AND relation = 'viewer'
			LIMIT 1
			`
			var exists int
			err := db.QueryRowContext(cctx, checkQuery, resourceID, pickedUser).Scan(&exists)
			ccancel()
			if err != nil && err != sql.ErrNoRows {
				return err
			}

			dur := time.Since(start)
			// Log every 100th iteration to avoid excessive output
			if done%100 == 0 {
				log.Printf("[clickhouse] [check_view_via_group_member] iter=%d resource=%d group=%d user=%d dur=%s", done, resourceID, groupID, pickedUser, dur)
			}
			done++
			return nil
		})
		cancel()

		if err != nil && err != context.DeadlineExceeded {
			log.Fatalf("[clickhouse] [check_view_via_group_member] streamQuery failed: %v", err)
		}
		break
	}
	log.Printf("[clickhouse] [check_view_via_group_member] DONE: iters=%d", iters)
}

// runLookupResourcesManageHeavyUser benchmarks LookupResources for "manager" relation
// for a user with many manager permissions (heavy user scenario). This tests the performance
// of resource enumeration for users with extensive access rights.
// User ID is specified via BENCH_LOOKUPRES_MANAGE_USER env variable.
// Iterations are controlled by BENCH_LOOKUPRES_MANAGE_ITER env variable (default: 10).
func runLookupResourcesManageHeavyUser(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	runLookupBench(db, "lookup_resources_manage_super", "manager", userID, iters)
}

// runLookupResourcesViewRegularUser benchmarks LookupResources for "viewer" relation
// for a user with typical view permissions (regular user scenario). This tests the performance
// of resource enumeration for users with normal access patterns.
// User ID is specified via BENCH_LOOKUPRES_VIEW_USER env variable.
// Iterations are controlled by BENCH_LOOKUPRES_VIEW_ITER env variable (default: 10).
func runLookupResourcesViewRegularUser(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	runLookupBench(db, "lookup_resources_view_regular", "viewer", userID, iters)
}
