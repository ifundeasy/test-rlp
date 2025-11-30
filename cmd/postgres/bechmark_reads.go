package postgres

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"test-tls/infrastructure"
	"test-tls/utils"
)

// PostgresBenchmarkReads runs read benchmarks against the Postgres dataset.
// It mirrors the behavior and logging of the Authzed streaming-only benchmarks
// and performs all work in a streaming fashion (no in-memory collection).
func PostgresBenchmarkReads() {
	ctx := context.Background()
	db, cleanup, err := infrastructure.NewPostgresFromEnv(ctx)
	if err != nil {
		log.Fatalf("[postgres] failed to create postgres client: %v", err)
	}
	defer cleanup()

	start := time.Now()
	heavyManageUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	regularViewUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[postgres] Running in streaming-only mode (no precollection). elapsed=%s heavyManageUser=%q regularViewUser=%q",
		elapsed, heavyManageUser, regularViewUser)

	runCheckManageDirectUser(db)
	runCheckManageOrgAdmin(db)
	runCheckViewViaGroupMember(db)
	runLookupResourcesManageHeavyUser(db)
	runLookupResourcesViewRegularUser(db)

	log.Println("[postgres] == Postgres read benchmarks DONE ==")
}

// runLookupBenchPG enumerates resources from the materialized view for a user
// and counts them. It streams rows from the DB and does not retain results.
func runLookupBenchPG(db *sql.DB, name, permission, userID string, iters int, timeout time.Duration) {
	if userID == "" {
		log.Printf("[postgres] [%s] skipped: no user specified", name)
		return
	}

	log.Printf("[postgres] [%s] iterations=%d user=%s", name, iters, userID)
	var total time.Duration
	var lastCount int

	for i := range iters {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		start := time.Now()

		rows, err := db.QueryContext(ctx, `SELECT resource_id FROM user_resource_permissions WHERE user_id = $1 AND relation = $2`, userID, permission)
		if err != nil {
			cancel()
			log.Fatalf("[postgres] [%s] LookupResources query failed: %v", name, err)
		}

		count := 0
		for rows.Next() {
			var resID int
			if err := rows.Scan(&resID); err != nil {
				rows.Close()
				cancel()
				log.Fatalf("[postgres] [%s] rows.Scan failed: %v", name, err)
			}
			count++
		}
		rows.Close()
		cancel()

		dur := time.Since(start)
		total += dur
		lastCount = count
		log.Printf("[postgres] [%s] iter=%d resources=%d duration=%s", name, i, count, dur.Truncate(time.Millisecond))
	}

	avg := time.Duration(0)
	if iters > 0 {
		avg = time.Duration(int64(total) / int64(iters))
	}
	log.Printf("[postgres] [%s] DONE: iters=%d lastCount=%d avg=%s total=%s", name, iters, lastCount, avg, total)
}

// runCheckManageDirectUser streams direct user->resource ACL rows and runs
// existence checks against the materialized view to emulate CheckPermission.
func runCheckManageDirectUser(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_CHECK_DIRECT_SUPER_ITER", 1000)
	log.Printf("[postgres] [check_manage_direct_user] streaming mode. iterations=%d", iters)

	lookupUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)
	done := 0

	for done < iters {
		if lookupUser != "" {
			// Stream resources for this user from the materialized view
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			rows, err := db.QueryContext(ctx, `SELECT resource_id FROM user_resource_permissions WHERE user_id = $1 AND relation = 'manager'`, lookupUser)
			if err != nil {
				cancel()
				log.Fatalf("[postgres] [check_manage_direct_user] lookup query failed: %v", err)
			}

			streamed := 0
			for rows.Next() {
				if done >= iters || streamed >= sampleLimit {
					break
				}
				var resID int
				if err := rows.Scan(&resID); err != nil {
					rows.Close()
					cancel()
					log.Fatalf("[postgres] [check_manage_direct_user] scan failed: %v", err)
				}
				streamed++

				// Existence check (emulates CheckPermission)
				cstart := time.Now()
				var exists bool
				qctx, qcancel := context.WithTimeout(context.Background(), 2*time.Second)
				err = db.QueryRowContext(qctx, `SELECT EXISTS(SELECT 1 FROM user_resource_permissions WHERE resource_id = $1 AND user_id = $2 AND relation = 'manager')`, resID, lookupUser).Scan(&exists)
				qcancel()
				if err != nil {
					rows.Close()
					cancel()
					log.Fatalf("[postgres] [check_manage_direct_user] check query failed: %v", err)
				}
				dur := time.Since(cstart)
				if done%100 == 0 {
					log.Printf("[postgres] [check_manage_direct_user] lookup iter=%d resource=%d user=%s dur=%s", done, resID, lookupUser, dur)
				}
				done++
			}
			rows.Close()
			cancel()
			if streamed == 0 {
				log.Printf("[postgres] [check_manage_direct_user] lookup-mode: no resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			continue
		}

		// Relationship-driven: stream resource_acl rows for user subjects with manager relation
		rows, err := db.QueryContext(context.Background(), `SELECT resource_id, subject_id FROM resource_acl WHERE subject_type = 'user' AND (relation = 'manager_user' OR relation = 'manager')`)
		if err != nil {
			log.Fatalf("[postgres] [check_manage_direct_user] resource_acl query failed: %v", err)
		}
		for rows.Next() {
			if done >= iters {
				break
			}
			var resID, userID int
			if err := rows.Scan(&resID, &userID); err != nil {
				rows.Close()
				log.Fatalf("[postgres] [check_manage_direct_user] scan failed: %v", err)
			}

			cstart := time.Now()
			var exists bool
			qctx, qcancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = db.QueryRowContext(qctx, `SELECT EXISTS(SELECT 1 FROM user_resource_permissions WHERE resource_id = $1 AND user_id = $2 AND relation = 'manager')`, resID, userID).Scan(&exists)
			qcancel()
			if err != nil {
				rows.Close()
				log.Fatalf("[postgres] [check_manage_direct_user] check query failed: %v", err)
			}
			dur := time.Since(cstart)
			if done%100 == 0 {
				log.Printf("[postgres] [check_manage_direct_user] iter=%d resource=%d user=%d dur=%s", done, resID, userID, dur)
			}
			done++
		}
		rows.Close()
	}
	log.Printf("[postgres] [check_manage_direct_user] DONE: iters=%d", iters)
}

// runCheckManageOrgAdmin streams resources and for each resource finds an org admin
// and performs an existence check against the materialized view.
func runCheckManageOrgAdmin(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_CHECK_ORGADMIN_ITER", 1000)
	log.Printf("[postgres] [check_manage_org_admin] streaming mode. iterations=%d", iters)
	lookupUser := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)
	done := 0

	for done < iters {
		if lookupUser != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			rows, err := db.QueryContext(ctx, `SELECT resource_id FROM user_resource_permissions WHERE user_id = $1 AND relation = 'manager'`, lookupUser)
			if err != nil {
				cancel()
				log.Fatalf("[postgres] [check_manage_org_admin] lookup query failed: %v", err)
			}
			streamed := 0
			for rows.Next() {
				if done >= iters || streamed >= sampleLimit {
					break
				}
				var resID int
				if err := rows.Scan(&resID); err != nil {
					rows.Close()
					cancel()
					log.Fatalf("[postgres] [check_manage_org_admin] scan failed: %v", err)
				}
				streamed++

				cstart := time.Now()
				var exists bool
				qctx, qcancel := context.WithTimeout(context.Background(), 2*time.Second)
				err = db.QueryRowContext(qctx, `SELECT EXISTS(SELECT 1 FROM user_resource_permissions WHERE resource_id = $1 AND user_id = $2 AND relation = 'manager')`, resID, lookupUser).Scan(&exists)
				qcancel()
				if err != nil {
					rows.Close()
					cancel()
					log.Fatalf("[postgres] [check_manage_org_admin] check query failed: %v", err)
				}
				dur := time.Since(cstart)
				if done%100 == 0 {
					log.Printf("[postgres] [check_manage_org_admin] lookup iter=%d resource=%d user=%s dur=%s", done, resID, lookupUser, dur)
				}
				done++
			}
			rows.Close()
			cancel()
			if streamed == 0 {
				log.Printf("[postgres] [check_manage_org_admin] lookup-mode: no resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			continue
		}

		// Stream resources table and find an admin for each org
		rows, err := db.QueryContext(context.Background(), `SELECT resource_id, org_id FROM resources`)
		if err != nil {
			log.Fatalf("[postgres] [check_manage_org_admin] resources query failed: %v", err)
		}
		for rows.Next() {
			if done >= iters {
				break
			}
			var resID, orgID int
			if err := rows.Scan(&resID, &orgID); err != nil {
				rows.Close()
				log.Fatalf("[postgres] [check_manage_org_admin] scan failed: %v", err)
			}

			// Find any admin for this org (on-demand)
			var adminUser int
			err = db.QueryRowContext(context.Background(), `SELECT user_id FROM org_memberships WHERE org_id = $1 AND role = 'admin' LIMIT 1`, orgID).Scan(&adminUser)
			if err == sql.ErrNoRows {
				// skip
				continue
			}
			if err != nil {
				rows.Close()
				log.Fatalf("[postgres] [check_manage_org_admin] find admin failed: %v", err)
			}

			cstart := time.Now()
			var exists bool
			qctx, qcancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = db.QueryRowContext(qctx, `SELECT EXISTS(SELECT 1 FROM user_resource_permissions WHERE resource_id = $1 AND user_id = $2 AND relation = 'manager')`, resID, adminUser).Scan(&exists)
			qcancel()
			if err != nil {
				rows.Close()
				log.Fatalf("[postgres] [check_manage_org_admin] check query failed: %v", err)
			}
			dur := time.Since(cstart)
			if done%100 == 0 {
				log.Printf("[postgres] [check_manage_org_admin] iter=%d resource=%d org=%d admin=%d dur=%s", done, resID, orgID, adminUser, dur)
			}
			done++
		}
		rows.Close()
	}
	log.Printf("[postgres] [check_manage_org_admin] DONE: iters=%d", iters)
}

// runCheckViewViaGroupMember streams viewer_group ACLs and checks permission for a
// picked group member (direct_member_user or fallback manager) without collecting.
func runCheckViewViaGroupMember(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_CHECK_VIEW_GROUP_ITER", 1000)
	log.Printf("[postgres] [check_view_via_group_member] streaming mode. iterations=%d", iters)
	lookupUser := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	sampleLimit := utils.GetEnvInt("BENCH_LOOKUP_SAMPLE_LIMIT", 1000)
	done := 0

	for done < iters {
		if lookupUser != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			rows, err := db.QueryContext(ctx, `SELECT resource_id FROM user_resource_permissions WHERE user_id = $1 AND relation = 'viewer'`, lookupUser)
			if err != nil {
				cancel()
				log.Fatalf("[postgres] [check_view_via_group_member] lookup query failed: %v", err)
			}
			streamed := 0
			for rows.Next() {
				if done >= iters || streamed >= sampleLimit {
					break
				}
				var resID int
				if err := rows.Scan(&resID); err != nil {
					rows.Close()
					cancel()
					log.Fatalf("[postgres] [check_view_via_group_member] scan failed: %v", err)
				}
				streamed++

				cstart := time.Now()
				var exists bool
				qctx, qcancel := context.WithTimeout(context.Background(), 2*time.Second)
				err = db.QueryRowContext(qctx, `SELECT EXISTS(SELECT 1 FROM user_resource_permissions WHERE resource_id = $1 AND user_id = $2 AND relation = 'viewer')`, resID, lookupUser).Scan(&exists)
				qcancel()
				if err != nil {
					rows.Close()
					cancel()
					log.Fatalf("[postgres] [check_view_via_group_member] check query failed: %v", err)
				}
				dur := time.Since(cstart)
				if done%100 == 0 {
					log.Printf("[postgres] [check_view_via_group_member] lookup iter=%d resource=%d user=%s dur=%s", done, resID, lookupUser, dur)
				}
				done++
			}
			rows.Close()
			cancel()
			if streamed == 0 {
				log.Printf("[postgres] [check_view_via_group_member] lookup-mode: no resources returned for user=%s", lookupUser)
				lookupUser = ""
			}
			continue
		}

		// Relationship-driven: stream resource_acl for viewer groups
		rows, err := db.QueryContext(context.Background(), `SELECT resource_id, subject_id FROM resource_acl WHERE subject_type = 'group' AND (relation = 'viewer_group' OR relation = 'viewer')`)
		if err != nil {
			log.Fatalf("[postgres] [check_view_via_group_member] resource_acl query failed: %v", err)
		}
		for rows.Next() {
			if done >= iters {
				break
			}
			var resID, groupID int
			if err := rows.Scan(&resID, &groupID); err != nil {
				rows.Close()
				log.Fatalf("[postgres] [check_view_via_group_member] scan failed: %v", err)
			}

			// Find a direct member
			var pickedUser int
			err = db.QueryRowContext(context.Background(), `SELECT user_id FROM group_memberships WHERE group_id = $1 AND role = 'direct_member' LIMIT 1`, groupID).Scan(&pickedUser)
			if err == sql.ErrNoRows {
				// fallback to manager
				err = db.QueryRowContext(context.Background(), `SELECT user_id FROM group_memberships WHERE group_id = $1 AND role = 'direct_manager' LIMIT 1`, groupID).Scan(&pickedUser)
				if err == sql.ErrNoRows {
					continue
				}
			}
			if err != nil {
				rows.Close()
				log.Fatalf("[postgres] [check_view_via_group_member] find member failed: %v", err)
			}

			cstart := time.Now()
			var exists bool
			qctx, qcancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = db.QueryRowContext(qctx, `SELECT EXISTS(SELECT 1 FROM user_resource_permissions WHERE resource_id = $1 AND user_id = $2 AND relation = 'viewer')`, resID, pickedUser).Scan(&exists)
			qcancel()
			if err != nil {
				rows.Close()
				log.Fatalf("[postgres] [check_view_via_group_member] check query failed: %v", err)
			}
			dur := time.Since(cstart)
			if done%100 == 0 {
				log.Printf("[postgres] [check_view_via_group_member] iter=%d resource=%d group=%d user=%d dur=%s", done, resID, groupID, pickedUser, dur)
			}
			done++
		}
		rows.Close()
	}
	log.Printf("[postgres] [check_view_via_group_member] DONE: iters=%d", iters)
}

func runLookupResourcesManageHeavyUser(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_MANAGE_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_MANAGE_USER")
	runLookupBenchPG(db, "lookup_resources_manage_super", "manager", userID, iters, 60*time.Second)
}

func runLookupResourcesViewRegularUser(db *sql.DB) {
	iters := utils.GetEnvInt("BENCH_LOOKUPRES_VIEW_ITER", 10)
	userID := os.Getenv("BENCH_LOOKUPRES_VIEW_USER")
	runLookupBenchPG(db, "lookup_resources_view_regular", "viewer", userID, iters, 60*time.Second)
}
