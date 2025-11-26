package cockroachdb

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"test-tls/infrastructure"
)

const (
	dataDir          = "data"
	resourceACLBatch = 5000 // commit every N ACL rows
)

// CockroachdbLoadData loads CSV data into CockroachDB using UPSERT (idempotent).
// Logging format is aligned with authzed_crdb_load_data.go.
func CockroachdbCreateData() {
	// Use a short timeout only for establishing the connection.
	connCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	db, cleanup, err := infrastructure.NewCockroachDBFromEnv(connCtx)
	if err != nil {
		log.Fatalf("[cockroachdb] load_data: connect failed: %v", err)
	}
	defer cleanup()

	// Long-running load uses a background context (no artificial deadline).
	ctx := context.Background()

	start := time.Now()
	totalRows := 0

	log.Printf("[cockroachdb] == Starting CockroachDB data import from CSV in %q ==", dataDir)

	// Phase 1: organizations.csv -> organizations
	func() {
		const filename = "organizations.csv"
		path := filepath.Join(dataDir, filename)

		f, err := os.Open(path)
		if err != nil {
			log.Fatalf("[cockroachdb] open %s: %v", path, err)
		}
		defer f.Close()

		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			log.Fatalf("[cockroachdb] read %s header: %v", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}
		stmt, err := tx.PrepareContext(ctx,
			"UPSERT INTO organizations (org_id) VALUES ($1)",
		)
		if err != nil {
			log.Fatalf("[cockroachdb] prepare UPSERT organizations: %v", err)
		}

		count := 0
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[cockroachdb] read %s row: %v", filename, err)
			}
			if len(rec) < 1 {
				log.Fatalf("[cockroachdb] invalid %s row: %#v", filename, rec)
			}

			orgID, err := strconv.ParseInt(rec[0], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse org_id: %v", err)
			}

			if _, err := stmt.ExecContext(ctx, orgID); err != nil {
				log.Fatalf("[cockroachdb] upsert organizations: %v", err)
			}
			count++
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Fatalf("[cockroachdb] commit organizations: %v", err)
		}

		totalRows += count
		log.Printf("[cockroachdb] Loaded organizations: %d rows (cumulative=%d)", count, totalRows)
	}()

	// Phase 2: users.csv -> users
	func() {
		const filename = "users.csv"
		path := filepath.Join(dataDir, filename)

		f, err := os.Open(path)
		if err != nil {
			log.Fatalf("[cockroachdb] open %s: %v", path, err)
		}
		defer f.Close()

		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			log.Fatalf("[cockroachdb] read %s header: %v", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}
		stmt, err := tx.PrepareContext(ctx,
			"UPSERT INTO users (user_id, org_id) VALUES ($1, $2)",
		)
		if err != nil {
			log.Fatalf("[cockroachdb] prepare UPSERT users: %v", err)
		}

		count := 0
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[cockroachdb] read %s row: %v", filename, err)
			}
			if len(rec) < 2 {
				log.Fatalf("[cockroachdb] invalid %s row: %#v", filename, rec)
			}

			userID, err := strconv.ParseInt(rec[0], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse user_id: %v", err)
			}
			orgID, err := strconv.ParseInt(rec[1], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse org_id (user): %v", err)
			}

			if _, err := stmt.ExecContext(ctx, userID, orgID); err != nil {
				log.Fatalf("[cockroachdb] upsert users: %v", err)
			}
			count++
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Fatalf("[cockroachdb] commit users: %v", err)
		}

		totalRows += count
		log.Printf("[cockroachdb] Loaded users: %d rows (cumulative=%d)", count, totalRows)
	}()

	// Phase 3: groups.csv -> groups
	func() {
		const filename = "groups.csv"
		path := filepath.Join(dataDir, filename)

		f, err := os.Open(path)
		if err != nil {
			log.Fatalf("[cockroachdb] open %s: %v", path, err)
		}
		defer f.Close()

		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			log.Fatalf("[cockroachdb] read %s header: %v", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}
		stmt, err := tx.PrepareContext(ctx,
			"UPSERT INTO groups (group_id, org_id) VALUES ($1, $2)",
		)
		if err != nil {
			log.Fatalf("[cockroachdb] prepare UPSERT groups: %v", err)
		}

		count := 0
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[cockroachdb] read %s row: %v", filename, err)
			}
			if len(rec) < 2 {
				log.Fatalf("[cockroachdb] invalid %s row: %#v", filename, rec)
			}

			groupID, err := strconv.ParseInt(rec[0], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse group_id: %v", err)
			}
			orgID, err := strconv.ParseInt(rec[1], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse org_id (group): %v", err)
			}

			if _, err := stmt.ExecContext(ctx, groupID, orgID); err != nil {
				log.Fatalf("[cockroachdb] upsert groups: %v", err)
			}
			count++
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Fatalf("[cockroachdb] commit groups: %v", err)
		}

		totalRows += count
		log.Printf("[cockroachdb] Loaded groups: %d rows (cumulative=%d)", count, totalRows)
	}()

	// Phase 4: org_memberships.csv -> org_memberships
	func() {
		const filename = "org_memberships.csv"
		path := filepath.Join(dataDir, filename)

		f, err := os.Open(path)
		if err != nil {
			log.Fatalf("[cockroachdb] open %s: %v", path, err)
		}
		defer f.Close()

		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			log.Fatalf("[cockroachdb] read %s header: %v", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}
		stmt, err := tx.PrepareContext(ctx,
			"UPSERT INTO org_memberships (org_id, user_id, role) VALUES ($1, $2, $3)",
		)
		if err != nil {
			log.Fatalf("[cockroachdb] prepare UPSERT org_memberships: %v", err)
		}

		count := 0
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[cockroachdb] read %s row: %v", filename, err)
			}
			if len(rec) < 3 {
				log.Fatalf("[cockroachdb] invalid %s row: %#v", filename, rec)
			}

			orgID, err := strconv.ParseInt(rec[0], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse org_id (membership): %v", err)
			}
			userID, err := strconv.ParseInt(rec[1], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse user_id (membership): %v", err)
			}
			role := rec[2]

			if _, err := stmt.ExecContext(ctx, orgID, userID, role); err != nil {
				log.Fatalf("[cockroachdb] upsert org_memberships: %v", err)
			}
			count++
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Fatalf("[cockroachdb] commit org_memberships: %v", err)
		}

		totalRows += count
		log.Printf("[cockroachdb] Loaded org_memberships: %d rows (cumulative=%d)", count, totalRows)
	}()

	// Phase 5: group_memberships.csv -> group_memberships
	func() {
		const filename = "group_memberships.csv"
		path := filepath.Join(dataDir, filename)

		f, err := os.Open(path)
		if err != nil {
			log.Fatalf("[cockroachdb] open %s: %v", path, err)
		}
		defer f.Close()

		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			log.Fatalf("[cockroachdb] read %s header: %v", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}
		stmt, err := tx.PrepareContext(ctx,
			"UPSERT INTO group_memberships (group_id, user_id, role) VALUES ($1, $2, $3)",
		)
		if err != nil {
			log.Fatalf("[cockroachdb] prepare UPSERT group_memberships: %v", err)
		}

		count := 0
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[cockroachdb] read %s row: %v", filename, err)
			}
			if len(rec) < 3 {
				log.Fatalf("[cockroachdb] invalid %s row: %#v", filename, rec)
			}

			groupID, err := strconv.ParseInt(rec[0], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse group_id (membership): %v", err)
			}
			userID, err := strconv.ParseInt(rec[1], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse user_id (group membership): %v", err)
			}
			role := rec[2]

			if _, err := stmt.ExecContext(ctx, groupID, userID, role); err != nil {
				log.Fatalf("[cockroachdb] upsert group_memberships: %v", err)
			}
			count++
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Fatalf("[cockroachdb] commit group_memberships: %v", err)
		}

		totalRows += count
		log.Printf("[cockroachdb] Loaded group_memberships: %d rows (cumulative=%d)", count, totalRows)
	}()

	// Phase 6: group_hierarchy.csv -> group_hierarchy
	func() {
		const filename = "group_hierarchy.csv"
		path := filepath.Join(dataDir, filename)

		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				log.Printf("[cockroachdb] %s not found, skipping nested groups", filename)
				return
			}
			log.Fatalf("[cockroachdb] open %s: %v", path, err)
		}
		defer f.Close()

		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			log.Fatalf("[cockroachdb] read %s header: %v", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}
		stmt, err := tx.PrepareContext(ctx,
			"UPSERT INTO group_hierarchy (parent_group_id, child_group_id, relation) VALUES ($1, $2, $3)",
		)
		if err != nil {
			log.Fatalf("[cockroachdb] prepare UPSERT group_hierarchy: %v", err)
		}

		count := 0
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[cockroachdb] read %s row: %v", filename, err)
			}
			if len(rec) < 3 {
				log.Fatalf("[cockroachdb] invalid %s row: %#v", filename, rec)
			}

			parentID, err := strconv.ParseInt(rec[0], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse parent_group_id: %v", err)
			}
			childID, err := strconv.ParseInt(rec[1], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse child_group_id: %v", err)
			}
			relation := rec[2]

			if _, err := stmt.ExecContext(ctx, parentID, childID, relation); err != nil {
				log.Fatalf("[cockroachdb] upsert group_hierarchy: %v", err)
			}
			count++
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Fatalf("[cockroachdb] commit group_hierarchy: %v", err)
		}

		totalRows += count
		log.Printf("[cockroachdb] Loaded group_hierarchy: %d rows (cumulative=%d)", count, totalRows)
	}()

	// Phase 7: resources.csv -> resources
	func() {
		const filename = "resources.csv"
		path := filepath.Join(dataDir, filename)

		f, err := os.Open(path)
		if err != nil {
			log.Fatalf("[cockroachdb] open %s: %v", path, err)
		}
		defer f.Close()

		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			log.Fatalf("[cockroachdb] read %s header: %v", filename, err)
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}
		stmt, err := tx.PrepareContext(ctx,
			"UPSERT INTO resources (resource_id, org_id) VALUES ($1, $2)",
		)
		if err != nil {
			log.Fatalf("[cockroachdb] prepare UPSERT resources: %v", err)
		}

		count := 0
		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[cockroachdb] read %s row: %v", filename, err)
			}
			if len(rec) < 2 {
				log.Fatalf("[cockroachdb] invalid %s row: %#v", filename, rec)
			}

			resourceID, err := strconv.ParseInt(rec[0], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse resource_id: %v", err)
			}
			orgID, err := strconv.ParseInt(rec[1], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse org_id (resource): %v", err)
			}

			if _, err := stmt.ExecContext(ctx, resourceID, orgID); err != nil {
				log.Fatalf("[cockroachdb] upsert resources: %v", err)
			}
			count++
		}

		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Fatalf("[cockroachdb] commit resources: %v", err)
		}

		totalRows += count
		log.Printf("[cockroachdb] Loaded resources: %d rows (cumulative=%d)", count, totalRows)
	}()

	// Phase 8: resource_acl.csv -> resource_acl (chunked transactions)
	func() {
		const filename = "resource_acl.csv"
		path := filepath.Join(dataDir, filename)

		f, err := os.Open(path)
		if err != nil {
			log.Fatalf("[cockroachdb] open %s: %v", path, err)
		}
		defer f.Close()

		r := csv.NewReader(f)
		if _, err := r.Read(); err != nil {
			log.Fatalf("[cockroachdb] read %s header: %v", filename, err)
		}

		sqlStmt := "UPSERT INTO resource_acl (resource_id, subject_type, subject_id, relation) VALUES ($1, $2, $3, $4)"

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}
		stmt, err := tx.PrepareContext(ctx, sqlStmt)
		if err != nil {
			log.Fatalf("[cockroachdb] prepare UPSERT resource_acl: %v", err)
		}

		count := 0
		rowsInTxn := 0

		commitAndReset := func() {
			stmt.Close()
			if err := tx.Commit(); err != nil {
				log.Fatalf("[cockroachdb] commit resource_acl batch: %v", err)
			}

			var err error
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				log.Fatalf("[cockroachdb] begin tx resource_acl (next batch): %v", err)
			}
			stmt, err = tx.PrepareContext(ctx, sqlStmt)
			if err != nil {
				log.Fatalf("[cockroachdb] prepare UPSERT resource_acl (next batch): %v", err)
			}
			rowsInTxn = 0
		}

		for {
			rec, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("[cockroachdb] read %s row: %v", filename, err)
			}
			if len(rec) < 4 {
				log.Fatalf("[cockroachdb] invalid %s row: %#v", filename, rec)
			}

			resourceID, err := strconv.ParseInt(rec[0], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse resource_id (ACL): %v", err)
			}
			subjectType := rec[1]
			subjectID, err := strconv.ParseInt(rec[2], 10, 64)
			if err != nil {
				log.Fatalf("[cockroachdb] parse subject_id: %v", err)
			}
			relation := rec[3]

			if _, err := stmt.ExecContext(ctx, resourceID, subjectType, subjectID, relation); err != nil {
				log.Fatalf("[cockroachdb] upsert resource_acl: %v", err)
			}
			count++
			rowsInTxn++

			if count%50000 == 0 {
				log.Printf("[cockroachdb] ... resource_acl progress: %d rows", count)
			}

			if rowsInTxn >= resourceACLBatch {
				commitAndReset()
			}
		}

		// Commit any remaining rows in the last batch.
		stmt.Close()
		if err := tx.Commit(); err != nil {
			log.Fatalf("[cockroachdb] commit resource_acl final batch: %v", err)
		}

		totalRows += count
		log.Printf("[cockroachdb] Loaded resource_acl: %d rows (cumulative=%d)", count, totalRows)
	}()

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[cockroachdb] CockroachDB data import DONE: totalRows=%d elapsed=%s", totalRows, elapsed)
}
