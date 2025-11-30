package cockroachdb

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"test-tls/infrastructure"
)

const (
	dataDir          = "data"
	resourceACLBatch = 5000 // commit every N ACL rows
	insertBatchSize  = 5000 // multi-row insert batch size
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
		defer tx.Rollback()

		count := 0
		// batching slices
		args := make([]interface{}, 0, insertBatchSize)
		placeholders := make([]string, 0, insertBatchSize)
		batchCount := 0

		flush := func() {
			if batchCount == 0 {
				return
			}
			query := fmt.Sprintf("INSERT INTO organizations (org_id) VALUES %s ON CONFLICT (org_id) DO NOTHING", strings.Join(placeholders, ","))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				log.Fatalf("[cockroachdb] upsert organizations batch failed: %v", err)
			}
			// reset
			args = args[:0]
			placeholders = placeholders[:0]
			batchCount = 0
		}

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

			args = append(args, orgID)
			// single-column placeholder (use current args length)
			placeholders = append(placeholders, fmt.Sprintf("($%d)", len(args)))
			batchCount++
			count++

			if batchCount >= insertBatchSize {
				flush()
			}
		}

		flush()

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
		defer tx.Rollback()

		count := 0
		args := make([]interface{}, 0, insertBatchSize*2)
		placeholders := make([]string, 0, insertBatchSize)
		batchCount := 0

		flush := func() {
			if batchCount == 0 {
				return
			}
			query := fmt.Sprintf("INSERT INTO users (user_id, org_id) VALUES %s ON CONFLICT (user_id) DO UPDATE SET org_id = EXCLUDED.org_id", strings.Join(placeholders, ","))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				log.Fatalf("[cockroachdb] upsert users batch failed: %v", err)
			}
			args = args[:0]
			placeholders = placeholders[:0]
			batchCount = 0
		}

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

			args = append(args, userID, orgID)
			// create placeholders like ($1,$2),($3,$4)... using current args length
			cur := len(args)
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d)", cur-1, cur))
			batchCount++
			count++

			if batchCount >= insertBatchSize {
				flush()
			}
		}

		flush()

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
		defer tx.Rollback()

		count := 0
		args := make([]interface{}, 0, insertBatchSize*2)
		placeholders := make([]string, 0, insertBatchSize)
		batchCount := 0

		flush := func() {
			if batchCount == 0 {
				return
			}
			query := fmt.Sprintf("INSERT INTO groups (group_id, org_id) VALUES %s ON CONFLICT (group_id) DO UPDATE SET org_id = EXCLUDED.org_id", strings.Join(placeholders, ","))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				log.Fatalf("[cockroachdb] upsert groups batch failed: %v", err)
			}
			args = args[:0]
			placeholders = placeholders[:0]
			batchCount = 0
		}

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

			args = append(args, groupID, orgID)
			cur := len(args)
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d)", cur-1, cur))
			batchCount++
			count++

			if batchCount >= insertBatchSize {
				flush()
			}
		}

		flush()

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
		defer tx.Rollback()

		count := 0
		args := make([]interface{}, 0, insertBatchSize*3)
		placeholders := make([]string, 0, insertBatchSize)
		batchCount := 0

		flush := func() {
			if batchCount == 0 {
				return
			}
			query := fmt.Sprintf("INSERT INTO org_memberships (org_id, user_id, role) VALUES %s ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role", strings.Join(placeholders, ","))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				log.Fatalf("[cockroachdb] upsert org_memberships batch failed: %v", err)
			}
			args = args[:0]
			placeholders = placeholders[:0]
			batchCount = 0
		}

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

			args = append(args, orgID, userID, role)
			cur := len(args)
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d)", cur-2, cur-1, cur))
			batchCount++
			count++

			if batchCount >= insertBatchSize {
				flush()
			}
		}

		flush()

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
		defer tx.Rollback()

		count := 0
		args := make([]interface{}, 0, insertBatchSize*3)
		placeholders := make([]string, 0, insertBatchSize)
		batchCount := 0

		flush := func() {
			if batchCount == 0 {
				return
			}
			query := fmt.Sprintf("INSERT INTO group_memberships (group_id, user_id, role) VALUES %s ON CONFLICT (group_id, user_id) DO UPDATE SET role = EXCLUDED.role", strings.Join(placeholders, ","))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				log.Fatalf("[cockroachdb] upsert group_memberships batch failed: %v", err)
			}
			args = args[:0]
			placeholders = placeholders[:0]
			batchCount = 0
		}

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

			args = append(args, groupID, userID, role)
			cur := len(args)
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d)", cur-2, cur-1, cur))
			batchCount++
			count++

			if batchCount >= insertBatchSize {
				flush()
			}
		}

		flush()

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
		defer tx.Rollback()

		count := 0
		args := make([]interface{}, 0, insertBatchSize*3)
		placeholders := make([]string, 0, insertBatchSize)
		batchCount := 0

		flush := func() {
			if batchCount == 0 {
				return
			}
			query := fmt.Sprintf("INSERT INTO group_hierarchy (parent_group_id, child_group_id, relation) VALUES %s ON CONFLICT (parent_group_id, child_group_id, relation) DO NOTHING", strings.Join(placeholders, ","))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				log.Fatalf("[cockroachdb] upsert group_hierarchy batch failed: %v", err)
			}
			args = args[:0]
			placeholders = placeholders[:0]
			batchCount = 0
		}

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

			args = append(args, parentID, childID, relation)
			cur := len(args)
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d)", cur-2, cur-1, cur))
			batchCount++
			count++

			if batchCount >= insertBatchSize {
				flush()
			}
		}

		flush()

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
		defer tx.Rollback()

		count := 0
		args := make([]interface{}, 0, insertBatchSize*2)
		placeholders := make([]string, 0, insertBatchSize)
		batchCount := 0

		flush := func() {
			if batchCount == 0 {
				return
			}
			query := fmt.Sprintf("INSERT INTO resources (resource_id, org_id) VALUES %s ON CONFLICT (resource_id) DO UPDATE SET org_id = EXCLUDED.org_id", strings.Join(placeholders, ","))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				log.Fatalf("[cockroachdb] upsert resources batch failed: %v", err)
			}
			args = args[:0]
			placeholders = placeholders[:0]
			batchCount = 0
		}

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

			args = append(args, resourceID, orgID)
			cur := len(args)
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d)", cur-1, cur))
			batchCount++
			count++

			if batchCount >= insertBatchSize {
				flush()
			}
		}

		flush()

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

		// We'll batch INSERT resource_acl rows in multi-row statements per transaction
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			log.Fatalf("[cockroachdb] begin tx %s: %v", filename, err)
		}

		count := 0
		rowsInTxn := 0

		// batching buffers for current txn
		args := make([]interface{}, 0, resourceACLBatch*4)
		placeholders := make([]string, 0, resourceACLBatch)

		commitAndReset := func() {
			if rowsInTxn > 0 {
				query := fmt.Sprintf("INSERT INTO resource_acl (resource_id, subject_type, subject_id, relation) VALUES %s ON CONFLICT (resource_id, subject_type, subject_id, relation) DO NOTHING", strings.Join(placeholders, ","))
				if _, err := tx.ExecContext(ctx, query, args...); err != nil {
					log.Fatalf("[cockroachdb] commit resource_acl batch failed: %v", err)
				}
			}
			if err := tx.Commit(); err != nil {
				log.Fatalf("[cockroachdb] commit resource_acl batch: %v", err)
			}

			// reset txn and buffers
			tx, err = db.BeginTx(ctx, nil)
			if err != nil {
				log.Fatalf("[cockroachdb] begin tx resource_acl (next batch): %v", err)
			}
			args = args[:0]
			placeholders = placeholders[:0]
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

			args = append(args, resourceID, subjectType, subjectID, relation)
			cur := len(args)
			// placeholders use 1-based parameter indexing
			placeholders = append(placeholders, fmt.Sprintf("($%d,$%d,$%d,$%d)", cur-3, cur-2, cur-1, cur))
			count++
			rowsInTxn++

			if count%resourceACLBatch == 0 {
				log.Printf("[cockroachdb] Loaded resource_acl progress: %d rows (cumulative=%d) elapsed=%s", count, totalRows+count, time.Since(start).Truncate(time.Millisecond))
			}

			if rowsInTxn >= resourceACLBatch {
				commitAndReset()
			}
		}

		// Commit any remaining rows in the last batch.
		commitAndReset()

		totalRows += count
		log.Printf("[cockroachdb] Loaded resource_acl: %d rows (cumulative=%d) elapsed=%s", count, totalRows, time.Since(start).Truncate(time.Millisecond))
	}()

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[cockroachdb] CockroachDB data import DONE: totalRows=%d elapsed=%s", totalRows, elapsed)
}
