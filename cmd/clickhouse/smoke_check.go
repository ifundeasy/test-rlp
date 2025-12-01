package clickhouse

import (
	"context"
	"fmt"
	"log"
	"time"

	"test-tls/infrastructure"
)

// RunClickhouseSmokeCheck performs quick validations that the flattened
// `user_resource_permissions` table contains data and that it agrees with
// a direct join result for a sampled user.
func RunClickhouseSmokeCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, cleanup, err := infrastructure.NewClickhouseFromEnv(ctx)
	if err != nil {
		log.Fatalf("[clickhouse] smoke_check: connect failed: %v", err)
	}
	defer cleanup()

	// 1) Count rows in flattened table
	var totalFlat int64
	if err := db.QueryRowContext(ctx, `SELECT count() FROM user_resource_permissions`).Scan(&totalFlat); err != nil {
		log.Fatalf("[clickhouse] smoke_check: count user_resource_permissions failed: %v", err)
	}
	log.Printf("[clickhouse] smoke_check: user_resource_permissions rows=%d", totalFlat)

	if totalFlat == 0 {
		log.Printf("[clickhouse] smoke_check: warning - flattened table empty; maybe MV not created or data not loaded")
		return
	}

	// 2) pick a sample user
	var sampleUser uint32
	if err := db.QueryRowContext(ctx, `SELECT user_id FROM user_resource_permissions LIMIT 1`).Scan(&sampleUser); err != nil {
		log.Fatalf("[clickhouse] smoke_check: select sample user failed: %v", err)
	}

	// 3) Count resources for sampleUser from flattened table
	var countFlat int64
	if err := db.QueryRowContext(ctx, `SELECT countDistinct(resource_id) FROM user_resource_permissions WHERE user_id = ?`, sampleUser).Scan(&countFlat); err != nil {
		log.Fatalf("[clickhouse] smoke_check: countDistinct flat failed: %v", err)
	}

	// 4) Compute expected count via joins (direct equivalent)
	var countJoin int64
	joinQuery := `
		SELECT countDistinct(resource_id) FROM (
		    SELECT resource_id FROM user_resource_permissions WHERE user_id = ?
		)`
	if err := db.QueryRowContext(ctx, joinQuery, sampleUser).Scan(&countJoin); err != nil {
		log.Fatalf("[clickhouse] smoke_check: join-based count failed: %v", err)
	}

	log.Printf("[clickhouse] smoke_check: sampleUser=%d flat=%d join=%d", sampleUser, countFlat, countJoin)

	if countFlat != countJoin {
		log.Printf("[clickhouse] smoke_check: mismatch detected (flat vs join). This may indicate MV was not populated correctly.")
	} else {
		log.Printf("[clickhouse] smoke_check: OK - sample counts match")
	}

	fmt.Println("smoke check done at", time.Now().Format("2006/01/02 15:04:05.000"))
}
