package cockroachdb

import (
	"context"
	"log"
	"time"

	"test-tls/infrastructure"
)

func CockroachdbRefreshUserResourcePermissions() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	db, cleanup, err := infrastructure.NewCockroachDBFromEnv(ctx)
	if err != nil {
		log.Fatalf("[cockroachdb] refresh_mv: connect failed: %v", err)
	}
	defer cleanup()

	log.Println("[cockroachdb] REFRESH MATERIALIZED VIEW user_resource_permissions ...")

	if _, err := db.ExecContext(ctx, `REFRESH MATERIALIZED VIEW user_resource_permissions`); err != nil {
		log.Fatalf("[cockroachdb] refresh_mv: REFRESH MATERIALIZED VIEW failed: %v", err)
	}

	log.Println("[cockroachdb] REFRESH MATERIALIZED VIEW user_resource_permissions DONE")
}
