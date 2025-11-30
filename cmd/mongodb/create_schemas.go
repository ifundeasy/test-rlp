package mongodb

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"test-tls/infrastructure"
)

// MongodbCreateSchemas creates MongoDB collections and indexes optimized for
// fast permission lookups using native multikey indexes on embedded arrays.
// Collections:
// - organizations: org-level admins/members
// - users: cross-org user membership hints
// - groups: direct members/managers and nested groups
// - resources: org, direct ACLs (users/groups) for manage/view
//
// Note: Actual document shapes are populated by load_data; here we ensure
// collections exist and indexes support nested arrays for quick lookups.
func MongodbCreateSchemas() {
	parent := context.Background()

	client, db, cleanup, err := infrastructure.NewMongoFromEnv(parent)
	if err != nil {
		log.Fatalf("[mongodb] connect error: %v", err)
		return
	}
	defer cleanup()

	// Common timeout for index creation.
	idxTimeout := 30 * time.Second

	// Helper: ensure collection exists (Mongo will create on first use; we touch it).
	ensureColl := func(name string) *mongo.Collection {
		c := db.Collection(name)
		// no-op: write concern not needed; creation happens with first insert.
		return c
	}

	// organizations: { org_id, admin_user_ids[], admin_group_ids[], member_user_ids[], member_group_ids[] }
	orgs := ensureColl("organizations")
	CreateIndexesWithLog(parent, orgs, []MongoIndexSpec{
		{Name: "org_id_unique", Keys: bson.D{{Key: "org_id", Value: 1}}, Unique: true},
		{Name: "admin_user_ids_idx", Keys: bson.D{{Key: "admin_user_ids", Value: 1}}},
		{Name: "admin_group_ids_idx", Keys: bson.D{{Key: "admin_group_ids", Value: 1}}},
		{Name: "member_user_ids_idx", Keys: bson.D{{Key: "member_user_ids", Value: 1}}},
		{Name: "member_group_ids_idx", Keys: bson.D{{Key: "member_group_ids", Value: 1}}},
	}, idxTimeout, "organizations")

	// users: { user_id, org_ids[], group_ids[] }
	users := ensureColl("users")
	CreateIndexesWithLog(parent, users, []MongoIndexSpec{
		{Name: "user_id_unique", Keys: bson.D{{Key: "user_id", Value: 1}}, Unique: true},
		{Name: "org_ids_idx", Keys: bson.D{{Key: "org_ids", Value: 1}}},
		{Name: "group_ids_idx", Keys: bson.D{{Key: "group_ids", Value: 1}}},
	}, idxTimeout, "users")

	// groups: { group_id, org_id, direct_member_user_ids[], direct_manager_user_ids[], member_group_ids[], manager_group_ids[] }
	groups := ensureColl("groups")
	CreateIndexesWithLog(parent, groups, []MongoIndexSpec{
		{Name: "group_id_unique", Keys: bson.D{{Key: "group_id", Value: 1}}, Unique: true},
		{Name: "org_id_idx", Keys: bson.D{{Key: "org_id", Value: 1}}},
		{Name: "direct_member_users_idx", Keys: bson.D{{Key: "direct_member_user_ids", Value: 1}}},
		{Name: "direct_manager_users_idx", Keys: bson.D{{Key: "direct_manager_user_ids", Value: 1}}},
		{Name: "member_group_ids_idx", Keys: bson.D{{Key: "member_group_ids", Value: 1}}},
		{Name: "manager_group_ids_idx", Keys: bson.D{{Key: "manager_group_ids", Value: 1}}},
	}, idxTimeout, "groups")

	// resources: { resource_id, org_id, manager_user_ids[], viewer_user_ids[], manager_group_ids[], viewer_group_ids[] }
	resources := ensureColl("resources")
	CreateIndexesWithLog(parent, resources, []MongoIndexSpec{
		{Name: "resource_id_unique", Keys: bson.D{{Key: "resource_id", Value: 1}}, Unique: true},
		{Name: "org_id_idx", Keys: bson.D{{Key: "org_id", Value: 1}}},
		// Single-field multikey indexes to accelerate direct lookups without org filter
		{Name: "manager_user_ids_idx", Keys: bson.D{{Key: "manager_user_ids", Value: 1}}},
		{Name: "viewer_user_ids_idx", Keys: bson.D{{Key: "viewer_user_ids", Value: 1}}},
		{Name: "manager_group_ids_idx", Keys: bson.D{{Key: "manager_group_ids", Value: 1}}},
		{Name: "viewer_group_ids_idx", Keys: bson.D{{Key: "viewer_group_ids", Value: 1}}},
		// Fast lookups: by user or group membership within an org
		{Name: "org_manage_user_idx", Keys: bson.D{{Key: "org_id", Value: 1}, {Key: "manager_user_ids", Value: 1}}},
		{Name: "org_view_user_idx", Keys: bson.D{{Key: "org_id", Value: 1}, {Key: "viewer_user_ids", Value: 1}}},
		{Name: "org_manage_group_idx", Keys: bson.D{{Key: "org_id", Value: 1}, {Key: "manager_group_ids", Value: 1}}},
		{Name: "org_view_group_idx", Keys: bson.D{{Key: "org_id", Value: 1}, {Key: "viewer_group_ids", Value: 1}}},
	}, idxTimeout, "resources")

	log.Printf("[mongodb] schema creation complete: organizations, users, groups, resources")
	_ = client // referenced via cleanup; kept for future extension
}
