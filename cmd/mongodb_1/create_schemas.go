package mongodb_1

import (
	"context"
	"log"
	"time"

	"test-tls/infrastructure"

	"go.mongodb.org/mongo-driver/bson"
)

// MongoCreateSchemas creates collections and indexes for the MongoDB-based
// benchmarks.
//
// Collections (normalized, 1:1 with CSV):
//   - organizations:     { _id: org_id }
//   - users:             { _id: user_id, org_id }
//   - groups:            { _id: group_id, org_id }
//   - org_memberships:   { org_id, user_id, role }        // role in {member,admin}
//   - group_memberships: { group_id, user_id, role }      // role currently "member"
//   - resources:         { _id: resource_id, org_id }
//   - resource_acl:      { resource_id, subject_type, subject_id, relation }
//
// Collection denormalized (Mongo-optimized):
//
//   - user_resource_perms:
//     { user_id, resource_id, can_manage, can_view }
//
//     Populated in load_data.go with precomputed policy results:
//
//     can_manage(user, resource) =
//     direct manager_user
//     OR via manager_groups
//     OR via org admin
//
//     can_view(user, resource) =
//     can_manage
//     OR direct viewer_user
//     OR via viewer_groups
//     OR via org member
//
// The query benchmark will only hit `user_resource_perms` with aggressive indexing.
func MongodbCreateSchemas() {
	ctx := context.Background()

	_, db, cleanup, err := infrastructure.NewMongoFromEnv(ctx)
	if err != nil {
		log.Fatalf("[mongodb_1] failed to create mongo client: %v", err)
	}
	defer cleanup()

	log.Printf("[mongodb_1] == Creating MongoDB collections and indexes ==")

	opTimeout := 30 * time.Second

	// Explicitly create collections. If a collection already exists, log and continue.
	collections := []string{
		"organizations",
		"users",
		"groups",
		"org_memberships",
		"group_memberships",
		"resources",
		"resource_acl",
		"user_resource_perms",
	}

	for _, name := range collections {
		ctxCreate, cancel := context.WithTimeout(ctx, opTimeout)
		err := db.CreateCollection(ctxCreate, name)
		cancel()
		if err != nil {
			// most likely: (NamespaceExists) collection already exists
			log.Printf("[mongodb_1] CreateCollection %s: %v (continuing)", name, err)
		} else {
			log.Printf("[mongodb_1] Created collection: %s", name)
		}
	}

	// 1) organizations: _id = org_id  (default _id index sudah cukup)

	// 2) users: { _id: user_id, org_id }
	{
		coll := db.Collection("users")
		indexes := []MongoIndexSpec{
			{
				Name: "ix_users_org_id",
				Keys: bson.D{{Key: "org_id", Value: 1}},
			},
		}
		CreateIndexesWithLog(ctx, coll, indexes, opTimeout, "users")
	}

	// 3) groups: { _id: group_id, org_id }
	{
		coll := db.Collection("groups")
		indexes := []MongoIndexSpec{
			{
				Name: "ix_groups_org_id",
				Keys: bson.D{{Key: "org_id", Value: 1}},
			},
		}
		CreateIndexesWithLog(ctx, coll, indexes, opTimeout, "groups")
	}

	// 4) org_memberships: { org_id, user_id, role }
	{
		coll := db.Collection("org_memberships")
		indexes := []MongoIndexSpec{
			{
				// For: "is user U admin/member in org O?"
				Name:   "ix_om_org_user",
				Keys:   bson.D{{Key: "org_id", Value: 1}, {Key: "user_id", Value: 1}},
				Unique: true,
			},
			{
				// For: "get all orgs belonging to user U" (if needed)
				Name: "ix_om_user_org",
				Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "org_id", Value: 1}},
			},
			{
				// For: "list all admins in org O"
				Name: "ix_om_org_role",
				Keys: bson.D{{Key: "org_id", Value: 1}, {Key: "role", Value: 1}},
			},
		}
		CreateIndexesWithLog(ctx, coll, indexes, opTimeout, "org_memberships")
	}

	// 5) group_memberships: { group_id, user_id, role }
	{
		coll := db.Collection("group_memberships")
		indexes := []MongoIndexSpec{
			{
				// expand group -> users
				Name: "ix_gm_group_user",
				Keys: bson.D{{Key: "group_id", Value: 1}, {Key: "user_id", Value: 1}},
			},
			{
				// expand user -> groups
				Name: "ix_gm_user_group",
				Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "group_id", Value: 1}},
			},
		}
		CreateIndexesWithLog(ctx, coll, indexes, opTimeout, "group_memberships")
	}

	// 6) resources: { _id: resource_id, org_id }
	{
		coll := db.Collection("resources")
		indexes := []MongoIndexSpec{
			{
				// org -> resources
				Name: "ix_resources_org_id",
				Keys: bson.D{{Key: "org_id", Value: 1}},
			},
		}
		CreateIndexesWithLog(ctx, coll, indexes, opTimeout, "resources")
	}

	// 7) resource_acl: { resource_id, subject_type, subject_id, relation }
	//    This is still useful as the source of truth before being compiled into user_resource_perms.
	{
		coll := db.Collection("resource_acl")
		indexes := []MongoIndexSpec{
			{
				// For: "does subject S have relation R on resource X?"
				Name: "ix_racl_res_subject_rel",
				Keys: bson.D{
					{Key: "resource_id", Value: 1},
					{Key: "subject_type", Value: 1},
					{Key: "subject_id", Value: 1},
					{Key: "relation", Value: 1},
				},
			},
			{
				// For: "list resources of subject S with relation R"
				Name: "ix_racl_subject_rel_res",
				Keys: bson.D{
					{Key: "subject_type", Value: 1},
					{Key: "subject_id", Value: 1},
					{Key: "relation", Value: 1},
					{Key: "resource_id", Value: 1},
				},
			},
		}
		CreateIndexesWithLog(ctx, coll, indexes, opTimeout, "resource_acl")
	}

	// 8) user_resource_perms: { user_id, resource_id, can_manage, can_view }
	//    This is used for the "Mongo full power" benchmark:
	//      - check manage: findOne({user_id, resource_id, can_manage:true})
	//      - check view:   findOne({user_id, resource_id, can_view:true})
	//      - list manage:  find({user_id, can_manage:true}).sort({resource_id:1})
	//      - list view:    find({user_id, can_view:true}).sort({resource_id:1})
	{
		coll := db.Collection("user_resource_perms")
		indexes := []MongoIndexSpec{
			{
				// ensure consistency: one user-resource row only
				Name:   "ux_urp_user_resource",
				Keys:   bson.D{{Key: "user_id", Value: 1}, {Key: "resource_id", Value: 1}},
				Unique: true,
			},
			{
				// for check/list manage
				Name: "ix_urp_user_manage_res",
				Keys: bson.D{
					{Key: "user_id", Value: 1},
					{Key: "can_manage", Value: 1},
					{Key: "resource_id", Value: 1},
				},
			},
			{
				// for check/list view
				Name: "ix_urp_user_view_res",
				Keys: bson.D{
					{Key: "user_id", Value: 1},
					{Key: "can_view", Value: 1},
					{Key: "resource_id", Value: 1},
				},
			},
		}
		CreateIndexesWithLog(ctx, coll, indexes, opTimeout, "user_resource_perms")
	}

	log.Printf("[mongodb_1] MongoDB schema and indexes created")
}
