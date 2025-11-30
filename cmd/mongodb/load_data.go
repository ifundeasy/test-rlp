package mongodb

import (
	"context"
	"encoding/csv"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"test-tls/infrastructure"
)

const (
	dataDir   = "data"
	batchSize = 10000
)

func openCSV(name string) (*csv.Reader, *os.File) {
	full := filepath.Join(dataDir, name)
	f, err := os.Open(full)
	if err != nil {
		log.Fatalf("[mongodb] open %s: %v", full, err)
	}
	return csv.NewReader(f), f
}

// MongodbCreateData ingests all CSVs into MongoDB using bulk upserts, mapping
// to the denormalized schema in create_schemas.go.
func MongodbCreateData() {
	client, db, cleanup, err := infrastructure.NewMongoFromEnv(context.Background())
	if err != nil {
		log.Fatalf("[mongodb] connect error: %v", err)
		return
	}
	defer cleanup()

	start := time.Now()
	log.Printf("[mongodb] == Starting Mongo data import from CSV in %q ==", dataDir)

	upsertOrgs(db, start)
	upsertGroups(db, start)
	upsertGroupMemberships(db, start)
	upsertGroupHierarchy(db, start)
	upsertResources(db, start)
	upsertResourceACL(db, start)

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[mongodb] Mongo data import DONE: elapsed=%s", elapsed)
	_ = client
}

// organizations: accumulate admin/member arrays per org
func upsertOrgs(db *mongo.Database, start time.Time) {
	r, f := openCSV("org_memberships.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[mongodb] read org_memberships header: %v", err)
	}

	coll := db.Collection("organizations")
	writes := make([]mongo.WriteModel, 0, batchSize)
	count := 0

	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[mongodb] read org_memberships row: %v", err)
		}
		if len(rec) < 3 {
			log.Fatalf("[mongodb] invalid org_memberships row: %#v", rec)
		}

		orgID := rec[0]
		userID := rec[1]
		role := rec[2]

		update := bson.D{}
		switch role {
		case "admin":
			update = bson.D{{Key: "$addToSet", Value: bson.D{{Key: "admin_user_ids", Value: userID}}}}
		default:
			update = bson.D{{Key: "$addToSet", Value: bson.D{{Key: "member_user_ids", Value: userID}}}}
		}

		// Ensure base doc with unique org_id
		base := bson.D{{Key: "$setOnInsert", Value: bson.D{{Key: "org_id", Value: orgID}}}}
		// Merge operations: add arrays + setOnInsert
		merged := bson.D{}
		for _, el := range update {
			merged = append(merged, el)
		}
		for _, el := range base {
			merged = append(merged, el)
		}

		writes = append(writes, &mongo.UpdateOneModel{
			Filter: bson.D{{Key: "org_id", Value: orgID}},
			Update: merged,
			Upsert: boolPtr(true),
		})
		count++
		if len(writes) >= batchSize {
			bulkExec(coll, writes)
			writes = writes[:0]
		}
		if count%10000 == 0 {
			log.Printf("[mongodb] Loaded org_memberships progress: %d rows elapsed=%s", count, time.Since(start).Truncate(time.Millisecond))
		}
	}
	if len(writes) > 0 {
		bulkExec(coll, writes)
	}
	log.Printf("[mongodb] Loaded org_memberships: %d rows", count)
}

// groups.csv -> create group doc with org_id
func upsertGroups(db *mongo.Database, start time.Time) {
	r, f := openCSV("groups.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[mongodb] read groups header: %v", err)
	}
	coll := db.Collection("groups")
	writes := make([]mongo.WriteModel, 0, batchSize)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[mongodb] read groups row: %v", err)
		}
		if len(rec) < 2 {
			log.Fatalf("[mongodb] invalid groups row: %#v", rec)
		}
		groupID := rec[0]
		orgID := rec[1]
		writes = append(writes, &mongo.UpdateOneModel{
			Filter: bson.D{{Key: "group_id", Value: groupID}},
			Update: bson.D{{Key: "$set", Value: bson.D{{Key: "group_id", Value: groupID}, {Key: "org_id", Value: orgID}}}},
			Upsert: boolPtr(true),
		})
		count++
		if len(writes) >= batchSize {
			bulkExec(coll, writes)
			writes = writes[:0]
		}
		if count%10000 == 0 {
			log.Printf("[mongodb] Loaded groups progress: %d rows elapsed=%s", count, time.Since(start).Truncate(time.Millisecond))
		}
	}
	if len(writes) > 0 {
		bulkExec(coll, writes)
	}
	log.Printf("[mongodb] Loaded groups: %d rows", count)
}

// group_memberships.csv -> add to direct_member_user_ids or direct_manager_user_ids
func upsertGroupMemberships(db *mongo.Database, start time.Time) {
	r, f := openCSV("group_memberships.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[mongodb] read group_memberships header: %v", err)
	}
	coll := db.Collection("groups")
	writes := make([]mongo.WriteModel, 0, batchSize)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[mongodb] read group_memberships row: %v", err)
		}
		if len(rec) < 3 {
			log.Fatalf("[mongodb] invalid group_memberships row: %#v", rec)
		}
		groupID := rec[0]
		userID := rec[1]
		role := rec[2]

		var field string
		switch role {
		case "direct_manager", "admin":
			field = "direct_manager_user_ids"
		default:
			field = "direct_member_user_ids"
		}

		writes = append(writes, &mongo.UpdateOneModel{
			Filter: bson.D{{Key: "group_id", Value: groupID}},
			Update: bson.D{
				{Key: "$setOnInsert", Value: bson.D{{Key: "group_id", Value: groupID}}},
				{Key: "$addToSet", Value: bson.D{{Key: field, Value: userID}}},
			},
			Upsert: boolPtr(true),
		})
		count++
		if len(writes) >= batchSize {
			bulkExec(coll, writes)
			writes = writes[:0]
		}
		if count%10000 == 0 {
			log.Printf("[mongodb] Loaded group_memberships progress: %d rows elapsed=%s", count, time.Since(start).Truncate(time.Millisecond))
		}
	}
	if len(writes) > 0 {
		bulkExec(coll, writes)
	}
	log.Printf("[mongodb] Loaded group_memberships: %d rows", count)
}

// group_hierarchy.csv -> member_group_ids or manager_group_ids
func upsertGroupHierarchy(db *mongo.Database, start time.Time) {
	r, f := openCSV("group_hierarchy.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		// File may not exist; mirror authzed logging
		if os.IsNotExist(err) {
			log.Printf("[mongodb] group_hierarchy.csv not found, skipping nested groups")
			return
		}
		log.Fatalf("[mongodb] read group_hierarchy header: %v", err)
	}
	coll := db.Collection("groups")
	writes := make([]mongo.WriteModel, 0, batchSize)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[mongodb] read group_hierarchy row: %v", err)
		}
		if len(rec) < 3 {
			log.Fatalf("[mongodb] invalid group_hierarchy row: %#v", rec)
		}
		parent := rec[0]
		child := rec[1]
		relation := rec[2]

		field := "member_group_ids"
		if relation == "manager_group" {
			field = "manager_group_ids"
		}

		writes = append(writes, &mongo.UpdateOneModel{
			Filter: bson.D{{Key: "group_id", Value: parent}},
			Update: bson.D{
				{Key: "$setOnInsert", Value: bson.D{{Key: "group_id", Value: parent}}},
				{Key: "$addToSet", Value: bson.D{{Key: field, Value: child}}},
			},
			Upsert: boolPtr(true),
		})
		count++
		if len(writes) >= batchSize {
			bulkExec(coll, writes)
			writes = writes[:0]
		}
		if count%10000 == 0 {
			log.Printf("[mongodb] Loaded group_hierarchy progress: %d rows elapsed=%s", count, time.Since(start).Truncate(time.Millisecond))
		}
	}
	if len(writes) > 0 {
		bulkExec(coll, writes)
	}
	log.Printf("[mongodb] Loaded group_hierarchy: %d rows", count)
}

// resources.csv -> create resource doc with org_id
func upsertResources(db *mongo.Database, start time.Time) {
	r, f := openCSV("resources.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[mongodb] read resources header: %v", err)
	}
	coll := db.Collection("resources")
	writes := make([]mongo.WriteModel, 0, batchSize)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[mongodb] read resources row: %v", err)
		}
		if len(rec) < 2 {
			log.Fatalf("[mongodb] invalid resources row: %#v", rec)
		}
		resID := rec[0]
		orgID := rec[1]
		writes = append(writes, &mongo.UpdateOneModel{
			Filter: bson.D{{Key: "resource_id", Value: resID}},
			Update: bson.D{{Key: "$set", Value: bson.D{{Key: "resource_id", Value: resID}, {Key: "org_id", Value: orgID}}}},
			Upsert: boolPtr(true),
		})
		count++
		if len(writes) >= batchSize {
			bulkExec(coll, writes)
			writes = writes[:0]
		}
		if count%10000 == 0 {
			log.Printf("[mongodb] Loaded resources progress: %d rows elapsed=%s", count, time.Since(start).Truncate(time.Millisecond))
		}
	}
	if len(writes) > 0 {
		bulkExec(coll, writes)
	}
	log.Printf("[mongodb] Loaded resources: %d rows", count)
}

// resource_acl.csv -> add IDs to manager/viewer arrays (user/group specific)
func upsertResourceACL(db *mongo.Database, start time.Time) {
	r, f := openCSV("resource_acl.csv")
	defer f.Close()
	if _, err := r.Read(); err != nil {
		log.Fatalf("[mongodb] read resource_acl header: %v", err)
	}
	coll := db.Collection("resources")
	writes := make([]mongo.WriteModel, 0, batchSize)
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("[mongodb] read resource_acl row: %v", err)
		}
		if len(rec) < 4 {
			log.Fatalf("[mongodb] invalid resource_acl row: %#v", rec)
		}
		resID := rec[0]
		subjectType := rec[1]
		subjectID := rec[2]
		relation := rec[3]

		var field string
		switch subjectType {
		case "user":
			switch relation {
			case "manager_user", "manager":
				field = "manager_user_ids"
			case "viewer_user", "viewer":
				field = "viewer_user_ids"
			default:
				log.Fatalf("[mongodb] unknown ACL relation for user: %q", relation)
			}
		case "group":
			switch relation {
			case "manager_group", "manager":
				field = "manager_group_ids"
			case "viewer_group", "viewer":
				field = "viewer_group_ids"
			default:
				log.Fatalf("[mongodb] unknown ACL relation for group: %q", relation)
			}
		default:
			log.Fatalf("[mongodb] unknown subject_type in resource_acl: %q", subjectType)
		}

		writes = append(writes, &mongo.UpdateOneModel{
			Filter: bson.D{{Key: "resource_id", Value: resID}},
			Update: bson.D{
				{Key: "$setOnInsert", Value: bson.D{{Key: "resource_id", Value: resID}}},
				{Key: "$addToSet", Value: bson.D{{Key: field, Value: subjectID}}},
			},
			Upsert: boolPtr(true),
		})
		count++
		if len(writes) >= batchSize {
			bulkExec(coll, writes)
			writes = writes[:0]
		}
		if count%10000 == 0 {
			log.Printf("[mongodb] Loaded resource_acl progress: %d rows elapsed=%s", count, time.Since(start).Truncate(time.Millisecond))
		}
	}
	if len(writes) > 0 {
		bulkExec(coll, writes)
	}
	log.Printf("[mongodb] Loaded resource_acl: %d rows", count)
}

func bulkExec(coll *mongo.Collection, writes []mongo.WriteModel) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	opts := options.BulkWrite().SetOrdered(false)
	if _, err := coll.BulkWrite(ctx, writes, opts); err != nil {
		log.Printf("[mongodb] BulkWrite error (may be duplicates): %v", err)
	}
}

func boolPtr(b bool) *bool { return &b }
