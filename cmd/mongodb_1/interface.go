package mongodb_1

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoIndexSpec struct {
	Name   string
	Keys   bson.D
	Unique bool
}

func CreateIndexesWithLog(
	parent context.Context,
	coll *mongo.Collection,
	specs []MongoIndexSpec,
	timeout time.Duration,
	collName string,
) {
	if len(specs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	models := make([]mongo.IndexModel, 0, len(specs))
	for _, s := range specs {
		opts := options.Index().SetName(s.Name)
		if s.Unique {
			opts = opts.SetUnique(true)
		}
		models = append(models, mongo.IndexModel{
			Keys:    s.Keys,
			Options: opts,
		})
	}

	names, err := coll.Indexes().CreateMany(ctx, models)
	if err != nil {
		log.Fatalf("[mongodb_1] create indexes on %s failed: %v", collName, err)
	}
	for _, name := range names {
		log.Printf("[mongodb_1] Index created on %s: %s", collName, name)
	}
}
