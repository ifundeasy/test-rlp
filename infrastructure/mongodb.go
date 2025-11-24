package infrastructure

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"test-tls/utils"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// MongoConfig holds the connection configuration for MongoDB.
type MongoConfig struct {
	URI            string
	Database       string
	ConnectTimeout time.Duration
}

// NewMongoFromEnv creates a *mongo.Client and *mongo.Database using environment
// variables and verifies the connection with a Ping before returning.
//
// Env vars:
//
//	MONGO_URI                 (optional; if set, overrides host/user/pass/port)
//	MONGO_HOST                (default: "localhost")
//	MONGO_PORT                (default: "27017")
//	MONGO_USER                (default: "root")
//	MONGO_PASSWORD            (default: "mongodbpwd123")
//	MONGO_DATABASE            (default: "rlp")
//	MONGO_AUTH_SOURCE         (default: "admin")
//	MONGO_CONNECT_TIMEOUT_SEC (default: 5)
//
// Usage:
//
//	client, db, cleanup, err := infrastructure.NewMongoFromEnv(context.Background())
//	if err != nil { log.Fatal(err) }
//	defer cleanup()
func NewMongoFromEnv(parentCtx context.Context) (*mongo.Client, *mongo.Database, func(), error) {
	cfg, err := loadMongoConfigFromEnv()
	if err != nil {
		return nil, nil, func() {}, err
	}

	clientOpts := options.Client().ApplyURI(cfg.URI)

	// Apply selection / connect timeouts if provided.
	if cfg.ConnectTimeout > 0 {
		clientOpts = clientOpts.
			SetConnectTimeout(cfg.ConnectTimeout).
			SetServerSelectionTimeout(cfg.ConnectTimeout)
	}

	ctx, cancel := context.WithTimeout(parentCtx, cfg.ConnectTimeout)
	defer cancel()

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, nil, func() {}, fmt.Errorf("mongodb: connect failed: %w", err)
	}

	// Verify the connection with a ping.
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, nil, func() {}, fmt.Errorf("mongodb: ping failed: %w", err)
	}

	db := client.Database(cfg.Database)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Disconnect(ctx); err != nil {
			log.Printf("mongodb: disconnect error: %v", err)
		}
	}

	return client, db, cleanup, nil
}

func loadMongoConfigFromEnv() (MongoConfig, error) {
	// If MONGO_URI is set, we trust it completely.
	if uri := utils.GetEnvWithDefault("MONGO_URI", ""); uri != "" {
		dbName := utils.GetEnvWithDefault("MONGO_DATABASE", "rlp")
		connectTimeoutSec := utils.MustEnvIntWithDefault("MONGO_CONNECT_TIMEOUT_SEC", 5)

		return MongoConfig{
			URI:            uri,
			Database:       dbName,
			ConnectTimeout: time.Duration(connectTimeoutSec) * time.Second,
		}, nil
	}

	// Otherwise, build URI from components (aligned with docker-compose).
	host := utils.GetEnvWithDefault("MONGO_HOST", "localhost")
	port := utils.MustEnvIntWithDefault("MONGO_PORT", 27017)
	user := utils.GetEnvWithDefault("MONGO_USER", "root")
	password := utils.GetEnvWithDefault("MONGO_PASSWORD", "mongodbpwd123")
	dbName := utils.GetEnvWithDefault("MONGO_DATABASE", "rlp")
	authSource := utils.GetEnvWithDefault("MONGO_AUTH_SOURCE", "admin")
	connectTimeoutSec := utils.MustEnvIntWithDefault("MONGO_CONNECT_TIMEOUT_SEC", 5)

	u := &url.URL{
		Scheme: "mongodb",
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
	}

	if dbName != "" {
		u.Path = "/" + dbName
	} else {
		u.Path = "/" // at least a slash
	}

	if user != "" {
		u.User = url.UserPassword(user, password)
	}

	q := url.Values{}
	if authSource != "" {
		q.Set("authSource", authSource)
	}
	u.RawQuery = q.Encode()

	return MongoConfig{
		URI:            u.String(),
		Database:       dbName,
		ConnectTimeout: time.Duration(connectTimeoutSec) * time.Second,
	}, nil
}
