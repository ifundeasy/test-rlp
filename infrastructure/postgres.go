package infrastructure

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"test-tls/utils"
	"time"

	_ "github.com/lib/pq"
)

// PostgresConfig holds the connection + pool configuration.
type PostgresConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	SSLMode         string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnectTimeout  time.Duration
}

// NewPostgresFromEnv creates a *sql.DB using environment variables and
// verifies the connection with a Ping before returning.
//
// Env vars:
//
//	PG_HOST                  (default: "localhost")
//	PG_PORT                  (default: "5432")
//	PG_USER                  (default: "postgres")
//	PG_PASSWORD              (default: "postgrespwd123")
//	PG_DATABASE              (default: "rlp")
//	PG_SSLMODE               (default: "disable")
//	PG_MAX_OPEN_CONNS        (default: 0 -> driver default)
//	PG_MAX_IDLE_CONNS        (default: 0 -> driver default)
//	PG_CONN_MAX_LIFETIME_SEC (default: 0 -> no limit)
//	PG_CONNECT_TIMEOUT_SEC   (default: 5)
//
// Usage:
//
//	db, cleanup, err := infrastructure.NewPostgresFromEnv(context.Background())
//	if err != nil { log.Fatal(err) }
//	defer cleanup()
func NewPostgresFromEnv(parentCtx context.Context) (*sql.DB, func(), error) {
	cfg, err := loadPostgresConfigFromEnv()
	if err != nil {
		return nil, func() {}, err
	}

	dsn, err := buildPostgresDSN(cfg)
	if err != nil {
		return nil, func() {}, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, func() {}, fmt.Errorf("sql.Open postgres: %w", err)
	}

	// Pool settings
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	// Ping with timeout so we fail fast on bad config.
	ctx, cancel := context.WithTimeout(parentCtx, cfg.ConnectTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, func() {}, fmt.Errorf("postgres ping failed: %w", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			log.Printf("postgres: close error: %v", err)
		}
	}

	return db, cleanup, nil
}

func loadPostgresConfigFromEnv() (PostgresConfig, error) {
	host := utils.GetEnvWithDefault("PG_HOST", "localhost")
	port := utils.MustEnvIntWithDefault("PG_PORT", 5432)

	// defaults disesuaikan dengan docker compose:
	// POSTGRES_USER=postgres
	// POSTGRES_PASSWORD=postgrespwd123
	// POSTGRES_DB=postgresdb
	user := utils.GetEnvWithDefault("PG_USER", "root")
	password := utils.GetEnvWithDefault("PG_PASSWORD", "postgrespwd123")
	dbname := utils.GetEnvWithDefault("PG_DATABASE", "rlp")
	sslmode := utils.GetEnvWithDefault("PG_SSLMODE", "disable")

	maxOpen := utils.MustEnvIntWithDefault("PG_MAX_OPEN_CONNS", 0)
	maxIdle := utils.MustEnvIntWithDefault("PG_MAX_IDLE_CONNS", 0)
	connMaxLifetimeSec := utils.MustEnvIntWithDefault("PG_CONN_MAX_LIFETIME_SEC", 0)
	connectTimeoutSec := utils.MustEnvIntWithDefault("PG_CONNECT_TIMEOUT_SEC", 5)

	return PostgresConfig{
		Host:            host,
		Port:            port,
		User:            user,
		Password:        password,
		Database:        dbname,
		SSLMode:         sslmode,
		MaxOpenConns:    maxOpen,
		MaxIdleConns:    maxIdle,
		ConnMaxLifetime: time.Duration(connMaxLifetimeSec) * time.Second,
		ConnectTimeout:  time.Duration(connectTimeoutSec) * time.Second,
	}, nil
}

func buildPostgresDSN(cfg PostgresConfig) (string, error) {
	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   "/" + cfg.Database,
	}

	q := url.Values{}
	if cfg.SSLMode != "" {
		q.Set("sslmode", cfg.SSLMode)
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}
