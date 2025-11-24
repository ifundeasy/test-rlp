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

// CockroachConfig holds the connection + pool configuration.
// CockroachDB speaks the PostgreSQL wire protocol, so we use lib/pq.
type CockroachConfig struct {
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

// NewCockroachFromEnv creates a *sql.DB using environment variables and
// verifies the connection with a Ping before returning.
//
// Env vars:
//
//	CRDB_HOST                  (default: "localhost")
//	CRDB_PORT                  (default: "26257")
//	CRDB_USER                  (default: "root")
//	CRDB_PASSWORD              (default: "")
//	CRDB_DATABASE              (default: "rlp")
//	CRDB_SSLMODE               (default: "disable")      // for --insecure, keep "disable"
//	CRDB_MAX_OPEN_CONNS        (default: 0 -> driver default)
//	CRDB_MAX_IDLE_CONNS        (default: 0 -> driver default)
//	CRDB_CONN_MAX_LIFETIME_SEC (default: 0 -> no limit)
//	CRDB_CONNECT_TIMEOUT_SEC   (default: 5)
//
// Usage:
//
//	db, cleanup, err := infrastructure.NewCockroachFromEnv(context.Background())
//	if err != nil { log.Fatal(err) }
//	defer cleanup()
func NewCockroachFromEnv(parentCtx context.Context) (*sql.DB, func(), error) {
	cfg, err := loadCockroachConfigFromEnv()
	if err != nil {
		return nil, func() {}, err
	}

	dsn, err := buildCockroachDSN(cfg)
	if err != nil {
		return nil, func() {}, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, func() {}, fmt.Errorf("sql.Open cockroach(postgres): %w", err)
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
		return nil, func() {}, fmt.Errorf("cockroach ping failed: %w", err)
	}

	log.Printf("[cockroachdb] Connected host=%s port=%d db=%q user=%q sslmode=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.SSLMode)

	cleanup := func() {
		if err := db.Close(); err != nil {
			log.Printf("[cockroachdb] close error: %v", err)
		}
	}

	return db, cleanup, nil
}

func loadCockroachConfigFromEnv() (CockroachConfig, error) {
	host := utils.GetEnvWithDefault("CRDB_HOST", "localhost")
	port := utils.MustEnvIntWithDefault("CRDB_PORT", 26257)

	// Typical Cockroach single-node defaults:
	// user=root, password="", db=rlp, sslmode=disable (for --insecure)
	user := utils.GetEnvWithDefault("CRDB_USER", "root")
	password := utils.GetEnvWithDefault("CRDB_PASSWORD", "cockroachdbpwd123")
	dbname := utils.GetEnvWithDefault("CRDB_DATABASE", "rlp")
	sslmode := utils.GetEnvWithDefault("CRDB_SSLMODE", "disable")

	maxOpen := utils.MustEnvIntWithDefault("CRDB_MAX_OPEN_CONNS", 0)
	maxIdle := utils.MustEnvIntWithDefault("CRDB_MAX_IDLE_CONNS", 0)
	connMaxLifetimeSec := utils.MustEnvIntWithDefault("CRDB_CONN_MAX_LIFETIME_SEC", 0)
	connectTimeoutSec := utils.MustEnvIntWithDefault("CRDB_CONNECT_TIMEOUT_SEC", 5)

	return CockroachConfig{
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

func buildCockroachDSN(cfg CockroachConfig) (string, error) {
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
	// You can optionally set connect_timeout here, but since we use PingContext with timeout,
	// it's not strictly necessary. Uncomment if you want server-side enforcement as well:
	// q.Set("connect_timeout", strconv.Itoa(int(cfg.ConnectTimeout.Seconds())))

	u.RawQuery = q.Encode()
	return u.String(), nil
}
