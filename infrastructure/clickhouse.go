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

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// ClickhouseConfig holds the connection + pool configuration.
type ClickhouseConfig struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnectTimeout  time.Duration
}

// NewClickhouseFromEnv creates a *sql.DB using environment variables and
// verifies the connection with a Ping before returning.
//
// Env vars:
//
//	CH_HOST                  (default: "localhost")
//	CH_PORT                  (default: 9000)
//	CH_USER                  (default: "default")
//	CH_PASSWORD              (default: "")
//	CH_DATABASE              (default: "default")
//	CH_MAX_OPEN_CONNS        (default: 0 -> driver default)
//	CH_MAX_IDLE_CONNS        (default: 0 -> driver default)
//	CH_CONN_MAX_LIFETIME_SEC (default: 0 -> no limit)
//	CH_CONNECT_TIMEOUT_SEC   (default: 5)
//
// Usage:
//
//	db, cleanup, err := infrastructure.NewClickhouseFromEnv(context.Background())
//	if err != nil { log.Fatal(err) }
//	defer cleanup()
func NewClickhouseFromEnv(parentCtx context.Context) (*sql.DB, func(), error) {
	cfg, err := loadClickhouseConfigFromEnv()
	if err != nil {
		return nil, func() {}, err
	}

	dsn, err := buildClickhouseDSN(cfg)
	if err != nil {
		return nil, func() {}, err
	}

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, func() {}, fmt.Errorf("sql.Open clickhouse: %w", err)
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
		return nil, func() {}, fmt.Errorf("clickhouse ping failed: %w", err)
	}

	cleanup := func() {
		if err := db.Close(); err != nil {
			log.Printf("clickhouse: close error: %v", err)
		}
	}

	return db, cleanup, nil
}

func loadClickhouseConfigFromEnv() (ClickhouseConfig, error) {
	host := utils.GetEnvWithDefault("CH_HOST", "localhost")
	port := utils.MustEnvIntWithDefault("CH_PORT", 9000)

	// Typical ClickHouse defaults:
	//   user:     default
	//   password: ""
	//   database: default
	user := utils.GetEnvWithDefault("CH_USER", "root")
	password := utils.GetEnvWithDefault("CH_PASSWORD", "clickhousepwd123")
	dbname := utils.GetEnvWithDefault("CH_DATABASE", "rlp")

	maxOpen := utils.MustEnvIntWithDefault("CH_MAX_OPEN_CONNS", 0)
	maxIdle := utils.MustEnvIntWithDefault("CH_MAX_IDLE_CONNS", 0)
	connMaxLifetimeSec := utils.MustEnvIntWithDefault("CH_CONN_MAX_LIFETIME_SEC", 0)
	connectTimeoutSec := utils.MustEnvIntWithDefault("CH_CONNECT_TIMEOUT_SEC", 5)

	return ClickhouseConfig{
		Host:            host,
		Port:            port,
		User:            user,
		Password:        password,
		Database:        dbname,
		MaxOpenConns:    maxOpen,
		MaxIdleConns:    maxIdle,
		ConnMaxLifetime: time.Duration(connMaxLifetimeSec) * time.Second,
		ConnectTimeout:  time.Duration(connectTimeoutSec) * time.Second,
	}, nil
}

func buildClickhouseDSN(cfg ClickhouseConfig) (string, error) {
	// DSN format (clickhouse-go v2, database/sql):
	//   clickhouse://username:password@host:9000/database?dial_timeout=5s
	u := &url.URL{
		Scheme: "clickhouse",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		Path:   "/" + cfg.Database,
	}

	q := url.Values{}
	if cfg.ConnectTimeout > 0 {
		q.Set("dial_timeout", cfg.ConnectTimeout.String())
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}
