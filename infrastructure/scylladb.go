package infrastructure

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"test-tls/utils"

	"github.com/gocql/gocql"
)

// ScyllaConfig holds the connection configuration for ScyllaDB / Cassandra.
type ScyllaConfig struct {
	Hosts          []string
	Port           int
	Keyspace       string
	Username       string
	Password       string
	Consistency    gocql.Consistency
	ConnectTimeout time.Duration
	Timeout        time.Duration
}

// NewScyllaFromEnv creates a *gocql.Session using environment variables,
// ensures that the keyspace exists, and verifies the connection with a
// simple "ping" query before returning.
//
// Env vars:
//
//	SCYLLA_HOSTS                 (comma-separated, default: "localhost")
//	SCYLLA_HOST                  (fallback if SCYLLA_HOSTS not set)
//	SCYLLA_PORT                  (default: 9042)
//	SCYLLA_KEYSPACE              (default: "rlp")
//	SCYLLA_USER                  (optional; default: "")
//	SCYLLA_PASSWORD              (optional; default: "")
//	SCYLLA_CONSISTENCY           (ONE|LOCAL_ONE|QUORUM|LOCAL_QUORUM|ALL; default: LOCAL_QUORUM)
//	SCYLLA_TIMEOUT_SEC           (per-query timeout; default: 5)
//	SCYLLA_CONNECT_TIMEOUT_SEC   (connect timeout; default: SCYLLA_TIMEOUT_SEC)
func NewScyllaFromEnv(parentCtx context.Context) (*gocql.Session, func(), error) {
	cfg := loadScyllaConfigFromEnv()

	if len(cfg.Hosts) == 0 {
		return nil, func() {}, fmt.Errorf("scylladb: no hosts configured")
	}

	log.Printf("[scylladb] Using hosts=%v port=%d keyspace=%q consistency=%v",
		cfg.Hosts, cfg.Port, cfg.Keyspace, cfg.Consistency)

	// Phase 1: connect without keyspace to create it if needed.
	adminCluster := gocql.NewCluster(cfg.Hosts...)
	adminCluster.Port = cfg.Port
	adminCluster.Timeout = cfg.Timeout
	adminCluster.ConnectTimeout = cfg.ConnectTimeout
	adminCluster.Consistency = cfg.Consistency

	if cfg.Username != "" {
		adminCluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	adminSession, err := adminCluster.CreateSession()
	if err != nil {
		return nil, func() {}, fmt.Errorf("scylladb: create admin session: %w", err)
	}

	if err := ensureKeyspace(parentCtx, adminSession, cfg); err != nil {
		adminSession.Close()
		return nil, func() {}, err
	}
	adminSession.Close()

	// Phase 2: connect to the target keyspace.
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Port = cfg.Port
	cluster.Keyspace = cfg.Keyspace
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.Consistency = cfg.Consistency

	if cfg.Username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, func() {}, fmt.Errorf("scylladb: create session: %w", err)
	}

	// "Ping" the cluster with a lightweight query against system.local.
	ctx, cancel := context.WithTimeout(parentCtx, cfg.Timeout)
	defer cancel()

	if err := session.Query("SELECT now() FROM system.local").WithContext(ctx).Exec(); err != nil {
		session.Close()
		return nil, func() {}, fmt.Errorf("scylladb: ping failed: %w", err)
	}

	cleanup := func() {
		session.Close()
	}

	return session, cleanup, nil
}

// ensureKeyspace creates the configured keyspace if it does not exist.
// For local/benchmark usage, we use SimpleStrategy and replication_factor=1.
func ensureKeyspace(ctx context.Context, session *gocql.Session, cfg ScyllaConfig) error {
	if cfg.Keyspace == "" {
		return fmt.Errorf("scylladb: keyspace is empty")
	}

	cql := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		cfg.Keyspace,
	)

	ctxTimeout, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	if err := session.Query(cql).WithContext(ctxTimeout).Exec(); err != nil {
		return fmt.Errorf("scylladb: create keyspace %q failed: %w", cfg.Keyspace, err)
	}

	return nil
}

// loadScyllaConfigFromEnv reads configuration from environment variables and
// returns a ScyllaConfig with defaults suitable for local/docker development.
func loadScyllaConfigFromEnv() ScyllaConfig {
	// Prefer SCYLLA_HOSTS, fall back to SCYLLA_HOST, then localhost.
	hostsCSV := utils.GetEnvWithDefault(
		"SCYLLA_HOSTS",
		utils.GetEnvWithDefault("SCYLLA_HOST", "localhost"),
	)

	rawHosts := strings.Split(hostsCSV, ",")
	hosts := make([]string, 0, len(rawHosts))
	for _, h := range rawHosts {
		h = strings.TrimSpace(h)
		if h != "" {
			hosts = append(hosts, h)
		}
	}
	if len(hosts) == 0 {
		hosts = []string{"localhost"}
	}

	port := utils.MustEnvIntWithDefault("SCYLLA_PORT", 9042)
	keyspace := utils.GetEnvWithDefault("SCYLLA_KEYSPACE", "rlp")
	user := utils.GetEnvWithDefault("SCYLLA_USER", "")
	password := utils.GetEnvWithDefault("SCYLLA_PASSWORD", "")

	timeoutSec := utils.MustEnvIntWithDefault("SCYLLA_TIMEOUT_SEC", 5)
	connectTimeoutSec := utils.MustEnvIntWithDefault("SCYLLA_CONNECT_TIMEOUT_SEC", timeoutSec)

	consistencyStr := strings.ToUpper(utils.GetEnvWithDefault("SCYLLA_CONSISTENCY", "LOCAL_QUORUM"))
	consistency := parseScyllaConsistency(consistencyStr)

	return ScyllaConfig{
		Hosts:          hosts,
		Port:           port,
		Keyspace:       keyspace,
		Username:       user,
		Password:       password,
		Consistency:    consistency,
		ConnectTimeout: time.Duration(connectTimeoutSec) * time.Second,
		Timeout:        time.Duration(timeoutSec) * time.Second,
	}
}

// parseScyllaConsistency maps a string env value to a gocql.Consistency,
// falling back to gocql.LocalQuorum on unknown values.
func parseScyllaConsistency(s string) gocql.Consistency {
	switch s {
	case "ONE":
		return gocql.One
	case "LOCAL_ONE":
		return gocql.LocalOne
	case "QUORUM":
		return gocql.Quorum
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum
	case "ALL":
		return gocql.All
	default:
		log.Printf("scylladb: unknown SCYLLA_CONSISTENCY=%q, defaulting to LOCAL_QUORUM", s)
		return gocql.LocalQuorum
	}
}
