package infrastructure

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"test-tls/utils"
	"time"

	authzed "github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// AuthzedConfig holds connection/config options for the SpiceDB client.
type AuthzedPgdbConfig struct {
	Endpoint   string        // e.g. "localhost:50051"
	Token      string        // preshared key / bearer token
	CACertPath string        // path to CA/server cert (PEM)
	Timeout    time.Duration // per-request timeout
}

// NewAuthzedPgdbClient creates a SpiceDB/Authzed client with TLS + bearer token auth.
// It returns (client, ctxWithTimeout, cancel, error).
func NewAuthzedPgdbClient(ctx context.Context, cfg AuthzedPgdbConfig) (*authzed.Client, context.Context, context.CancelFunc, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "localhost:50052"
	}
	if cfg.CACertPath == "" {
		cfg.CACertPath = "docker/spicedb/cert.pem"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}

	// Load CA cert
	caPEM, err := os.ReadFile(cfg.CACertPath)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read CA cert %q: %w", cfg.CACertPath, err)
	}

	rootCAs := x509.NewCertPool()
	if ok := rootCAs.AppendCertsFromPEM(caPEM); !ok {
		return nil, nil, nil, fmt.Errorf("failed to append CA certs")
	}

	tlsConfig := &tls.Config{
		RootCAs: rootCAs,
		// Set this if your cert CN/SAN is not "localhost":
		// ServerName: "spicedb.local",
	}

	client, err := authzed.NewClient(
		cfg.Endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpcutil.WithBearerToken(cfg.Token),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create authzed client: %w", err)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, cfg.Timeout)
	return client, ctxWithTimeout, cancel, nil
}

// NewAuthzedClientFromEnv builds config from env vars with sane defaults.
func NewAuthzedPgdbClientFromEnv(ctx context.Context) (*authzed.Client, context.Context, context.CancelFunc, error) {
	cfg := AuthzedPgdbConfig{
		Endpoint:   utils.Getenv("SPICEDB_ENDPOINT", "localhost:50052"),
		Token:      utils.Getenv("SPICEDB_TOKEN", "spicdbgrpcpwd123"),
		CACertPath: utils.Getenv("SPICEDB_CA_CERT", "docker/spicedb/cert.pem"),
		Timeout:    10 * time.Second,
	}
	return NewAuthzedPgdbClient(ctx, cfg)
}
