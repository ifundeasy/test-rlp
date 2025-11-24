package infrastructure

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"test-tls/utils"

	elasticsearch "github.com/elastic/go-elasticsearch/v9"
)

// ElasticsearchConfig holds connection configuration for Elasticsearch cluster.
type ElasticsearchConfig struct {
	Addresses          []string
	Username           string
	Password           string
	APIKey             string
	CloudID            string
	Timeout            time.Duration
	InsecureSkipVerify bool
}

// NewElasticsearchFromEnv creates an Elasticsearch client using environment
// variables and verifies the connection with a lightweight Info call.
//
// Env vars:
//
//	ELASTICSEARCH_URLS               (comma-separated, default: "http://localhost:9200")
//	ELASTICSEARCH_URL                (fallback if ELASTICSEARCH_URLS not set)
//	ELASTICSEARCH_USERNAME           (optional; default: "elastic")
//	ELASTICSEARCH_PASSWORD           (optional; default: "elasticsearchpwd123")
//	ELASTICSEARCH_API_KEY            (optional; "id:api_key" or just "api_key")
//	ELASTICSEARCH_CLOUD_ID           (optional; for Elastic Cloud)
//	ELASTICSEARCH_TIMEOUT_SEC        (per-request timeout hint; default: 5)
//	ELASTICSEARCH_INSECURE_SKIP_TLS  (true/false; default: false)
func NewElasticsearchFromEnv(parentCtx context.Context) (*elasticsearch.Client, func(), error) {
	cfg := loadElasticsearchConfigFromEnv()

	if len(cfg.Addresses) == 0 && cfg.CloudID == "" {
		return nil, func() {}, fmt.Errorf("elasticsearch: no addresses or cloud ID configured")
	}

	transport := buildElasticsearchTransport(cfg)

	esCfg := elasticsearch.Config{
		Addresses: cfg.Addresses,
		CloudID:   cfg.CloudID,
		Username:  cfg.Username,
		Password:  cfg.Password,
		APIKey:    cfg.APIKey,
		Transport: transport,
	}

	client, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, func() {}, fmt.Errorf("elasticsearch: create client: %w", err)
	}

	log.Printf(
		"[elasticsearch] Using addresses=%v cloud_id_set=%v auth_user=%v timeout=%s insecure_tls=%v",
		cfg.Addresses,
		cfg.CloudID != "",
		cfg.Username != "",
		cfg.Timeout,
		cfg.InsecureSkipVerify,
	)

	// "Ping" the cluster via Info to ensure connectivity.
	ctx, cancel := context.WithTimeout(parentCtx, cfg.Timeout)
	defer cancel()

	res, err := client.Info(
		client.Info.WithContext(ctx),
	)
	if err != nil {
		return nil, func() {}, fmt.Errorf("elasticsearch: info call failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, func() {}, fmt.Errorf("elasticsearch: info call returned error status: %s", res.Status())
	}

	cleanup := func() {
		// go-elasticsearch client does not expose Close;
		// HTTP connections will be cleaned up by the runtime.
	}

	return client, cleanup, nil
}

// loadElasticsearchConfigFromEnv reads configuration from environment variables
// and returns an ElasticsearchConfig with sensible defaults for local/docker use.
func loadElasticsearchConfigFromEnv() ElasticsearchConfig {
	// Prefer ELASTICSEARCH_URLS, fall back to ELASTICSEARCH_URL, then local.
	urlsCSV := utils.GetEnvWithDefault(
		"ELASTICSEARCH_URLS",
		utils.GetEnvWithDefault("ELASTICSEARCH_URL", "http://localhost:9200"),
	)

	raw := strings.Split(urlsCSV, ",")
	addresses := make([]string, 0, len(raw))
	for _, a := range raw {
		a = strings.TrimSpace(a)
		if a != "" {
			addresses = append(addresses, a)
		}
	}
	if len(addresses) == 0 {
		addresses = []string{"http://localhost:9200"}
	}

	// Defaults aligned with docker-compose:
	//   image: elasticsearch:9.2.1
	//   ELASTIC_PASSWORD=elasticsearchpwd123
	//   xpack.security.enabled=true
	username := utils.GetEnvWithDefault("ELASTICSEARCH_USERNAME", "elastic")
	password := utils.GetEnvWithDefault("ELASTICSEARCH_PASSWORD", "elasticsearchpwd123")

	apiKey := utils.GetEnvWithDefault("ELASTICSEARCH_API_KEY", "")
	cloudID := utils.GetEnvWithDefault("ELASTICSEARCH_CLOUD_ID", "")

	timeoutSec := utils.MustEnvIntWithDefault("ELASTICSEARCH_TIMEOUT_SEC", 5)
	timeout := time.Duration(timeoutSec) * time.Second

	insecureStr := utils.GetEnvWithDefault("ELASTICSEARCH_INSECURE_SKIP_TLS", "false")
	insecureSkip, err := strconv.ParseBool(strings.TrimSpace(insecureStr))
	if err != nil {
		log.Printf("[elasticsearch] invalid ELASTICSEARCH_INSECURE_SKIP_TLS=%q, defaulting to false", insecureStr)
		insecureSkip = false
	}

	return ElasticsearchConfig{
		Addresses:          addresses,
		Username:           username,
		Password:           password,
		APIKey:             apiKey,
		CloudID:            cloudID,
		Timeout:            timeout,
		InsecureSkipVerify: insecureSkip,
	}
}

// buildElasticsearchTransport builds an HTTP transport with sane defaults for
// Elasticsearch, including optional TLS verification skip for local/dev clusters.
func buildElasticsearchTransport(cfg ElasticsearchConfig) http.RoundTripper {
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 10,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}

	if cfg.InsecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true, // intended for local/dev only
		}
	}

	return transport
}
