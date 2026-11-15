package queue

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// ErrUnauthorized means the API key is unknown or revoked.
var ErrUnauthorized = errors.New("unauthorized")

// hashKey is a plain SHA-256: API keys are 256-bit random values, so a slow password hash
// (bcrypt/argon2) would add latency to every request without adding security.
func hashKey(key string) []byte {
	h := sha256.Sum256([]byte(key))
	return h[:]
}

// CreateAPIKey mints a new key for the tenant. The plaintext is returned exactly once and
// only its hash is stored.
func (s *Store) CreateAPIKey(ctx context.Context, tenantID, name string) (string, error) {
	var raw [32]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate key: %w", err)
	}
	key := "js_" + base64.RawURLEncoding.EncodeToString(raw[:])
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO api_keys (key_hash, tenant_id, name) VALUES ($1, $2, $3)`,
		hashKey(key), tenantID, name); err != nil {
		return "", fmt.Errorf("store key: %w", err)
	}
	return key, nil
}

// TenantForKey resolves an API key to its tenant. The tenant always comes from here,
// never from the request body.
func (s *Store) TenantForKey(ctx context.Context, key string) (string, error) {
	var tenant string
	err := s.pool.QueryRow(ctx,
		`SELECT tenant_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`,
		hashKey(key)).Scan(&tenant)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrUnauthorized
	}
	if err != nil {
		return "", fmt.Errorf("lookup key: %w", err)
	}
	return tenant, nil
}
