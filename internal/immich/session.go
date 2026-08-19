package immich

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// SessionTokenSource supplies the access token required by Immich sync
// endpoints. Renew is called after Immich rejects a previously supplied token.
type SessionTokenSource interface {
	Token(ctx context.Context) (string, error)
	Renew(ctx context.Context, rejected string) (string, error)
}

// SessionClient signs in with an Immich user's credentials and keeps the
// current session access token in memory. Immich does not accept API keys for
// its sync endpoints, so the sidecar uses this client for sync and album calls.
//
// A rejected token is replaced by signing in again. The rejected token is not
// persisted, and concurrent callers share the single renewed session.
type SessionClient struct {
	baseURL  string
	email    string
	password string
	client   *http.Client

	mu    sync.Mutex
	token string
}

// NewSessionClient creates a session-token source for an Immich user.
func NewSessionClient(baseURL, email, password string) *SessionClient {
	return &SessionClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		email:    email,
		password: password,
		client:   &http.Client{Timeout: 30 * time.Second},
	}
}

// Token returns the current access token, signing in when the sidecar starts.
func (c *SessionClient) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" {
		return c.token, nil
	}
	return c.login(ctx)
}

// Renew replaces rejected only if it is still the current token. If another
// request has already renewed the session, it returns that fresh token instead.
func (c *SessionClient) Renew(ctx context.Context, rejected string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && c.token != rejected {
		return c.token, nil
	}
	return c.login(ctx)
}

type loginCredentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func (c *SessionClient) login(ctx context.Context) (string, error) {
	body, err := json.Marshal(loginCredentials{Email: c.email, Password: c.password})
	if err != nil {
		return "", fmt.Errorf("marshal Immich login: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/auth/login", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create Immich login request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST /api/auth/login: %w", err)
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", fmt.Errorf("read Immich login response: %w", err)
	}
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("Immich login returned %d: %s", resp.StatusCode, response)
	}
	var result struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(response, &result); err != nil {
		return "", fmt.Errorf("decode Immich login response: %w", err)
	}
	if result.AccessToken == "" {
		return "", fmt.Errorf("Immich login returned an empty access token")
	}
	c.token = result.AccessToken
	return c.token, nil
}

type staticSessionToken string

func (t staticSessionToken) Token(context.Context) (string, error) { return string(t), nil }
func (t staticSessionToken) Renew(context.Context, string) (string, error) {
	return string(t), nil
}

func staticSessionTokenSource(token string) SessionTokenSource { return staticSessionToken(token) }
