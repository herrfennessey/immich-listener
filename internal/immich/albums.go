package immich

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// AlbumClient reads album membership from the Immich API. It implements
// core.AlbumResolver.
type AlbumClient struct {
	baseURL    string
	session    SessionTokenSource
	httpClient *http.Client
}

// NewAlbumClient creates an AlbumClient.
func NewAlbumClient(baseURL string, session SessionTokenSource) *AlbumClient {
	return &AlbumClient{
		baseURL:    baseURL,
		session:    session,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Albums returns the IDs of the albums that contain the asset. It calls
// GET /api/albums?assetId=<id>. An asset in no album returns an empty slice.
func (c *AlbumClient) Albums(ctx context.Context, assetID string) ([]string, error) {
	q := url.Values{"assetId": {assetID}}
	reqURL := c.baseURL + "/api/albums?" + q.Encode()

	resp, err := c.do(ctx, func(token string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("x-immich-session-token", token)
		return req, nil
	})
	if err != nil {
		return nil, fmt.Errorf("GET /api/albums: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("albums returned %d: %s", resp.StatusCode, body)
	}

	var albums []struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&albums); err != nil {
		return nil, fmt.Errorf("decode albums: %w", err)
	}

	ids := make([]string, 0, len(albums))
	for _, a := range albums {
		ids = append(ids, a.ID)
	}
	return ids, nil
}

func (c *AlbumClient) do(ctx context.Context, newRequest func(token string) (*http.Request, error)) (*http.Response, error) {
	token, err := c.session.Token(ctx)
	if err != nil {
		return nil, err
	}
	req, err := newRequest(token)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}
	resp.Body.Close()

	token, err = c.session.Renew(ctx, token)
	if err != nil {
		return nil, err
	}
	req, err = newRequest(token)
	if err != nil {
		return nil, err
	}
	return c.httpClient.Do(req)
}
