//go:build integration

package immich

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"testing"
)

type e2eImmich struct {
	baseURL string
	token   string
}

func newE2EImmich(t *testing.T, suite *e2eSuite) *e2eImmich {
	t.Helper()
	response, err := postE2EJSON(context.Background(), suite.immichURL+"/api/auth/login", map[string]string{
		"email": e2eEmail, "password": e2ePassword,
	}, http.StatusCreated)
	if err != nil {
		t.Fatalf("login E2E user: %v", err)
	}
	var login struct {
		AccessToken string `json:"accessToken"`
	}
	if err := json.Unmarshal(response, &login); err != nil || login.AccessToken == "" {
		t.Fatalf("decode Immich login: token=%q err=%v", login.AccessToken, err)
	}
	return &e2eImmich{baseURL: suite.immichURL, token: login.AccessToken}
}

func (c *e2eImmich) UploadImage(t *testing.T, ctx context.Context, filename string, image []byte) string {
	t.Helper()
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	file, err := form.CreateFormFile("assetData", filename)
	if err != nil {
		t.Fatalf("create image upload field: %v", err)
	}
	if _, err := file.Write(image); err != nil {
		t.Fatalf("write image upload field: %v", err)
	}
	for key, value := range map[string]string{"fileCreatedAt": "2026-08-17T11:00:00.000Z", "fileModifiedAt": "2026-08-17T11:00:00.000Z"} {
		if err := form.WriteField(key, value); err != nil {
			t.Fatalf("write upload field %s: %v", key, err)
		}
	}
	if err := form.Close(); err != nil {
		t.Fatalf("close upload multipart body: %v", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/assets", &body)
	if err != nil {
		t.Fatalf("create asset upload request: %v", err)
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	req.Header.Set("x-immich-session-token", c.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload image to Immich: %v", err)
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload image: status=%d err=%v response=%s", resp.StatusCode, err, response)
	}
	var asset struct{ ID string `json:"id"` }
	if err := json.Unmarshal(response, &asset); err != nil || asset.ID == "" {
		t.Fatalf("decode asset upload: id=%q err=%v response=%s", asset.ID, err, response)
	}
	return asset.ID
}

func postE2EJSON(ctx context.Context, url string, value any, wantStatus int) ([]byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != wantStatus {
		return nil, fmt.Errorf("POST %s status=%d want=%d response=%s", url, resp.StatusCode, wantStatus, response)
	}
	return response, nil
}

func e2eImage(t *testing.T) []byte {
	t.Helper()
	image, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIHWP4z8DwHwAFgAI/ScL6sAAAAABJRU5ErkJggg==")
	if err != nil {
		t.Fatalf("decode image fixture: %v", err)
	}
	return image
}
