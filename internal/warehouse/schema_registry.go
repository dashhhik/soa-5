package warehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type SchemaRegistryClient struct {
	baseURL string
	client  *http.Client
}

func NewSchemaRegistryClient(baseURL string) *SchemaRegistryClient {
	return &SchemaRegistryClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *SchemaRegistryClient) SetCompatibility(ctx context.Context, subject, compatibility string) error {
	payload := map[string]string{"compatibility": compatibility}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("%s/config/%s", c.baseURL, url.PathEscape(subject))
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return readRegistryError(resp)
	}
	return nil
}

func (c *SchemaRegistryClient) Register(ctx context.Context, subject, schema string) (int, error) {
	payload := map[string]string{
		"schema":     schema,
		"schemaType": "AVRO",
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	endpoint := fmt.Sprintf("%s/subjects/%s/versions", c.baseURL, url.PathEscape(subject))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return 0, readRegistryError(resp)
	}

	var decoded struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return 0, err
	}
	if decoded.ID == 0 {
		return 0, fmt.Errorf("schema registry returned empty schema id for subject %s", subject)
	}
	return decoded.ID, nil
}

func (c *SchemaRegistryClient) SchemaByID(ctx context.Context, id int) (string, error) {
	endpoint := fmt.Sprintf("%s/schemas/ids/%d", c.baseURL, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")

	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", readRegistryError(resp)
	}
	var decoded struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return "", err
	}
	if decoded.Schema == "" {
		return "", fmt.Errorf("schema registry returned empty schema for id %d", id)
	}
	return decoded.Schema, nil
}

func readRegistryError(resp *http.Response) error {
	var decoded struct {
		Message string `json:"message"`
		Error   int    `json:"error_code"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err == nil && decoded.Message != "" {
		return fmt.Errorf("schema registry error: status=%s message=%s", resp.Status, decoded.Message)
	}
	return fmt.Errorf("schema registry error: status=%s", resp.Status)
}
