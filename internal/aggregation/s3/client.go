package s3

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"

	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	client *minio.Client
}

func New(endpoint, accessKey, secretKey string, useSSL bool) (*Client, error) {
	normalized, err := normalizeEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	client, err := minio.New(normalized, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}

	return &Client{client: client}, nil
}

func (c *Client) PutObject(ctx context.Context, bucket, key, contentType string, payload []byte) error {
	_, err := c.client.PutObject(ctx, bucket, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{
		ContentType: contentType,
	})
	return err
}

func normalizeEndpoint(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("s3 endpoint must not be empty")
	}
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return "", fmt.Errorf("invalid s3 endpoint %q: %w", endpoint, err)
		}
		if parsed.Host != "" {
			return parsed.Host, nil
		}
		if parsed.Path != "" {
			return parsed.Path, nil
		}
		return "", fmt.Errorf("invalid s3 endpoint %q", endpoint)
	}
	return endpoint, nil
}
