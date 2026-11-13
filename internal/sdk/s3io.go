package sdk

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// maxDataset is the largest object a Distributed Map reads or writes in
// memory. AWS reads up to 10 GB. A local server reads up to this.
const maxDataset = 256 << 20

// S3Object is one ListObjectsV2 entry, the item a Distributed Map gets
// without LOAD_AND_FLATTEN.
type S3Object struct {
	Etag         string
	Key          string
	LastModified int64
	Size         int64
	StorageClass string
}

// s3 returns the S3 client for the Distributed Map reader and writer. It is
// built once, from the same endpoint as aws-sdk:s3 tasks.
func (c *Caller) s3() (*s3.Client, error) {
	if c == nil || c.cfg.Endpoints["s3"] == "" {
		return nil, errors.New("S3 has no endpoint. Set AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL.")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.s3c == nil {
		c.s3c = newS3Client(c.awsConfig(), c.cfg.Endpoints["s3"])
	}
	return c.s3c, nil
}

// S3Read gets one object for a Distributed Map reader. params is a
// GetObject input, such as {"Bucket":"b","Key":"k"}. A gzip body is
// decompressed. A service error is a *ServiceError named S3.<Code>Exception.
func (c *Caller) S3Read(ctx context.Context, params []byte) ([]byte, error) {
	in := new(s3.GetObjectInput)
	if err := decode(params, in); err != nil {
		return nil, err
	}
	return c.read(ctx, in)
}

func (c *Caller) read(ctx context.Context, in *s3.GetObjectInput) ([]byte, error) {
	client, err := c.s3()
	if err != nil {
		return nil, err
	}
	out, err := client.GetObject(ctx, in)
	if err != nil {
		return nil, mapError("S3", FallbackName("s3"), err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(io.LimitReader(out.Body, maxDataset+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxDataset {
		return nil, fmt.Errorf("s3://%s/%s is larger than %d bytes", aws.ToString(in.Bucket), aws.ToString(in.Key), maxDataset)
	}
	return decompress(body, aws.ToString(in.Key))
}

// S3ReadKey gets bucket/key.
func (c *Caller) S3ReadKey(ctx context.Context, bucket, key string) ([]byte, error) {
	return c.read(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
}

// decompress handles gzip by its magic bytes. zstd is refused.
func decompress(body []byte, key string) ([]byte, error) {
	switch {
	case len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b:
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		out, err := io.ReadAll(io.LimitReader(zr, maxDataset+1))
		if err != nil {
			return nil, err
		}
		if len(out) > maxDataset {
			return nil, fmt.Errorf("%s is larger than %d bytes after decompression", key, maxDataset)
		}
		return out, nil
	case len(body) >= 4 && body[0] == 0x28 && body[1] == 0xb5 && body[2] == 0x2f && body[3] == 0xfd:
		return nil, fmt.Errorf("%s is zstd compressed, which this server does not read", key)
	}
	return body, nil
}

// S3List lists every object for a Distributed Map reader, following
// continuation tokens. params is a ListObjectsV2 input.
func (c *Caller) S3List(ctx context.Context, params []byte, limit int64) ([]S3Object, error) {
	in := new(s3.ListObjectsV2Input)
	if err := decode(params, in); err != nil {
		return nil, err
	}
	client, err := c.s3()
	if err != nil {
		return nil, err
	}
	var out []S3Object
	for {
		page, err := client.ListObjectsV2(ctx, in)
		if err != nil {
			return nil, mapError("S3", FallbackName("s3"), err)
		}
		for _, o := range page.Contents {
			obj := S3Object{
				Etag:         aws.ToString(o.ETag),
				Key:          aws.ToString(o.Key),
				Size:         aws.ToInt64(o.Size),
				StorageClass: string(o.StorageClass),
			}
			if o.LastModified != nil {
				obj.LastModified = o.LastModified.Unix()
			}
			out = append(out, obj)
			if limit > 0 && int64(len(out)) >= limit {
				return out, nil
			}
		}
		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			return out, nil
		}
		in.ContinuationToken = page.NextContinuationToken
	}
}

// S3Write puts one object for a Distributed Map result writer.
func (c *Caller) S3Write(ctx context.Context, bucket, key string, body []byte) error {
	client, err := c.s3()
	if err != nil {
		return err
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return mapError("S3", FallbackName("s3"), err)
	}
	return nil
}
