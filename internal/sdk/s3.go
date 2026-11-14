package sdk

import (
	"context"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// maxBody is the largest object body getObject returns. It is the state
// payload limit.
const maxBody = 262144

var s3Service = service{
	errorPrefix: "S3",
	env:         "S3",
	client:      (*s3.Client)(nil),
	actions: func(cfg aws.Config, endpoint string) map[string]handler {
		c := newS3Client(cfg, endpoint)
		return map[string]handler{
			"abortMultipartUpload":    op(c.AbortMultipartUpload),
			"completeMultipartUpload": op(c.CompleteMultipartUpload),
			"copyObject":              op(c.CopyObject),
			"createBucket":            op(c.CreateBucket),
			"createMultipartUpload":   op(c.CreateMultipartUpload),
			"deleteBucket":            op(c.DeleteBucket),
			"deleteObject":            op(c.DeleteObject),
			"deleteObjectTagging":     op(c.DeleteObjectTagging),
			"deleteObjects":           op(c.DeleteObjects),
			"getBucketLocation":       op(c.GetBucketLocation),
			"getBucketTagging":        op(c.GetBucketTagging),
			"getObject":               s3GetObject(c),
			"getObjectAttributes":     op(c.GetObjectAttributes),
			"getObjectTagging":        op(c.GetObjectTagging),
			"headBucket":              op(c.HeadBucket),
			"headObject":              s3HeadObject(c),
			"listBuckets":             op(c.ListBuckets),
			"listMultipartUploads":    op(c.ListMultipartUploads),
			"listObjectVersions":      op(c.ListObjectVersions),
			"listObjects":             op(c.ListObjects),
			"listObjectsV2":           op(c.ListObjectsV2),
			"listParts":               op(c.ListParts),
			"putBucketTagging":        op(c.PutBucketTagging),
			"putObject":               s3PutObject(c),
			"putObjectTagging":        op(c.PutObjectTagging),
		}
	},
}

func newS3Client(cfg aws.Config, endpoint string) *s3.Client {
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		// A local endpoint has no bucket subdomains.
		o.UsePathStyle = true
		// Local emulators do not all accept the default CRC32 trailer.
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
}

// s3GetObject returns Body as a string, the way Step Functions does.
func s3GetObject(c *s3.Client) handler {
	return func(ctx context.Context, params []byte) (string, error) {
		in := new(s3.GetObjectInput)
		if err := decode(params, in); err != nil {
			return "", err
		}
		out, err := c.GetObject(ctx, in)
		if err != nil {
			return "", err
		}
		defer out.Body.Close()
		body, err := io.ReadAll(io.LimitReader(out.Body, maxBody+1))
		if err != nil {
			return "", err
		}
		if len(body) > maxBody {
			return "", &ServiceError{Name: "States.DataLimitExceeded", Cause: "The S3 object body exceeded the maximum size of 262144 bytes."}
		}
		out.Body = nil
		extra := map[string]any{"Body": string(body)}
		if out.Metadata == nil {
			extra["Metadata"] = map[string]any{}
		}
		return encode(out, extra)
	}
}

// s3HeadObject always returns Metadata, as AWS does. The Go SDK leaves it
// nil when the object has no user metadata.
func s3HeadObject(c *s3.Client) handler {
	return func(ctx context.Context, params []byte) (string, error) {
		in := new(s3.HeadObjectInput)
		if err := decode(params, in); err != nil {
			return "", err
		}
		out, err := c.HeadObject(ctx, in)
		if err != nil {
			return "", err
		}
		var extra map[string]any
		if out.Metadata == nil {
			extra = map[string]any{"Metadata": map[string]any{}}
		}
		return encode(out, extra)
	}
}

// s3PutObject takes Body as a string. Any other JSON value is written as its
// JSON text.
func s3PutObject(c *s3.Client) handler {
	return func(ctx context.Context, params []byte) (string, error) {
		rest, rawBody, hasBody, err := splitMember(params, "Body")
		if err != nil {
			return "", err
		}
		in := new(s3.PutObjectInput)
		if err := decode(rest, in); err != nil {
			return "", err
		}
		if hasBody {
			in.Body = strings.NewReader(textOf(rawBody))
		}
		out, err := c.PutObject(ctx, in)
		if err != nil {
			return "", err
		}
		return encode(out, nil)
	}
}
