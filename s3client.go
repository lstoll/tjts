package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func newS3Client(ctx context.Context, sc s3Config) (*s3.Client, error) {
	if sc.Region == "" {
		return nil, fmt.Errorf("s3.region is required")
	}
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(sc.Region),
		config.WithHTTPClient(&http.Client{Timeout: 30 * time.Second}),
	}
	if sc.AccessKey != "" && sc.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(sc.AccessKey, sc.SecretKey, ""),
		))
	}
	awscfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	var s3opts []func(*s3.Options)
	if sc.Endpoint != "" {
		s3opts = append(s3opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(sc.Endpoint)
			o.UsePathStyle = sc.UsePathStyle
		})
	} else if sc.UsePathStyle {
		s3opts = append(s3opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
	return s3.NewFromConfig(awscfg, s3opts...), nil
}

func ensureS3Bucket(ctx context.Context, client *s3.Client, bucket string) error {
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}
	var responseError *smithyhttp.ResponseError
	if !errors.As(err, &responseError) || responseError.HTTPStatusCode() != http.StatusNotFound {
		return fmt.Errorf("head bucket: %w", err)
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	// AWS requires LocationConstraint outside us-east-1; compatible endpoints
	// generally accept the same constraint.
	if client.Options().Region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{LocationConstraint: types.BucketLocationConstraint(client.Options().Region)}
	}
	_, err = client.CreateBucket(ctx, input)
	if err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}
