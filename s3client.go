package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func newS3Client(ctx context.Context, sc s3Config) (*s3.Client, error) {
	if sc.Region == "" {
		return nil, fmt.Errorf("s3.region is required")
	}
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(sc.Region),
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
	if _, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}); err == nil {
		return nil
	}
	_, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return fmt.Errorf("create bucket %q: %w", bucket, err)
	}
	return nil
}
