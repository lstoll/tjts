package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// s3ChunkStore uploads segments to S3-compatible storage and issues presigned GET URLs.
type s3ChunkStore struct {
	client     *s3.Client
	presign    *s3.PresignClient
	bucket     string
	presignTTL time.Duration
	idx        *chunkIndex
}

func newS3ChunkStore(client *s3.Client, bucket string, presignTTL time.Duration, idx *chunkIndex) *s3ChunkStore {
	if presignTTL <= 0 {
		presignTTL = time.Hour
	}
	return &s3ChunkStore{
		client:     client,
		presign:    s3.NewPresignClient(client),
		bucket:     bucket,
		presignTTL: presignTTL,
		idx:        idx,
	}
}

// LoadStream lists existing objects for a stream and rebuilds the in-memory index.
func (s *s3ChunkStore) LoadStream(ctx context.Context, streamID string) error {
	prefix := streamID + "/"
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	type row struct {
		keyTime time.Time
		rc      recordedChunk
	}
	var rows []row
	for paginator.HasMorePages() {
		out, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list %s: %w", streamID, err)
		}
		for _, obj := range out.Contents {
			if obj.Key == nil || obj.LastModified == nil {
				continue
			}
			_, kt, dur, chunkID, err := decodeObjectKey(*obj.Key)
			if err != nil {
				continue
			}
			lm := *obj.LastModified
			rows = append(rows, row{
				keyTime: kt,
				rc: recordedChunk{
					ChunkID:   chunkID,
					Duration:  dur,
					FetchedAt: lm.UTC(),
					ObjectKey: *obj.Key,
				},
			})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].keyTime.Equal(rows[j].keyTime) {
			return rows[i].keyTime.Before(rows[j].keyTime)
		}
		return rows[i].rc.ObjectKey < rows[j].rc.ObjectKey
	})
	chunks := make([]recordedChunk, len(rows))
	for i := range rows {
		chunks[i] = rows[i].rc
		chunks[i].Sequence = i + 1
	}
	s.idx.ReplaceStream(streamID, chunks)
	return nil
}

// FetcherStore returns chunk storage for one stream's fetcher.
func (s *s3ChunkStore) FetcherStore(streamID string) *stationChunkStore {
	return &stationChunkStore{streamID: streamID, parent: s}
}

// PresignedGET returns a time-limited URL to download the segment.
func (s *s3ChunkStore) PresignedGET(ctx context.Context, rc recordedChunk) (string, error) {
	out, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(rc.ObjectKey),
	}, s3.WithPresignExpires(s.presignTTL))
	if err != nil {
		return "", fmt.Errorf("presign %s: %w", rc.ObjectKey, err)
	}
	return out.URL, nil
}

// GetObjectReader streams an object body (e.g. for ICY).
func (s *s3ChunkStore) GetObjectReader(ctx context.Context, rc recordedChunk) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(rc.ObjectKey),
	})
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", rc.ObjectKey, err)
	}
	return out.Body, nil
}

// DeleteObject removes an object from the bucket.
func (s *s3ChunkStore) DeleteObject(ctx context.Context, objectKey string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("delete %s: %w", objectKey, err)
	}
	return nil
}

type stationChunkStore struct {
	streamID string
	parent   *s3ChunkStore
}

func (s *stationChunkStore) WriteChunk(ctx context.Context, chunkName string, chunkDuration float64, r io.Reader) error {
	if s.parent.idx.HasLogical(s.streamID, chunkName) {
		return nil
	}
	seq := s.parent.idx.NextSequence(s.streamID)
	ts := time.Now().UTC()
	key := encodeObjectKey(s.streamID, ts, chunkDuration, chunkName)
	body, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read chunk body: %w", err)
	}
	_, err = s.parent.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.parent.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}
	s.parent.idx.Append(s.streamID, recordedChunk{
		Sequence:  seq,
		ChunkID:   chunkName,
		Duration:  chunkDuration,
		FetchedAt: ts,
		ObjectKey: key,
	})
	return nil
}

func (s *stationChunkStore) ChunkExists(_ context.Context, chunkName string) bool {
	return s.parent.idx.HasLogical(s.streamID, chunkName)
}
