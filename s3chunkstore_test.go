package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type memoryS3 struct {
	mu                sync.Mutex
	objects           map[string][]byte
	failPut, failList bool
	puts, lists       int
}

func testS3(t *testing.T) (*s3ChunkStore, *memoryS3) {
	t.Helper()
	backend := &memoryS3{objects: map[string][]byte{}}
	client := s3.New(s3.Options{Region: "us-east-1", BaseEndpoint: aws.String("https://storage.invalid"), UsePathStyle: true,
		Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1,
		HTTPClient: &http.Client{Transport: icyTestTransport(backend.request)},
	})
	return newS3ChunkStore(client, "test", time.Hour, newChunkIndex()), backend
}

func (m *memoryS3) request(r *http.Request) (*http.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := http.StatusOK
	var body []byte
	if r.URL.Query().Get("list-type") == "2" {
		m.lists++
		if m.failList {
			return nil, fmt.Errorf("list failed")
		}
		keys := []string{}
		for k := range m.objects {
			if strings.HasPrefix(k, r.URL.Query().Get("prefix")) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		start := 0
		if r.URL.Query().Get("continuation-token") != "" {
			start = 1
		}
		type object struct {
			Key          string
			LastModified string
		}
		result := struct {
			XMLName               xml.Name `xml:"ListBucketResult"`
			Contents              []object
			IsTruncated           bool
			NextContinuationToken string `xml:",omitempty"`
		}{}
		end := len(keys)
		if start == 0 && end > 1 {
			end = 1
			result.IsTruncated = true
			result.NextContinuationToken = "next"
		}
		for _, k := range keys[start:end] {
			result.Contents = append(result.Contents, object{k, "2030-01-01T00:00:00Z"})
		}
		var err error
		body, err = xml.Marshal(result)
		if err != nil {
			return nil, err
		}
	} else {
		key := strings.TrimPrefix(r.URL.Path, "/test/")
		switch r.Method {
		case http.MethodPut:
			if m.failPut {
				return nil, fmt.Errorf("put failed")
			}
			b, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			m.objects[key] = b
			m.puts++
		case http.MethodGet:
			var ok bool
			body, ok = m.objects[key]
			if !ok {
				status = http.StatusNotFound
			}
		case http.MethodDelete:
			delete(m.objects, key)
			status = http.StatusNoContent
		default:
			status = http.StatusForbidden
		}
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
}

func TestS3RestartRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, backend := testS3(t)
	aac, err := os.ReadFile("testdata/aac-pid221.aac")
	if err != nil {
		t.Fatal(err)
	}
	station := store.FetcherStore("radio")
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := station.WriteChunk(ctx, "one.aac", 1, bytes.NewReader(aac)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if backend.puts != 1 {
		t.Fatalf("duplicate upload: %d puts", backend.puts)
	}
	first, _ := store.idx.GetChunk("radio", "one.aac")
	if err := station.WriteChunk(ctx, "two.aac", 2, bytes.NewReader(aac)); err != nil {
		t.Fatal(err)
	}
	restored := newS3ChunkStore(store.client, "test", time.Hour, newChunkIndex())
	if err := restored.LoadStream(ctx, "radio"); err != nil {
		t.Fatal(err)
	}
	chunks, err := restored.idx.Chunks(ctx, "radio", 1, 10)
	if err != nil {
		t.Fatal(err)
	}
	if backend.lists != 2 || len(chunks) != 2 {
		t.Fatalf("pagination: %d lists, %d chunks", backend.lists, len(chunks))
	}
	if !chunks[0].FetchedAt.Equal(first.FetchedAt) {
		t.Fatalf("key timestamp lost on restore: %s != %s", chunks[0].FetchedAt, first.FetchedAt)
	}
	if chunks[0].ChunkID != "one.aac" || chunks[1].Sequence != 2 {
		t.Fatalf("bad restored order: %#v", chunks)
	}
	r, err := restored.GetObjectReader(ctx, chunks[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(b, aac) {
		t.Fatalf("bad restored body: %v", err)
	}
	backend.failList = true
	if err := restored.LoadStream(ctx, "radio"); err == nil {
		t.Fatal("expected list failure")
	}
	if _, ok := restored.idx.GetChunk("radio", "one.aac"); !ok {
		t.Fatal("failed list replaced index")
	}
	backend.failPut = true
	if err := station.WriteChunk(ctx, "failed.aac", 1, bytes.NewReader(aac)); err == nil {
		t.Fatal("expected put failure")
	}
	if _, ok := store.idx.GetChunk("radio", "failed.aac"); ok {
		t.Fatal("failed upload published")
	}
}

func TestS3RejectInvalidChunks(t *testing.T) {
	store, backend := testS3(t)
	for _, tc := range []struct {
		duration float64
		body     []byte
	}{{0, []byte("x")}, {1, nil}, {1, []byte("not audio")}, {1, make([]byte, maxSegmentBytes+1)}} {
		if err := store.FetcherStore("radio").WriteChunk(context.Background(), "bad", tc.duration, bytes.NewReader(tc.body)); err == nil {
			t.Fatal("accepted invalid segment")
		}
	}
	if backend.puts != 0 {
		t.Fatal("invalid segments uploaded")
	}
	backend.objects["radio/not-a-chunk-key"] = []byte("bad")
	if err := store.LoadStream(context.Background(), "radio"); err == nil {
		t.Fatal("accepted unrecognized object")
	}
}

func TestBucketCheckDoesNotCreateOnDeniedAccess(t *testing.T) {
	store, backend := testS3(t)
	if err := ensureS3Bucket(context.Background(), store.client, store.bucket); err == nil {
		t.Fatal("expected forbidden error")
	}
	if backend.puts != 0 {
		t.Fatal("attempted bucket creation on access failure")
	}
}
