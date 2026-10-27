package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/sirupsen/logrus"
)

func TestCalcSleep(t *testing.T) {
	now := time.Now()
	nowFn = func() time.Time { return now }
	defer func() { nowFn = time.Now }()

	for _, tc := range []struct {
		Name       string
		StartTime  time.Time
		ServedTime time.Duration
		WantSleep  time.Duration
	}{
		{
			Name:       "Behind by 30 seconds",
			StartTime:  now.Add(-1 * time.Minute),
			ServedTime: 30 * time.Second,
			WantSleep:  1 * time.Nanosecond,
		},
		{
			Name:       "On time",
			StartTime:  now.Add(-1 * time.Minute),
			ServedTime: 1*time.Minute + 30*time.Second,
			WantSleep:  1 * time.Nanosecond,
		},
		{
			Name:       "40 seconds ahead",
			StartTime:  now.Add(-1 * time.Minute),
			ServedTime: 1*time.Minute + 40*time.Second,
			WantSleep:  10 * time.Second,
		},
		{
			Name:       "50 seconds ahead",
			StartTime:  now.Add(-1 * time.Hour),
			ServedTime: 1*time.Hour + 50*time.Second,
			WantSleep:  20 * time.Second,
		},
		{
			Name:       "Behind by 1 minute",
			StartTime:  now.Add(-1 * time.Hour),
			ServedTime: 1*time.Hour - 1*time.Minute - 30*time.Second,
			WantSleep:  1 * time.Nanosecond,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			got := calculateIcySleep(tc.StartTime, tc.ServedTime)
			if got != tc.WantSleep {
				t.Errorf("want sleep %s, got: %s", tc.WantSleep, got)
			}
		})
	}
}

// shortReader models network reads which split 188-byte TS packets.
type shortReader struct{ io.Reader }

func (r shortReader) Read(p []byte) (int, error) {
	if len(p) > 17 {
		p = p[:17]
	}
	return r.Reader.Read(p)
}

func TestDemuxAAC(t *testing.T) {
	ts, err := os.ReadFile("testdata/aac-pid221.ts")
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/aac-pid221.aac")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragmented := range []bool{false, true} {
		var r io.Reader = bytes.NewReader(ts)
		if fragmented {
			r = shortReader{r}
		}
		var out bytes.Buffer
		if err := demuxAAC(&out, r); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Bytes(), want) {
			t.Fatalf("fragmented=%t: extracted %d bytes, want %d identical AAC bytes", fragmented, out.Len(), len(want))
		}
	}
	for _, data := range [][]byte{nil, ts[:188], ts[:len(ts)-1], ts[:3*188]} {
		if err := demuxAAC(io.Discard, bytes.NewReader(data)); err == nil {
			t.Fatalf("expected error for %d-byte incomplete stream", len(data))
		}
	}
}

type icyTestTransport func(*http.Request) (*http.Response, error)

func (f icyTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestIcecastChunkResponse(t *testing.T) {
	ts, err := os.ReadFile("testdata/aac-pid221.ts")
	if err != nil {
		t.Fatal(err)
	}
	aac, err := os.ReadFile("testdata/aac-pid221.aac")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		chunkID    string
		body       []byte
		storeError bool
		status     int
	}{
		{"transport stream", "segment.ts", ts, false, http.StatusOK},
		{"raw AAC", "segment.aac", aac, false, http.StatusOK},
		{"invalid TS", "segment.ts", []byte("invalid"), false, http.StatusBadGateway},
		{"empty AAC", "segment.aac", nil, false, http.StatusBadGateway},
		{"storage failure", "segment.ts", nil, true, http.StatusBadGateway},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			client := s3.New(s3.Options{
				Region: "us-east-1", BaseEndpoint: aws.String("https://storage.invalid"), UsePathStyle: true,
				Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1,
				HTTPClient: &http.Client{Transport: icyTestTransport(func(*http.Request) (*http.Response, error) {
					if tc.storeError {
						return nil, errors.New("storage unavailable")
					}
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(tc.body))}, nil
				})},
			})
			idx := newChunkIndex()
			// A long duration makes the handler wait after the first chunk. Cancel
			// on Flush to verify that the player receives bytes before that wait.
			if err := idx.RecordChunk(ctx, "test", tc.chunkID, 60, time.Now().Add(-time.Minute)); err != nil {
				t.Fatal(err)
			}
			store := newS3ChunkStore(client, "test", time.Hour, idx)
			l := logrus.New()
			l.SetOutput(io.Discard)
			server := newIcyServer(l, []configStream{{ID: "test", Name: "Test", BaseTimezone: "Europe/Berlin"}}, idx, store)
			rec := &cancelOnFlush{ResponseRecorder: httptest.NewRecorder(), cancel: cancel}
			req := httptest.NewRequest(http.MethodGet, "/icecast?stream=test&tz=Europe%2FBerlin", nil).WithContext(ctx)
			server.ServeIcecast(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status %d, want %d: %s", rec.Code, tc.status, rec.Body.String())
			}
			if tc.status == http.StatusOK {
				if !rec.Flushed {
					t.Error("audio was not flushed")
				}
				if !bytes.Equal(rec.Body.Bytes(), aac) {
					t.Errorf("audio differs: got %d bytes, want %d", rec.Body.Len(), len(aac))
				}
			}
		})
	}
}

type cancelOnFlush struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w *cancelOnFlush) Flush() { w.ResponseRecorder.Flush(); w.cancel() }
