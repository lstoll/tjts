package main

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// encodeObjectKey builds the S3 object key. ts should be UTC (e.g. time.Now().UTC()).
// The fixed-width Unix nanoseconds segment sorts lexicographically in time order.
// HLS MEDIA-SEQUENCE lives only in recordedChunk.Sequence, not in the key.
func encodeObjectKey(streamID string, ts time.Time, durationSec float64, chunkID string) string {
	ts = ts.UTC()
	durMs := int(durationSec*1000 + 0.5)
	enc := base64.RawURLEncoding.EncodeToString([]byte(chunkID))
	// 19 digits fits int64 Unix nanoseconds; lexicographic order = time order.
	return fmt.Sprintf("%s/%019d__d%d__%s", streamID, ts.UnixNano(), durMs, enc)
}

func decodeObjectKey(key string) (streamID string, keyTime time.Time, durationSec float64, chunkID string, err error) {
	i := strings.IndexByte(key, '/')
	if i <= 0 || i >= len(key)-1 {
		return "", time.Time{}, 0, "", fmt.Errorf("invalid key %q", key)
	}
	streamID = key[:i]
	suffix := key[i+1:]
	parts := strings.SplitN(suffix, "__", 3)
	if len(parts) != 3 {
		return "", time.Time{}, 0, "", fmt.Errorf("invalid key suffix in %q", key)
	}
	nano, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return "", time.Time{}, 0, "", fmt.Errorf("timestamp in %q: %w", key, err)
	}
	if len(parts[1]) < 2 || parts[1][0] != 'd' {
		return "", time.Time{}, 0, "", fmt.Errorf("duration marker in %q", key)
	}
	durMs, err := strconv.Atoi(parts[1][1:])
	if err != nil {
		return "", time.Time{}, 0, "", fmt.Errorf("duration in %q: %w", key, err)
	}
	b, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", time.Time{}, 0, "", fmt.Errorf("chunk id encoding in %q: %w", key, err)
	}
	chunkID = string(b)
	durationSec = float64(durMs) / 1000.0
	keyTime = time.Unix(0, nano).UTC()
	return streamID, keyTime, durationSec, chunkID, nil
}
