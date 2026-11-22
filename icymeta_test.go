package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// parseIcy splits an ICY stream back into audio and the metadata blocks seen.
func parseIcy(t *testing.T, b []byte, metaint int) (audio []byte, metas []string) {
	t.Helper()
	for len(b) > 0 {
		n := min(metaint, len(b))
		audio = append(audio, b[:n]...)
		b = b[n:]
		if len(b) == 0 {
			break
		}
		l := int(b[0]) * 16
		if len(b) < 1+l {
			t.Fatalf("truncated metadata block")
		}
		metas = append(metas, strings.TrimRight(string(b[1:1+l]), "\x00"))
		b = b[1+l:]
	}
	return audio, metas
}

func TestIcyMetaWriter(t *testing.T) {
	var out bytes.Buffer
	m := newIcyMetaWriter(&out)
	audio := make([]byte, 50000)
	for i := range audio {
		audio[i] = byte(i)
	}
	// Uneven writes check the count carries across segments.
	var fracs []float64
	titles := func(frac float64) string {
		fracs = append(fracs, frac)
		if frac < 0.5 {
			return "first"
		}
		return "second"
	}
	for _, part := range [][]byte{audio[:7000], audio[7000:40000], audio[40000:]} {
		if err := m.WriteAudio(part, titles); err != nil {
			t.Fatal(err)
		}
	}
	got, metas := parseIcy(t, out.Bytes(), icyMetaInt)
	if !bytes.Equal(got, audio) {
		t.Fatal("audio altered")
	}
	// Boundaries at 16000 and 32000 (both in the 33000-byte middle write, either
	// side of its midpoint) and 48000, where the title is unchanged.
	want := []string{"StreamTitle='first';", "StreamTitle='second';", ""}
	if fmt.Sprint(metas) != fmt.Sprint(want) {
		t.Fatalf("metas = %q, want %q", metas, want)
	}
	if fmt.Sprintf("%.3f", fracs) != "[0.273 0.758 0.800]" {
		t.Fatalf("fracs = %.3f", fracs)
	}
}

func TestEncodeIcyMeta(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"Don't Stop", "StreamTitle='Don't Stop';"},
		{"evil';x", "StreamTitle='evil'x';"},
		{"evil';;x", "StreamTitle='evil'x';"},
		{"", "StreamTitle='';"},
	} {
		b := encodeIcyMeta(tc.in)
		if len(b)-1 != int(b[0])*16 {
			t.Fatalf("%q: length byte %d for %d bytes", tc.in, b[0], len(b)-1)
		}
		if got := strings.TrimRight(string(b[1:]), "\x00"); got != tc.want {
			t.Errorf("%q: got %q want %q", tc.in, got, tc.want)
		}
	}
	long := encodeIcyMeta(strings.Repeat("é", 5000))
	if long[0] != 255 || !strings.HasSuffix(strings.TrimRight(string(long[1:]), "\x00"), "é';") {
		t.Fatalf("long title not truncated cleanly: len byte %d", long[0])
	}
}
