package main

import (
	"io"
	"strings"
	"unicode/utf8"
)

// icyMetaInt is the number of audio bytes between ICY metadata blocks.
const icyMetaInt = 16000

// icyMaxMeta is the largest payload a one-byte length (x16) can describe.
const icyMaxMeta = 255 * 16

// icyMetaWriter interleaves ICY metadata blocks into an audio stream. The byte
// count carries across writes, as blocks don't align with segment boundaries.
type icyMetaWriter struct {
	w io.Writer
	// untilMeta is the number of audio bytes left before the next block.
	untilMeta int
	sent      bool
	last      string
}

func newIcyMetaWriter(w io.Writer) *icyMetaWriter {
	return &icyMetaWriter{w: w, untilMeta: icyMetaInt}
}

// WriteAudio writes audio, inserting a block at each metaint boundary. titleAt
// gets the boundary's position within audio as a fraction in [0,1], so the
// title reflects the audio at that point rather than the segment start.
func (m *icyMetaWriter) WriteAudio(audio []byte, titleAt func(frac float64) string) error {
	total := len(audio)
	for len(audio) > 0 {
		n := min(m.untilMeta, len(audio))
		if _, err := m.w.Write(audio[:n]); err != nil {
			return err
		}
		audio = audio[n:]
		m.untilMeta -= n
		if m.untilMeta > 0 {
			return nil
		}
		frac := float64(total-len(audio)) / float64(total)
		if _, err := m.w.Write(m.block(titleAt(frac))); err != nil {
			return err
		}
		m.untilMeta = icyMetaInt
	}
	return nil
}

// block returns the encoded metadata. An unchanged title is sent as an empty
// block, which players treat as "no change".
func (m *icyMetaWriter) block(title string) []byte {
	if m.sent && title == m.last {
		return []byte{0}
	}
	m.sent, m.last = true, title
	return encodeIcyMeta(title)
}

func encodeIcyMeta(title string) []byte {
	// Parsers find the end of the value by "';", so it can't appear inside.
	for strings.Contains(title, "';") {
		title = strings.ReplaceAll(title, "';", "'")
	}
	title = strings.ToValidUTF8(title, "")
	const prefix, suffix = "StreamTitle='", "';"
	if maxTitle := icyMaxMeta - len(prefix) - len(suffix); len(title) > maxTitle {
		title = title[:maxTitle]
		for !utf8.ValidString(title) {
			title = title[:len(title)-1]
		}
	}
	payload := prefix + title + suffix
	blocks := (len(payload) + 15) / 16
	out := make([]byte, 1+blocks*16)
	out[0] = byte(blocks)
	copy(out[1:], payload)
	return out
}
