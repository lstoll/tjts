package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/asticode/go-astits"
)

const maxSegmentBytes = 16 << 20
const tsPacketSize = 188

func readSegment(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, maxSegmentBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) == 0 || len(b) > maxSegmentBytes {
		return nil, fmt.Errorf("audio segment must contain 1..%d bytes", maxSegmentBytes)
	}
	return b, nil
}

type audioInfo struct {
	format string
	pid    int
}

// Inspect the bytes, rather than trusting a filename or upstream content type.
func extractAAC(b []byte) ([]byte, audioInfo, error) {
	if len(b) > 0 && b[0] == 0x47 {
		out, pid, err := extractTSAAC(b)
		return out, audioInfo{"mpegts", pid}, err
	}
	// Packed audio HLS can prefix ADTS with an ID3 timestamp tag.
	for len(b) >= 10 && string(b[:3]) == "ID3" {
		size := 0
		for _, v := range b[6:10] {
			if v&0x80 != 0 {
				return nil, audioInfo{}, fmt.Errorf("invalid ID3 size")
			}
			size = size<<7 | int(v)
		}
		size += 10
		if b[5]&0x10 != 0 {
			size += 10
		}
		if size > len(b) {
			return nil, audioInfo{}, fmt.Errorf("truncated ID3 tag")
		}
		b = b[size:]
	}
	return b, audioInfo{"adts", -1}, validateADTS(b)
}

func validateADTS(b []byte) error {
	if len(b) == 0 {
		return fmt.Errorf("no AAC frames")
	}
	for len(b) > 0 {
		if len(b) < 7 || b[0] != 0xff || b[1]&0xf6 != 0xf0 || b[2]>>2&15 >= 13 {
			return fmt.Errorf("invalid ADTS AAC header")
		}
		n := int(b[3]&3)<<11 | int(b[4])<<3 | int(b[5]>>5)
		header := 7
		if b[1]&1 == 0 {
			header = 9
		}
		if n < header || n > len(b) {
			return fmt.Errorf("truncated ADTS AAC frame")
		}
		b = b[n:]
	}
	return nil
}

type tsPayload struct {
	pid           int
	start         bool
	cc            byte
	discontinuity bool
	body          []byte
}

func parseTSPacket(b []byte) (tsPayload, error) {
	var p tsPayload
	if len(b) != tsPacketSize || b[0] != 0x47 || b[1]&0x80 != 0 || b[3]&0xc0 != 0 {
		return p, fmt.Errorf("invalid, damaged or encrypted TS packet")
	}
	p.pid = int(b[1]&31)<<8 | int(b[2])
	p.start = b[1]&0x40 != 0
	p.cc = b[3] & 15
	control := b[3] >> 4 & 3
	if control == 0 {
		return p, fmt.Errorf("invalid TS adaptation control")
	}
	at := 4
	if control&2 != 0 {
		at += 1 + int(b[4])
		if at > 188 {
			return p, fmt.Errorf("invalid TS adaptation length")
		}
		if b[4] > 0 {
			p.discontinuity = b[5]&0x80 != 0
		}
	}
	if control&1 != 0 {
		if at >= 188 {
			return p, fmt.Errorf("missing TS payload")
		}
		p.body = b[at:]
	}
	return p, nil
}

// go-astits handles PSI sections, descriptors and PES assembly. Preflight
// packet boundaries and continuity here: the demuxer can otherwise skip damaged
// packets and return apparently successful, truncated audio.
func extractTSAAC(b []byte) ([]byte, int, error) {
	if len(b) == 0 || len(b)%tsPacketSize != 0 {
		return nil, -1, fmt.Errorf("truncated TS segment")
	}
	type previousPacket struct {
		cc     byte
		packet []byte
	}
	previous := make(map[int]previousPacket)
	for at := 0; at < len(b); at += tsPacketSize {
		packet := b[at : at+tsPacketSize]
		p, err := parseTSPacket(packet)
		if err != nil {
			return nil, -1, err
		}
		if len(p.body) == 0 || p.pid == 8191 {
			continue
		}
		if prev, ok := previous[p.pid]; ok && !p.discontinuity {
			if p.cc == prev.cc && bytes.Equal(packet, prev.packet) {
				continue
			}
			if p.cc != (prev.cc+1)&15 {
				return nil, -1, fmt.Errorf("TS PID %d continuity error", p.pid)
			}
		}
		previous[p.pid] = previousPacket{p.cc, packet}
	}
	var parserErrors bytes.Buffer
	demux := astits.NewDemuxer(context.Background(), bytes.NewReader(b),
		astits.DemuxerOptPacketSize(tsPacketSize),
		astits.DemuxerOptLogger(log.New(&parserErrors, "", 0)))
	var audio []byte
	audioPID := -1
	for {
		data, err := demux.NextData()
		if errors.Is(err, astits.ErrNoMorePackets) {
			break
		}
		if err != nil {
			return nil, audioPID, fmt.Errorf("demux TS: %w", err)
		}
		if data.PAT != nil {
			programs := 0
			for _, program := range data.PAT.Programs {
				if program.ProgramNumber != 0 {
					programs++
				}
			}
			if programs != 1 {
				return nil, audioPID, fmt.Errorf("expected one TS program, got %d", programs)
			}
		}
		if data.PMT != nil {
			detected := -1
			for _, stream := range data.PMT.ElementaryStreams {
				if stream.StreamType == astits.StreamTypeADTS {
					detected = int(stream.ElementaryPID)
					break
				}
			}
			if detected < 0 {
				return nil, audioPID, fmt.Errorf("no ADTS AAC stream in PMT")
			}
			if audioPID >= 0 && audioPID != detected {
				return nil, audioPID, fmt.Errorf("AAC PID changed within segment")
			}
			audioPID = detected
		}
		if data.PES != nil && int(data.PID) == audioPID {
			audio = append(audio, data.PES.Data...)
		}
	}
	// At EOF go-astits logs errors for incomplete buffered PES instead of
	// returning them. Treat those as failures, even if earlier PES decoded.
	if parserErrors.Len() > 0 {
		return nil, audioPID, fmt.Errorf("demux TS: %s", parserErrors.String())
	}
	return audio, audioPID, validateADTS(audio)
}

func demuxAAC(w io.Writer, r io.Reader) error {
	b, err := readSegment(r)
	if err != nil {
		return err
	}
	out, _, err := extractTSAAC(b)
	if err != nil {
		return err
	}
	n, err := w.Write(out)
	if err == nil && n != len(out) {
		return io.ErrShortWrite
	}
	return err
}
