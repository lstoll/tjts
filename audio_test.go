package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

func audioFixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Change a fixture's PMT and packet headers together. No fixed audio PID is
// allowed to leak into detection, including when adjacent chunks change PID.
func remapAudio(t testing.TB, b []byte, pid int, streamType byte) []byte {
	t.Helper()
	b = bytes.Clone(b)
	for at := 0; at < len(b); at += 188 {
		packet := b[at : at+188]
		p, err := parseTSPacket(packet)
		if err != nil {
			t.Fatal(err)
		}
		if p.pid == 221 {
			packet[1] = (packet[1] & 0xe0) | byte(pid>>8)
			packet[2] = byte(pid)
		}
		if p.pid == 256 && p.start {
			section := p.body[1+int(p.body[0]):]
			n := 3 + (int(section[1]&15)<<8 | int(section[2]))
			section = section[:n]
			es := 12 + (int(section[10]&15)<<8 | int(section[11]))
			section[es] = streamType
			section[es+1] = section[es+1]&0xe0 | byte(pid>>8)
			section[es+2] = byte(pid)
			binary.BigEndian.PutUint32(section[n-4:], mpegCRC(section[:n-4]))
		}
	}
	return b
}

func TestAudioDetection(t *testing.T) {
	ts := audioFixture(t, "aac-pid221.ts")
	aac := audioFixture(t, "aac-pid221.aac")
	for _, pid := range []int{221, 257, 512, 8190} {
		audio, info, err := extractAAC(remapAudio(t, ts, pid, 15))
		if err != nil || info.pid != pid || !bytes.Equal(audio, aac) {
			t.Fatalf("PID %d: info=%+v err=%v", pid, info, err)
		}
	}
	id3 := append([]byte{'I', 'D', '3', 4, 0, 0, 0, 0, 0, 3, 'a', 'b', 'c'}, aac...)
	for _, b := range [][]byte{aac, id3} {
		audio, info, err := extractAAC(b)
		if err != nil || info.format != "adts" || !bytes.Equal(audio, aac) {
			t.Fatalf("packed AAC: %+v %v", info, err)
		}
	}
	badCC := bytes.Clone(ts)
	for at := 0; at < len(badCC); at += 188 {
		p, err := parseTSPacket(badCC[at : at+188])
		if err != nil {
			t.Fatal(err)
		}
		if p.pid == 221 && !p.start {
			badCC[at+3] ^= 1
			break
		}
	}
	badPMT := bytes.Clone(ts)
	badPMT[188+20] ^= 0x40
	for _, b := range [][]byte{nil, ts[:len(ts)-1], aac[:len(aac)-1], badCC, badPMT, remapAudio(t, ts, 221, 0x11), []byte("<html>failure</html>"), {'I', 'D', '3', 4, 0, 0, 127, 127, 127, 127}} {
		if _, _, err := extractAAC(b); err == nil {
			t.Fatalf("accepted corrupt or unsupported %d-byte audio", len(b))
		}
	}
}

func FuzzExtractAAC(f *testing.F) {
	f.Add(audioFixture(f, "aac-pid221.ts"))
	f.Add(audioFixture(f, "aac-pid221.aac"))
	f.Add([]byte("ID3"))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > maxSegmentBytes {
			t.Skip()
		}
		audio, _, err := extractAAC(b)
		if err == nil {
			if err := validateADTS(audio); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func mpegCRC(b []byte) uint32 {
	crc := uint32(0xffffffff)
	for _, v := range b {
		crc ^= uint32(v) << 24
		for range 8 {
			if crc&0x80000000 != 0 {
				crc = crc<<1 ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
