package tjts

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var (
	testChunks = [][]byte{
		{0x00, 0x00, 0x00, 0x00},
		{0x01, 0x01, 0x01, 0x01},
		{0x02, 0x02, 0x02, 0x02},
		{0x03, 0x03, 0x03, 0x03},
		{0x04, 0x04, 0x04, 0x04},
		{0x05, 0x05, 0x05, 0x05},
		{0x06, 0x06, 0x06, 0x06},
		{0x07, 0x07, 0x07, 0x07},
		{0x08, 0x08, 0x08, 0x08},
		{0x09, 0x09, 0x09, 0x09},
	}
	testTimestamps = []time.Time{
		time.Unix(1631624400, 0),
		time.Unix(1631624400, 0).Add(1 * time.Second),
		time.Unix(1631624400, 0).Add(2 * time.Second),
		time.Unix(1631624400, 0).Add(3 * time.Second),
		time.Unix(1631624400, 0).Add(4 * time.Second),
		time.Unix(1631624400, 0).Add(5 * time.Second),
		time.Unix(1631624400, 0).Add(6 * time.Second),
		time.Unix(1631624400, 0).Add(7 * time.Second),
		time.Unix(1631624400, 0).Add(8 * time.Second),
		time.Unix(1631624400, 0).Add(9 * time.Second),
	}
)

const (
	testChunkBitrate = 32
	testMaxOffset    = 10 * time.Second
)

func TestBufE2E(t *testing.T) {
	p := filepath.Join(t.TempDir(), "data.dat")

	b, err := openBuffer(p, testChunkBitrate, 1*time.Second, testMaxOffset)
	if err != nil {
		t.Fatal(err)
	}

	if b.slots != len(testChunks) {
		t.Fatalf("want %d slots but found %d. This might be the max offset not lining up with the test data length", len(testChunks), b.slots)
	}

	for i, c := range testChunks {
		t.Logf("writing chunk %d to currPos %d", i, b.currSlot())
		if err := b.WriteChunk(testTimestamps[i], c); err != nil {
			t.Fatal(err)
		}
	}

	// start near the middle, expect to iterate the lot
	var (
		idx = 4
		at  = idx - 1
	)

	for {
		if b.currSlot() == idx {
			break
		}
		b.incSlot()
	}

	c, err := b.CursorFrom(testTimestamps[idx].Add(1 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < len(testChunks); i++ {
		got, err := c.Next()
		if err != nil {
			t.Fatal(err)
		}
		want := []byte{byte(at), byte(at), byte(at), byte(at)}
		if !bytes.Equal(got, want) {
			t.Errorf("idx %d at %d, want %v got %v", i, at, want, got)
		}
		at++
		if at >= 10 {
			at = 0
		}
	}
}

func TestBufCalcs(t *testing.T) {
	const slots = 4

	var (
		ts []time.Time
		tb [][]byte
	)
	for i := 0; i < 4; i++ {
		tm := time.Unix(1631624400, 0).Add(time.Duration(i) * time.Second)
		ts = append(ts, tm)
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(tm.Unix()))
		tb = append(tb, b)
	}
	if len(ts) != 4 {
		t.Fatalf("want 4 ts, got: %d", len(ts))
	}
	for _, it := range tb {
		if len(it) != timestampLen {
			t.Fatalf("want ts len %d, got %d", timestampLen, len(it))
		}
	}

	// now, we build what we'd expect in a dataset from scratch

	/* header */
	d := []byte{
		0x00, 0x01, // v1 header
		16, 0x00, 0x00, 0x00, // bitrate, 16 in LE
		8, 0x00, 0x00, 0x00, // maxOffset seconds, 8 in LE
		2, 0x00, 0x00, 0x00, // current slot, 2 in LE
	}
	assertUint32(t, d[2:6], 16)
	assertUint32(t, d[6:10], 8)
	assertUint32(t, d[10:14], 2)

	// append 4 slots, with identifying 16 bit data
	for i := 0; i < slots; i++ {
		d = append(d, tb[i]...)         // append the timestamp
		d = append(d, byte(i), byte(i)) // append two identifying bytes, to match bitrate of 16
	}

	// returns a new buf with a fresh duple of data
	makeb := func() *diskBuf {
		dbd := make([]byte, len(d))
		copy(dbd, d)
		return &diskBuf{
			slots:   slots,
			slotLen: 2,
			d:       dbd,
		}
	}

	t.Run("header peek", func(t *testing.T) {
		fp := filepath.Join(t.TempDir(), "dat")

		if err := os.WriteFile(fp, d, 0644); err != nil {
			t.Fatal(err)
		}

		br, mo, err := peekHeader(fp)
		if err != nil {
			t.Fatal(err)
		}
		if br != 16 || mo != 8 {
			t.Fatalf("want bitrate %d and maxOffset %d, but trying to reate for %d and %d", 16, 8, br, mo)
		}
	})

	t.Run("slot calcs", func(t *testing.T) {
		b := makeb()

		for i, it := range ts {
			tsa := b.tsAt(i)
			if !tsa.Equal(it) {
				t.Errorf("at slot %d, want time %s but got %s", i, it, tsa)
			}
		}

		for i := 0; i < slots; i++ {
			p := b.slotPos(i)
			if b.d[p+8] != byte(i) {
				t.Errorf("idx %d +8 (past timestamp, first byte) want byte %d, got %d", i, byte(i), byte(b.d[p+8]))
			}
			if b.d[p+9] != byte(i) {
				t.Errorf("idx %d +9 (past timestamp, other byte) want byte %d, got %d", i, byte(i), byte(b.d[p+9]))
			}
		}
	})

	t.Run("slot incrementing", func(t *testing.T) {
		b := makeb()
		b.d[10] = 0 // reset current slot to 0

		for i := 0; i < slots; i++ {
			gs := b.currSlot()
			if gs != i {
				t.Errorf("idx %d curr slot should be %d, got %d", i, i, gs)
			}
			b.incSlot()
		}

		cs := b.currSlot()
		if cs != 0 {
			t.Errorf("after writing to each slot, the curr slot should be reset to 0, but got: %d", cs)
		}

		b.incSlot()
		ns := b.currSlot()
		if ns != 1 {
			t.Errorf("inc past end + 1 should return new slot 1, got: %d", ns)
		}
	})

	t.Run("calcBufSize", func(t *testing.T) {
		l, s, err := calcBufSize(16, 1*time.Second, 4*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if len(d) != l+v1HeaderLen {
			t.Errorf("want len %d, got: %d", len(d), l+v1HeaderLen)
		}
		if s != len(ts) {
			t.Errorf("want %d slots, got: %d", len(ts), s)
		}
	})
}

func assertUint32(t *testing.T, d []byte, v uint32) {
	if len(d) != 4 {
		t.Fatal("d should be 4 bytes")
	}
	g := binary.LittleEndian.Uint32(d)
	if g != v {
		t.Fatalf("want %d, got %d", v, g)
	}
}
