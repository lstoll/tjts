package tjts

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"syscall"
	"time"
)

var (
	hdrv1 = []byte{0x00, 0x01}
)

const (
	v1HeaderLen = 2 + 4 + 4 + 4 // v1 + uint32 bitrate + uint32 maxOffset seconds + uint32 currSlot
)

const (
	timestampLen = 8 // int64
)

type diskBuf struct {
	f *os.File
	// d is the file backed buffer we keep data in. The format is header,uint32 currSlot[int64
	// timestamp,2 seconds of fixed bitrate data]
	d []byte

	// slots is the number of slots we have in the buffer
	slots int
	// slotLen is how many bytes we keep per slot, excluding timestamp
	slotLen int

	mu sync.RWMutex
}

func openBuffer(path string, bitrate int, timePerChunk time.Duration, maxOffset time.Duration) (*diskBuf, error) {
	b := &diskBuf{}

	if _, err := os.Stat(path); err == nil {
		// peek to make sure the existing buffer lines up with what we're creating
		bbr, bmo, err := peekHeader(path)
		if err != nil {
			return nil, err
		}
		if bbr != bitrate || bmo != int(maxOffset.Seconds()) {
			return nil, fmt.Errorf("buffer on disk for bitrate %d and maxOffset %s, but trying to reate for %d and %s", bbr, time.Duration(bmo), bitrate, maxOffset)
		}
	}

	var err error
	b.f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	dlen, slots, err := calcBufSize(bitrate, timePerChunk, maxOffset)
	if err != nil {
		return nil, err
	}
	b.slots = slots
	b.slotLen = bitrate / 8 * int(timePerChunk.Seconds())

	dlen = dlen + v1HeaderLen

	if err := syscall.Ftruncate(int(b.f.Fd()), int64(dlen)); err != nil {
		_ = b.f.Close()
		return nil, fmt.Errorf("setting len on %s: %v", path, err)
	}

	b.d, err = syscall.Mmap(int(b.f.Fd()), 0, dlen, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		_ = b.f.Close()
		return nil, err
	}

	if len(b.d) != dlen {
		return nil, fmt.Errorf("mmap buffer should be %d bytes, but it %d", dlen, len(b.d))
	}

	// set header on file
	copy(b.d[0:2], hdrv1)
	binary.LittleEndian.PutUint32(b.d[2:6], uint32(bitrate))
	binary.LittleEndian.PutUint32(b.d[6:10], uint32(maxOffset.Seconds()))

	return b, nil
}

func (b *diskBuf) Close() error {
	if err := syscall.Munmap(b.d); err != nil {
		return err
	}
	return b.f.Close()
}

// ChunkLen returns the number of bytes chunks that are written to this buffer
// should be.
func (b *diskBuf) ChunkLen() int {
	return b.slotLen
}

// WriteChunk writes a chunk to the buffer. The data must be exactly ChunkLen long.
func (b *diskBuf) WriteChunk(t time.Time, d []byte) error {
	if len(d) != b.ChunkLen() {
		return fmt.Errorf("chunk must be exactly %d bytes long, got: %d", b.ChunkLen(), len(d))
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	idx := b.slotPos(b.currSlot())

	log.Printf("diskBuf WriteChunk: slot %d buffIdx %d", b.currSlot(), idx)

	binary.LittleEndian.PutUint64(b.d[idx:idx+timestampLen], uint64(t.Unix()))
	copy(b.d[idx+timestampLen:idx+timestampLen+b.ChunkLen()], d)

	b.incSlot()

	return nil
}

func (b *diskBuf) CursorFrom(f time.Time) (*cursor, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// starting from the current position, we want to walk back until we find
	// the first timestamp that is before our current position. If we loop the
	// entire way around and don't find one, go with the oldest position.
	//
	// TODO - this is a super naïve seek, we could probably optimize this a bit.

	var (
		foundIdx int       = b.currSlot() - 1
		oldest   time.Time = b.tsAt(b.currSlot())
	)

	// loop curr -> 0, then max -> curr
	i := b.currSlot() - 1
	if i < 0 {
		i = 0
	}
	for {
		if i < 0 {
			i = b.slots - 1
		}
		if i == b.currSlot() {
			// back to where we started, time to end
			break
		}
		st := b.tsAt(i)
		if st.Before(f) {
			foundIdx = i
			break
		}
		if st.Before(oldest) {
			oldest = st
			foundIdx = i
		}
		i--
	}

	log.Printf("diskBuf CursorFrom found idx %d for time %s", foundIdx, f)

	return &cursor{b: b, at: foundIdx}, nil
}

// currSlot indicate the current writing position inside the buffer. Cursor
// should be used for reading
func (b *diskBuf) currSlot() int {
	return int(binary.LittleEndian.Uint32(b.d[10:14]))
}

// incSlot increments the current writing position inside the buffer
func (b *diskBuf) incSlot() {
	next := b.currSlot() + 1
	if next >= b.slots {
		next = 0
	}

	binary.LittleEndian.PutUint32(b.d[10:14], uint32(next))
}

// slotPos returns the starting index for the given slot. This will correspond
// to where the timestamp is written.
func (b *diskBuf) slotPos(idx int) int {
	return v1HeaderLen + idx*(timestampLen+b.slotLen)
}

func (b *diskBuf) tsAt(idx int) time.Time {
	ut := binary.LittleEndian.Uint64(b.d[b.slotPos(idx) : b.slotPos(idx)+timestampLen])
	return time.Unix(int64(ut), 0)
}

type cursor struct {
	b  *diskBuf
	at int
}

func (c *cursor) Next() ([]byte, error) {
	c.b.mu.RLock()
	defer c.b.mu.RUnlock()

	// TODO - validate next ts > curr ts too

	i := c.b.slotPos(c.at) + timestampLen
	log.Printf("cursor Next: cursor slot %d bufIdx %d", c.at, i)
	c.at++
	if c.at >= c.b.slots {
		c.at = 0
	}

	return c.b.d[i : i+c.b.ChunkLen()], nil
}

// peekHeader reads an existing paths header, checking that it has the right
// version header, and returning the bitrate and max offset it was configured
func peekHeader(path string) (bitrate int, maxOffset int, err error) {
	r, err := os.Open(path)
	if err != nil {
		return 0, 0, fmt.Errorf("opening %s: %v", path, err)
	}

	h := make([]byte, v1HeaderLen)
	if _, err = io.ReadFull(r, h); err != nil {
		return 0, 0, fmt.Errorf("reading first %d bytes from %s: %v", v1HeaderLen, path, err)
	}
	if !bytes.Equal(hdrv1, h[0:2]) {
		return 0, 0, fmt.Errorf("not a buf v1 file")
	}
	br := binary.LittleEndian.Uint32(h[2:6])
	mo := binary.LittleEndian.Uint32(h[6:10])
	return int(br), int(mo), nil
}

func calcBufSize(bitrate int, timePerChunk time.Duration, maxOffset time.Duration) (len int, slots int, err error) {
	// first, work out how many chunks we need. we need the time to be perfectly
	// divisible by the chunk size
	if (maxOffset % timePerChunk) != 0 {
		return 0, 0, fmt.Errorf("maxOffset must be divisible in to %s chunks", timePerChunk)
	}
	slots = int(maxOffset / timePerChunk)
	// length is num chunks * bitrate in bytes * room for a uint64 per chunk
	if (bitrate % 8) != 0 {
		return 0, 0, fmt.Errorf("bitrate must be divisible in to bytes")
	}
	len = slots*(bitrate/8*int(timePerChunk.Seconds())) + slots*8
	return len, slots, nil
}
