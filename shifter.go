package tjts

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"time"
)

// Shifter represents a module that can time-shift the icecast
// bitstream.  When called for an offset, it should return a chan that
// will receive bytes to send to the user
type Shifter interface {
	StreamFrom(offset time.Duration) (data chan []byte, closer chan struct{})
	Shutdown()
}

type memShifter struct {
	dataIn      chan []byte
	store       *streamStore
	subscribers []*subscriber
	cacheFile   string
}

// memStore is the in-memory structure for a stream
type streamStore struct {
	TimeStore  []time.Time
	ChunkStore [][]byte
	CurrPos    int
	Len        int
}

// subscriber represents a consumer and their "index"
type subscriber struct {
	data    chan []byte
	currPos int
	closed  bool
}

// NewMemShifter returns a shifter for the given stream that uses memory  as
// it's storage mechanism. It will buffer for maxOffset. If cacheFile isn't
// empty, the contents of the memory store will be written here every
// cacheInterval. If this file exists and start time, it will be read in to
// memory
func NewMemShifter(data chan []byte, timePerChunk time.Duration, maxOffset time.Duration, cacheFile string, cacheInterval time.Duration) Shifter {
	storeSize := int(maxOffset / timePerChunk)
	m := &memShifter{
		dataIn:    data,
		cacheFile: cacheFile,
		store: &streamStore{
			TimeStore:  make([]time.Time, storeSize),
			ChunkStore: make([][]byte, storeSize),
			CurrPos:    0,
			Len:        storeSize,
		},
	}
	if cacheFile != "" {
		// Read it if it exists
		f, err := os.Open(cacheFile)
		if err == nil {
			log.Printf("Loading cache file from: %q", cacheFile)
			decoder := gob.NewDecoder(f)
			err = decoder.Decode(m.store)
			if err != nil {
				log.Printf("Error decoding cache file, ignoring: %q", err)
			}
		}
		f.Close()

		go func() {
			// Start the cacher
			for range time.NewTicker(cacheInterval).C {
				if err := m.writeCache(); err != nil {
					fmt.Printf("Error writing cache file: %q", err)
				}

			}
		}()
	}
	go m.start()
	return m
}

func (m *memShifter) Shutdown() {
	if err := m.writeCache(); err != nil {
		fmt.Printf("Error writing cache file: %q", err)
	}
}

func (m *memShifter) StreamFrom(offset time.Duration) (chan []byte, chan struct{}) {
	dataChan := make(chan []byte, 32)
	closeChan := make(chan struct{})

	f := false
	sf := m.store.CurrPos // Default to start time being "curr pos"
	st := time.Now().Add(-offset)
	for i := m.store.CurrPos + 1; i < m.store.Len; i++ {
		sp := i
		if sp >= m.store.Len {
			sp = sp - m.store.Len
		}
		if m.store.TimeStore[i].After(st) {
			sf = i
			f = true
			break
		}
	}
	if !f {
		for i := 0; i < m.store.CurrPos+1; i++ {
			if m.store.TimeStore[i].After(st) {
				sf = i
				break
			}
		}
	}
	log.Printf("starting at %d", sf)
	sub := &subscriber{data: dataChan, currPos: sf}
	m.subscribers = append(m.subscribers, sub)

	// Pre seed chunks to give the user a "cache"
	for i := sf - 5; i < sf; i++ {
		sp := i
		if sp < 0 {
			sp = m.store.Len + sp
		}
		if len(m.store.ChunkStore[sp]) == 0 {
			// Nothing here yet, bail out
			break
		}
		log.Printf("pre-seeding chunk from %d", sp)
		dataChan <- m.store.ChunkStore[sp]
	}

	go func() {
		select {
		case <-closeChan:
			log.Print("Closing subscriber")
			sub.closed = true
		}
	}()

	return dataChan, closeChan
}

func (m *memShifter) start() {
	log.Print("Starting offset store")

	for d := range m.dataIn {
		// Store the chunks, update the offset
		m.store.ChunkStore[m.store.CurrPos] = d
		m.store.TimeStore[m.store.CurrPos] = time.Now()
		m.store.CurrPos++
		if m.store.CurrPos >= m.store.Len {
			m.store.CurrPos = 0
		}

		// For every subscriber channel, send them their current chunk
		for i, sub := range m.subscribers {
			if sub.closed {
				m.subscribers = append(m.subscribers[:i], m.subscribers[i+1:]...)
				continue
			}
			select {
			case sub.data <- m.store.ChunkStore[sub.currPos]:
				// log.Printf("Send chunk len %d from pos %d to sub\n", len(m.store.ChunkStore[sub.currPos]), sub.currPos)
			default:
				log.Printf("Err sending data to subscribed %#v", sub)
				close(sub.data)
			}
			sub.currPos++
			if sub.currPos >= m.store.Len {
				sub.currPos = 0
			}
		}
	}
	log.Print("Offset store ending. This is an error!!!!!")
}

func (m *memShifter) writeCache() error {
	if m.cacheFile != "" {
		log.Printf("Writing stream cache to %q", m.cacheFile)
		f, err := os.Create(m.cacheFile + ".tmp")
		if err != nil {
			log.Printf("Error creating cache file: %q", err)
			return err
		}
		encoder := gob.NewEncoder(f)
		encoder.Encode(m.store)
		f.Close()
		if err := os.Rename(m.cacheFile+".tmp", m.cacheFile); err != nil {
			log.Printf("Error renaming temp cache file: %q", err)
			return err
		}
	}
	return nil
}
