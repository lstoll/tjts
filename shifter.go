package main

import (
	"log"
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
	store       *diskBuf
	subscribers []*subscriber
	cacheFile   string
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
//
// TODO - rename
func NewMemShifter(data chan []byte, timePerChunk time.Duration, maxOffset time.Duration, cacheFile string, cacheInterval time.Duration) (Shifter, error) {
	if cacheFile == "" {
		panic("provide cacheFile now")
	}
	if timePerChunk != 2*time.Second {
		panic("hard coded the chunk time now, either make it configurable or make it 2 seconds")
	}
	// TODO - we pass a chunkbites of 16386 to the client, which works out to be
	// 65536 bitrate with 2s chunks. so consolicate what we set there with what
	// we set here, via some params or whatever
	b, err := openBuffer(cacheFile, 65536, 2*time.Second, maxOffset)
	if err != nil {
		return nil, err
	}
	m := &memShifter{
		dataIn:    data,
		cacheFile: cacheFile,
		store:     b,
	}
	go m.start()
	return m, nil
}

func (m *memShifter) Shutdown() {
	if err := m.store.Close(); err != nil {
		log.Printf("Error closing storage: %q", err)
	}
}

func (m *memShifter) StreamFrom(offset time.Duration) (chan []byte, chan struct{}) {
	log.Printf("stream requested from %s", offset)
	dataChan := make(chan []byte, 32)
	closeChan := make(chan struct{})

	t := time.Now().Add(-offset)
	c, err := m.store.CursorFrom(t)
	if err != nil {
		// TODO
		panic(err)
	}

	go func() {
		// preseed
		log.Print("preseeding")
		for i := 0; i < 5; i++ {
			b, err := c.Next()
			if err != nil {
				log.Printf("cursor next error: %v", err)
				return
			}
			dataChan <- b
		}

		log.Print("starting ticker")

		// TODO - config interval
		tr := time.NewTicker(2 * time.Second)
		for {
			select {
			case <-closeChan:
				tr.Stop()
				log.Print("closing")
				return
			case tk := <-tr.C:
				b, err := c.Next()
				if err != nil {
					log.Printf("cursor next error: %v", err)
					return
				}
				log.Printf("send data tick %s", tk)
				dataChan <- b
			}
		}
	}()

	return dataChan, closeChan
}

func (m *memShifter) start() {
	log.Print("Starting offset store")

	for d := range m.dataIn {
		if err := m.store.WriteChunk(time.Now(), d); err != nil {
			log.Fatalf("writing chunk: %v", err)
		}
	}
}
