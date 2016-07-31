package tjts

import (
	"log"
	"time"
)

// Shifter represents a module that can time-shift the icecast
// bitstream.  When called for an offset, it should return a chan that
// will receive bytes to send to the user
type Shifter interface {
	StreamFrom(offset time.Duration) (data chan []byte, closer chan struct{})
}

type memShifter struct {
	dataIn      chan []byte
	store       *streamStore
	subscribers []*subscriber
}

// memStore is the in-memory structure for a stream
type streamStore struct {
	timeStore  []time.Time
	chunkStore [][]byte
	currPos    int
	len        int
}

// subscriber represents a consumer and their "index"
type subscriber struct {
	data    chan []byte
	currPos int
	closed  bool
}

// NewMemShifter returns a shifter for the given stream that uses memory  as
// it's storage mechanism. It will buffer for maxOffset
func NewMemShifter(data chan []byte, timePerChunk time.Duration, maxOffset time.Duration) Shifter {
	storeSize := int(maxOffset / timePerChunk)
	s := &memShifter{
		dataIn: data,
		store: &streamStore{
			timeStore:  make([]time.Time, storeSize),
			chunkStore: make([][]byte, storeSize),
			currPos:    0,
			len:        storeSize,
		},
	}
	go s.start()
	return s
}

func (m *memShifter) StreamFrom(offset time.Duration) (chan []byte, chan struct{}) {
	dataChan := make(chan []byte)
	closeChan := make(chan struct{})

	f := false
	sf := m.store.currPos // Default to start time being "curr pos"
	st := time.Now().Add(-offset)
	for i := m.store.currPos + 1; i < m.store.len; i++ {
		if m.store.timeStore[i].After(st) {
			sf = i
			f = true
			break
		}
	}
	if !f {
		for i := 0; i < m.store.currPos+1; i++ {
			if m.store.timeStore[i].After(st) {
				sf = i
				break
			}
		}
	}
	log.Printf("starting at %d", sf)
	sub := &subscriber{data: dataChan, currPos: sf}
	m.subscribers = append(m.subscribers, sub)

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
		m.store.chunkStore[m.store.currPos] = d
		m.store.timeStore[m.store.currPos] = time.Now()
		m.store.currPos++
		if m.store.currPos >= m.store.len {
			m.store.currPos = 0
		}

		// For every subscriber channel, send them their current chunk
		for i, sub := range m.subscribers {
			if sub.closed {
				m.subscribers = append(m.subscribers[:i], m.subscribers[i+1:]...)
				continue
			}
			select {
			case sub.data <- m.store.chunkStore[sub.currPos]:
			default:
				log.Printf("Err sending data to subscribed %#v", sub)
				close(sub.data)
			}
			sub.currPos++
			if sub.currPos >= m.store.len {
				sub.currPos = 0
			}
		}
	}
	log.Print("Offset store ending. This is an error!!!!!")
}
