package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

const metadataFetchInterval = 10 * time.Second

// for each station, get the metadata URL
// fetch it, and stick it in the DB. both raw json, and extracted
// poll regularly - look at what the website does
// don't GC this - may as well keep it forever
// upsert? i.e do we keep each entry, or roll them up
// way to get the song playing at this point in time (first where time is < now)
// web endpoint to get it. Add it to the index page via json req

type ABCNowPlaying struct {
	NextUpdated time.Time            `json:"next_updated"`
	LastUpdated time.Time            `json:"last_updated"`
	Next        *ABCNowPlayingEntity `json:"next"`
	Now         *ABCNowPlayingEntity `json:"now"`
	Prev        *ABCNowPlayingEntity `json:"prev"`

	// we capture this too, so we can archive it
	Raw json.RawMessage `json:"-"`
}

type ABCNowPlayingEntity struct {
	Entity     string    `json:"entity"`
	Arid       string    `json:"arid"`
	PlayedTime time.Time `json:"played_time"`
	ServiceID  string    `json:"service_id"`
	Recording  struct {
		Entity      string      `json:"entity"`
		Arid        string      `json:"arid"`
		Title       string      `json:"title"`
		Metadata    interface{} `json:"metadata"`
		Description interface{} `json:"description"`
		Duration    int         `json:"duration"`
		Artists     []struct {
			Entity  string        `json:"entity"`
			Arid    string        `json:"arid"`
			Name    string        `json:"name"`
			Artwork []interface{} `json:"artwork"`
			Links   []struct {
				Entity         string      `json:"entity"`
				Arid           string      `json:"arid"`
				URL            string      `json:"url"`
				IDComponent    string      `json:"id_component"`
				Title          string      `json:"title"`
				MiniSynopsis   interface{} `json:"mini_synopsis"`
				ShortSynopsis  interface{} `json:"short_synopsis"`
				MediumSynopsis interface{} `json:"medium_synopsis"`
				Type           string      `json:"type"`
				Provider       string      `json:"provider"`
				External       bool        `json:"external"`
			} `json:"links"`
			IsAustralian interface{} `json:"is_australian"`
			Type         string      `json:"type"`
			Role         interface{} `json:"role"`
		} `json:"artists"`
		Releases []struct {
			Entity  string      `json:"entity"`
			Arid    string      `json:"arid"`
			Title   string      `json:"title"`
			Format  interface{} `json:"format"`
			Artwork []struct {
				Entity         string      `json:"entity"`
				Arid           string      `json:"arid"`
				URL            string      `json:"url"`
				Type           string      `json:"type"`
				Title          interface{} `json:"title"`
				MiniSynopsis   interface{} `json:"mini_synopsis"`
				ShortSynopsis  interface{} `json:"short_synopsis"`
				MediumSynopsis interface{} `json:"medium_synopsis"`
				Width          int         `json:"width"`
				Height         int         `json:"height"`
				Sizes          []struct {
					URL         string `json:"url"`
					Width       int    `json:"width"`
					Height      int    `json:"height"`
					AspectRatio string `json:"aspect_ratio"`
				} `json:"sizes"`
			} `json:"artwork"`
			Links []struct {
				Entity         string      `json:"entity"`
				Arid           string      `json:"arid"`
				URL            string      `json:"url"`
				IDComponent    string      `json:"id_component"`
				Title          string      `json:"title"`
				MiniSynopsis   interface{} `json:"mini_synopsis"`
				ShortSynopsis  interface{} `json:"short_synopsis"`
				MediumSynopsis interface{} `json:"medium_synopsis"`
				Type           string      `json:"type"`
				Provider       string      `json:"provider"`
				External       bool        `json:"external"`
			} `json:"links"`
			Artists []struct {
				Entity  string        `json:"entity"`
				Arid    string        `json:"arid"`
				Name    string        `json:"name"`
				Artwork []interface{} `json:"artwork"`
				Links   []struct {
					Entity         string      `json:"entity"`
					Arid           string      `json:"arid"`
					URL            string      `json:"url"`
					IDComponent    string      `json:"id_component"`
					Title          string      `json:"title"`
					MiniSynopsis   interface{} `json:"mini_synopsis"`
					ShortSynopsis  interface{} `json:"short_synopsis"`
					MediumSynopsis interface{} `json:"medium_synopsis"`
					Type           string      `json:"type"`
					Provider       string      `json:"provider"`
					External       bool        `json:"external"`
				} `json:"links"`
				IsAustralian interface{} `json:"is_australian"`
				Type         string      `json:"type"`
				Role         interface{} `json:"role"`
			} `json:"artists"`
			RecordLabel    interface{} `json:"record_label"`
			ReleaseYear    string      `json:"release_year"`
			ReleaseAlbumID interface{} `json:"release_album_id"`
		} `json:"releases"`
		Artwork []interface{} `json:"artwork"`
		Links   []struct {
			Entity         string      `json:"entity"`
			Arid           string      `json:"arid"`
			URL            string      `json:"url"`
			IDComponent    string      `json:"id_component"`
			Title          string      `json:"title"`
			MiniSynopsis   interface{} `json:"mini_synopsis"`
			ShortSynopsis  interface{} `json:"short_synopsis"`
			MediumSynopsis interface{} `json:"medium_synopsis"`
			Type           string      `json:"type"`
			Provider       string      `json:"provider"`
			External       bool        `json:"external"`
		} `json:"links"`
	} `json:"recording"`
	Release struct {
		Entity  string      `json:"entity"`
		Arid    string      `json:"arid"`
		Title   string      `json:"title"`
		Format  interface{} `json:"format"`
		Artwork []struct {
			Entity         string      `json:"entity"`
			Arid           string      `json:"arid"`
			URL            string      `json:"url"`
			Type           string      `json:"type"`
			Title          interface{} `json:"title"`
			MiniSynopsis   interface{} `json:"mini_synopsis"`
			ShortSynopsis  interface{} `json:"short_synopsis"`
			MediumSynopsis interface{} `json:"medium_synopsis"`
			Width          int         `json:"width"`
			Height         int         `json:"height"`
			Sizes          []struct {
				URL         string `json:"url"`
				Width       int    `json:"width"`
				Height      int    `json:"height"`
				AspectRatio string `json:"aspect_ratio"`
			} `json:"sizes"`
		} `json:"artwork"`
		Links []struct {
			Entity         string      `json:"entity"`
			Arid           string      `json:"arid"`
			URL            string      `json:"url"`
			IDComponent    string      `json:"id_component"`
			Title          string      `json:"title"`
			MiniSynopsis   interface{} `json:"mini_synopsis"`
			ShortSynopsis  interface{} `json:"short_synopsis"`
			MediumSynopsis interface{} `json:"medium_synopsis"`
			Type           string      `json:"type"`
			Provider       string      `json:"provider"`
			External       bool        `json:"external"`
		} `json:"links"`
		Artists []struct {
			Entity  string        `json:"entity"`
			Arid    string        `json:"arid"`
			Name    string        `json:"name"`
			Artwork []interface{} `json:"artwork"`
			Links   []struct {
				Entity         string      `json:"entity"`
				Arid           string      `json:"arid"`
				URL            string      `json:"url"`
				IDComponent    string      `json:"id_component"`
				Title          string      `json:"title"`
				MiniSynopsis   interface{} `json:"mini_synopsis"`
				ShortSynopsis  interface{} `json:"short_synopsis"`
				MediumSynopsis interface{} `json:"medium_synopsis"`
				Type           string      `json:"type"`
				Provider       string      `json:"provider"`
				External       bool        `json:"external"`
			} `json:"links"`
			IsAustralian interface{} `json:"is_australian"`
			Type         string      `json:"type"`
			Role         interface{} `json:"role"`
		} `json:"artists"`
		RecordLabel    interface{} `json:"record_label"`
		ReleaseYear    string      `json:"release_year"`
		ReleaseAlbumID interface{} `json:"release_album_id"`
	} `json:"release"`
}

type nowPlayingFetcher struct {
	l        logrus.FieldLogger
	db       *sql.DB
	url      *url.URL
	streamID string
	hc       *http.Client
	stopC    chan struct{}
}

func newNowPlayingFetcher(l logrus.FieldLogger, db *sql.DB, streamID, nowPlayingURL string) (*nowPlayingFetcher, error) {
	u, err := url.Parse(nowPlayingURL)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %v", nowPlayingURL, err)
	}

	return &nowPlayingFetcher{
		l:        l,
		db:       db,
		url:      u,
		streamID: streamID,
		hc: &http.Client{
			Timeout: time.Second * 5,
		},
		stopC: make(chan struct{}),
	}, nil
}

func (n *nowPlayingFetcher) Run() error {
	n.l.Debug("Run started")

	// set the initial ticker to fire immeditely. We'll reset it once inspecting
	// the playlist we got
	ticker := time.NewTicker(1 * time.Nanosecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.l.Debugf("tick, fetching %s", n.url.String())

			np, err := n.getNowPlaying()
			if err != nil {
				nowPlayingFetchErrorCount.WithLabelValues(n.streamID).Inc()
				n.l.WithError(err).Warn("getting now playing")
				continue
			}

			if err := n.insert(context.TODO(), np); err != nil {
				nowPlayingFetchErrorCount.WithLabelValues(n.streamID).Inc()
				n.l.WithError(err).Warn("inserting now playing into DB")
				continue
			}

			// TODO - is the metadata next update field useful?
			ticker.Reset(metadataFetchInterval)
		case <-n.stopC:
			return nil
		}
	}
}

func (n *nowPlayingFetcher) Interrupt(_ error) {
	n.stopC <- struct{}{}
}

func (n *nowPlayingFetcher) getNowPlaying() (*ABCNowPlaying, error) {
	r, err := n.hc.Get(n.url.String())
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wanted 200 from %s, got: %d", n.url.String(), r.StatusCode)
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response from %s: %v", n.url.String(), err)
	}

	np := ABCNowPlaying{}

	if err := json.Unmarshal(b, &np); err != nil {
		return nil, fmt.Errorf("parsing response json from %s: %v", n.url.String(), err)
	}

	np.Raw = b

	return &np, nil
}

func (n *nowPlayingFetcher) insert(ctx context.Context, np *ABCNowPlaying) error {
	// for now we're only capturing raw data, insert that and ignore the rest.
	if _, err := n.db.ExecContext(ctx, `insert into now_playing_raw (stream_id, data, fetched_at) values ($1, $2, $3)`,
		n.streamID, np.Raw, time.Now()); err != nil {
		return fmt.Errorf("inserting data: %v", err)
	}
	return nil
}
