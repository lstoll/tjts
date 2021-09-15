package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	defaultMaxOffset = 24 * time.Hour
)

// configFile maps our on-disk config to something usable.
type configFile struct {
	// ChunkDir is the root path on the filesystem where we store media chunks.
	// Chunks will be stored in a per-stream dir under this
	ChunkDir string `yaml:"chunkDir"`
	// DBPath is where the sqlite state DB will live
	DBPath string `yaml:"dbPath"`
	// MaxOffsetTime is how long in the past we will keep media around, i.e the
	// maximum time one can offset
	MaxOffsetTime time.Duration `yaml:"maxOffset"`
	// Streams is an array of stations to cache and serve
	Streams []struct {
		// ID is a unique identifier for this stream. lowercase alpha only.
		ID string `yaml:"id"`
		// Name of the stream, when displaying it
		Name string `yaml:"name"`
		// URL to the hls m3u8 for this stream
		URL string `yaml:"url"`
	} `yaml:"streams"`
}

func loadAndValdiateConfig(path string) (configFile, error) {
	fb, err := os.ReadFile(path)
	if err != nil {
		return configFile{}, fmt.Errorf("reading %s: %v", path, err)
	}

	cf := configFile{}

	if err := yaml.Unmarshal(fb, &cf); err != nil {
		return configFile{}, fmt.Errorf("unmarshaling %s: %v", path, err)
	}

	var ems []string

	if cf.ChunkDir == "" {
		ems = append(ems, "chunkDir must be specified")
	}
	if cf.DBPath == "" {
		ems = append(ems, "dbPath must be specified")
	}
	if len(cf.Streams) == 0 {
		ems = append(ems, "must specify at least one stream")
	}
	for _, s := range cf.Streams {
		if s.ID == "" {
			ems = append(ems, "streams must have id")
		}
		// TODO - id right format etc, no dupes
		if s.Name == "" {
			ems = append(ems, fmt.Sprintf("%s: stream must have name", s.ID))
		}
		if s.URL == "" {
			ems = append(ems, fmt.Sprintf("%s: stream must have url", s.ID))
		}
	}

	if cf.MaxOffsetTime == 0 {
		cf.MaxOffsetTime = defaultMaxOffset
	}

	if len(ems) > 0 {
		return cf, fmt.Errorf("validation error(s) validating config: %s", strings.Join(ems, ", "))
	}

	return cf, nil
}
