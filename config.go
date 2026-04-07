package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	defaultMaxOffset  = 24 * time.Hour
	defaultPresignTTL = time.Hour
)

type configStream struct {
	ID           string `yaml:"id"`
	Name         string `yaml:"name"`
	URL          string `yaml:"url"`
	BaseTimezone string `yaml:"baseTimezone"`
}

// s3Config configures S3-compatible object storage (DigitalOcean Spaces, MinIO, AWS S3).
type s3Config struct {
	Endpoint     string        `yaml:"endpoint"`
	Region       string        `yaml:"region"`
	Bucket       string        `yaml:"bucket"`
	AccessKey    string        `yaml:"accessKey"`
	SecretKey    string        `yaml:"secretKey"`
	UsePathStyle bool          `yaml:"usePathStyle"`
	PresignTTL   time.Duration `yaml:"presignTTL"`
}

type configFile struct {
	S3            s3Config       `yaml:"s3"`
	MaxOffsetTime time.Duration  `yaml:"maxOffset"`
	Streams       []configStream `yaml:"streams"`
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

	if cf.S3.Bucket == "" {
		ems = append(ems, "s3.bucket must be specified")
	}
	if cf.S3.Region == "" {
		ems = append(ems, "s3.region must be specified")
	}
	if len(cf.Streams) == 0 {
		ems = append(ems, "must specify at least one stream")
	}
	for _, s := range cf.Streams {
		if s.ID == "" {
			ems = append(ems, "streams must have id")
		}
		if s.Name == "" {
			ems = append(ems, fmt.Sprintf("%s: stream must have name", s.ID))
		}
		if s.URL == "" {
			ems = append(ems, fmt.Sprintf("%s: stream must have url", s.ID))
		}
		if s.BaseTimezone == "" {
			ems = append(ems, fmt.Sprintf("%s: stream must have base timezone", s.ID))
		}
	}

	if cf.MaxOffsetTime == 0 {
		cf.MaxOffsetTime = defaultMaxOffset
	}
	if cf.S3.PresignTTL == 0 {
		cf.S3.PresignTTL = defaultPresignTTL
	}

	if len(ems) > 0 {
		return cf, fmt.Errorf("validation error(s) validating config: %s", strings.Join(ems, ", "))
	}

	return cf, nil
}
