package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfig(t *testing.T) {
	if _, err := loadAndValdiateConfig("config.yaml"); err != nil {
		t.Fatal(err)
	}
}

func TestConfigRejectsInvalidSettings(t *testing.T) {
	baseline, err := os.ReadFile("config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{
		string(baseline) + "\nchunkDir: /old/cache\n",
		strings.Replace(string(baseline), "id: doublej", "id: ../escape", 1),
		strings.Replace(string(baseline), "Australia/Sydney", "NotATimezone", 1),
		string(baseline) + "\nmaxOffset: -1h\n",
		strings.Replace(string(baseline), "presignTTL: 1h", "presignTTL: 169h", 1),
		strings.Replace(string(baseline), "https://mediaserviceslive", "file://mediaserviceslive", 1),
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadAndValdiateConfig(path); err == nil {
			t.Fatalf("accepted invalid config: %s", text)
		}
	}
}
