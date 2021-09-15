package main

import "testing"

func TestConfig(t *testing.T) {
	if _, err := loadAndValdiateConfig("config.yaml"); err != nil {
		t.Fatal(err)
	}
}
