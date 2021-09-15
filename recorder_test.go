package main

import "testing"

func TestDBOpen(t *testing.T) {
	r, err := newRecorder(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
}
