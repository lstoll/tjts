package main

import "testing"

func TestDBOpen(t *testing.T) {
	db, err := newDB(t.TempDir() + "/db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}
