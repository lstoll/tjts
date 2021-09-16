package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
)

func TestWeb(t *testing.T) {
	streams := []configStream{
		{
			ID:   "one",
			Name: "Stream One",
		},
		{
			ID:   "two",
			Name: "Stream Two",
		},
	}

	i := newIndex(logrus.New(), streams)

	rec := httptest.NewRecorder()

	i.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if rec.Result().StatusCode != http.StatusOK {
		t.Errorf("want ok, got: %d", rec.Result().StatusCode)
	}

	t.Logf("rec body: %s", rec.Body.String())
}
