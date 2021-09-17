package main

import (
	_ "embed"
	"html/template"
	"net/http"

	"github.com/sirupsen/logrus"
)

var (
	//go:embed index.html.tmpl
	indexData string
	indexTmpl = template.Must(template.New("index.html").Parse(indexData))
)

type index struct {
	l logrus.FieldLogger

	streams []tmplStream
}

func newIndex(l logrus.FieldLogger, streams []configStream) *index {
	var ts []tmplStream
	for _, s := range streams {
		ts = append(ts, tmplStream{
			JSID: template.JS(s.ID),
			URL:  template.JSStr("/m3u8?stream=" + s.ID),
			Name: s.Name,
		})
	}

	return &index{l: l, streams: ts}
}

type tmplStream struct {
	JSID template.JS
	URL  template.JSStr
	Name string
}

func (i *index) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	data := map[string]interface{}{
		"Streams": i.streams,
	}

	if err := indexTmpl.Execute(w, data); err != nil {
		i.l.WithError(err).Error("executing template")
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}
}
