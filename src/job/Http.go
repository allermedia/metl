// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

import (
	log "github.com/Sirupsen/logrus"
	"net/http"
	"path/filepath"
)

type HTTPFetcher struct {
	proto string
}

func (ff *HTTPFetcher) String() string {
	return "HTTP"
}

func (hf *HTTPFetcher) Fetch(from string, to string) (string, error) {
	logFields := log.Fields{
		"pkg":     "job",
		"func":    "HTTPFetch",
		"from":    from,
		"to":      to,
		"jobname": filepath.Base(to),
	}

	log.WithFields(logFields).Info("Fetching input file")

	resp, err := http.Get(hf.proto + from)
	if err != nil {
		return "", err
	}

	dest := filepath.Join(to, filepath.Base(from))
	if err := fileCopy(dest, resp.Body); err != nil {
		return "", err
	}

	log.Debugf("Downloaded file to %s", dest)

	return dest, nil
}
