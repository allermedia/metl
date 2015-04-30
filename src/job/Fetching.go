// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

import (
	log "github.com/Sirupsen/logrus"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
)

type Fetcher interface {
	Fetch(from string, toDirectory string) (string, error)
}

type FileFetcher struct{}

func (ff *FileFetcher) Fetch(from string, to string) (string, error) {
	logFields := log.Fields{
		"pkg":     "job",
		"func":    "FileFetch",
		"from":    from,
		"to":      to,
		"jobname": filepath.Base(to),
	}

	log.WithFields(logFields).Info("Fetching input file")

	s, err := os.Open(from)
	if err != nil {
		return "", err
	}
	defer s.Close()

	dest := filepath.Join(to, filepath.Base(from))
	if err := fileCopy(dest, s); err != nil {
		return "", err
	}

	log.WithFields(logFields).Debug("Copied file")

	return dest, nil
}

func (ff *FileFetcher) String() string {
	return "FileCopy"
}

// Atomic file copying
func fileCopy(dest string, source io.Reader) error {

	destDir := filepath.Dir(dest)
	tmpf, err := ioutil.TempFile(destDir, "tmp")
	if err != nil {
		return err
	}
	defer tmpf.Close()

	if _, err := io.Copy(tmpf, source); err != nil {
		os.Remove(tmpf.Name())
		return err
	}

	if err := os.Rename(tmpf.Name(), dest); err != nil {
		return err
	}

	return nil
}
