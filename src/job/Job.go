// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"metl"
	"notifications"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	jobLocked = errors.New("job is already locked")
	// Helper wrapper functions
	getLockFile, getStoragePath func() string

	// Download files to this directory
	downloadDirectory string = "downloads"
)

type Job struct {
	StartTime time.Time `toml:"-"`

	Name        string
	Description string
	Author      string
	Schedule    string

	Notifications struct {
		All      Notification
		Warnings Notification
		Fatals   Notification
	}

	Job struct {
		Fetching struct {
			File string
		}
		Parsing struct {
			Engine  string
			Options map[string]interface{}
		}
		Processing struct {
			Workers    int
			AddColumns []string
			AllowEmpty bool
			Columns    []ProcessColumn
		}
		Outputting struct {
			Engine  string
			Options map[string]interface{}
		}
	}
}

type Notification struct {
	Hipchat string
	Email   []string
}

type ProcessColumn struct {
	Name       string
	Mapping    string
	Type       string
	Transform  string
	Discard    bool
	Failure    string
	AllowEmpty bool

	Length         int
	CharacterRange []string
	Precision      int
}

type JobFile struct {
	workers    int
	addColumns []string
	Filepath   string
	Parser     Parser
	Mapping    ColumnMapper
	Output     Outputter
	Notify     []notifications.Notifier
	Stats      struct {
		Processed *Counter
		Accepted  *Counter
	}
}

type Counter struct {
	sync.RWMutex
	count uint
}

func (c *Counter) Count() {
	c.Lock()
	c.count++
	c.Unlock()
}

func (c *Counter) GetCount() uint {
	c.RLock()
	defer c.RUnlock()
	return c.count
}

func init() {
	getLockFile = func() string {
		return metl.Etl.GetLockFilePath()
	}
	getStoragePath = func() string {
		return metl.Etl.GetLocalStoragePath()
	}
}

func New(file string) *Job {
	log.WithFields(
		log.Fields{
			"pkg":  "job",
			"func": "New",
			"file": file,
		},
	).Info("Opening job file")

	jobConfig := new(Job)
	jobConfig.StartTime = time.Now()
	if _, err := toml.DecodeFile(file+".toml", jobConfig); err != nil {
		log.Fatal(err)
		return nil
	}

	// register job's signal handling preference

	return jobConfig
}

func (jf *JobFile) Run() {
	var wg sync.WaitGroup

	log.WithFields(log.Fields{
		"struct":  "JobFile",
		"func":    "Run",
		"workers": jf.workers,
	}).Info("Starting job processing")

	input := make(chan RowRaw)
	output := make(chan RowProcessed)

	wg.Add(1)
	go func() {
		err := jf.Parser.Open(jf.Filepath)
		if err != nil {
			log.Fatal(err)
		}
		defer jf.Parser.Close()

		for jf.Parser.Next() {
			input <- jf.Parser.Row()
			jf.Stats.Processed.Count()
		}
		close(input)
		wg.Done()
	}()

	for i := 0; i < jf.workers; i++ {
		wg.Add(1)
		go func() {
			addCols := len(jf.addColumns) > 0
			for r := range input {
				// @TODO - rethink this
				if addCols {
					for _, v := range jf.addColumns {
						r.AddColumn(v, "")
					}
				}
				row := r.Process(&jf.Mapping)
				if row != nil {
					output <- row
					jf.Stats.Accepted.Count()
				}
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		close(output)
	}()

	jf.Output.Open()
	for out := range output {
		jf.Output.Write(out)
	}
	jf.Output.Close()
}

func (j *Job) Fetch() (*JobFile, error) {
	// Parse out fetching client, and file location
	parts := strings.SplitN(j.Job.Fetching.File, "://", 2)

	var fetcher Fetcher
	switch parts[0] {
	case "file":
		fetcher = &FileFetcher{}
	case "http":
		fetcher = &HTTPFetcher{
			proto: parts[0] + "://",
		}
	default:
		j.Unlock()
		log.Fatalf("Fetcher %s does not exist", parts[0])
	}

	archive := filepath.Join(getStoragePath(), downloadDirectory, j.Name)
	log.WithFields(log.Fields{
		"dir": archive,
	}).Debug("Creating archive directory")
	if err := os.MkdirAll(archive, os.FileMode(0750)); err != nil {
		log.Fatalf("Error creating %s: %s", archive, err)
	}

	file, err := fetcher.Fetch(parts[1], archive)
	if err != nil {
		log.WithFields(log.Fields{
			"fetcher": fetcher,
			"from":    parts[1],
		}).Fatal("Failed fetching file: ", err)
	}

	if j.Job.Processing.AllowEmpty {
		log.Info("Allowing empty columns")
	}

	// load the processing rules
	processor := NewColumnMap()
	for _, column := range j.Job.Processing.Columns {
		column.AllowEmpty = j.Job.Processing.AllowEmpty
		processor.AddColumn(column)
	}

	var parser Parser
	switch j.Job.Parsing.Engine {
	case "csv":
		parser = &CSVParser{
			Options: j.Job.Parsing.Options,
		}
	}
	log.Infof("Loaded %s parser", parser)

	var outputter Outputter
	switch j.Job.Outputting.Engine {
	case "stdout":
		outputter = &Stdout{}
	case "mysql":
		outputter = &Mysql{
			Options: j.Job.Outputting.Options,
		}
	default:
		j.Unlock()
		log.Fatalf("Outputter %s does not exist", j.Job.Outputting.Engine)
	}

	notifiers := make([]notifications.Notifier, 0)
	if j.Notifications.All.Hipchat != "" {
		hipparts := strings.Split(j.Notifications.All.Hipchat, "@")
		notifiers = append(notifiers, &notifications.HipChat{
			Token: hipparts[0],
			Room:  hipparts[1],
		})
	}

	jf := &JobFile{
		Filepath:   file,
		workers:    j.Job.Processing.Workers,
		addColumns: j.Job.Processing.AddColumns,
		Parser:     parser,
		Mapping:    processor,
		Output:     outputter,
		Notify:     notifiers,
		Stats: struct {
			Processed *Counter
			Accepted  *Counter
		}{
			Processed: &Counter{},
			Accepted:  &Counter{},
		},
	}

	return jf, nil
}

func (j *Job) Done(jf *JobFile) {
	msg := notifications.Message{
		Jobname:   j.Name,
		Status:    "OK",
		TimeTaken: time.Since(j.StartTime),
		Rows:      jf.Stats.Processed.GetCount(),
		Accepted:  jf.Stats.Accepted.GetCount(),
		Rejected:  jf.Stats.Processed.GetCount() - jf.Stats.Accepted.GetCount(),
	}

	log.Infof("Processed %d rows: accepted %d and rejected %d in %v", msg.Rows, msg.Accepted, msg.Rejected, msg.TimeTaken)

	if len(jf.Notify) > 0 {
		var wg sync.WaitGroup
		for _, n := range jf.Notify {
			wg.Add(1)
			go func() {
				log.Infof("Notifying %s", n)
				n.Notify(msg)
				wg.Done()
			}()
		}

		wg.Wait()
	}
}

func (j *Job) Lock() {
	log.Info("Locking job")

	lockFile := getLockFile()
	file, err := os.OpenFile(lockFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		log.WithFields(
			log.Fields{
				"filename": lockFile,
			},
		).Fatal("Unable to open locking file: ", err)
	}
	defer file.Close()

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
	if err != nil {
		log.Fatal("Failed to lock file")
	}

	defer func() {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		if err != nil {
			log.Fatal("Failed to unlock file")
		}
	}()

	reader := bufio.NewScanner(file)
	for reader.Scan() {
		bits := strings.Split(reader.Text(), "|")
		if bits[0] == j.Name {
			log.WithFields(
				log.Fields{
					"jobname": j.Name,
				},
			).Fatalf("Job is already running")
		}
	}

	if _, err = file.WriteString(fmt.Sprintf("%s|%d\n", j.Name, j.StartTime.Unix())); err != nil {
		log.WithFields(
			log.Fields{
				"filename": lockFile,
			},
		).Fatal("Unable to write to locking file: ", err)
	}

}

func (j *Job) Unlock() {
	log.Info("Unlocking job")

	lockFile := getLockFile()
	file, err := os.Open(lockFile)
	if err != nil {
		log.WithFields(
			log.Fields{
				"filename": lockFile,
			},
		).Fatal("Unable to open locking file: ", err)
	}
	defer file.Close()

	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX)
	if err != nil {
		log.Fatal("Failed to lock file")
	}

	defer func() {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		if err != nil {
			log.Fatal("Failed to unlock file")
		}
	}()

	// Atomic file updating.
	tmpf, err := ioutil.TempFile(getStoragePath(), "tmp")
	if err != nil {
		log.Fatal("Unable to create temporary lock file.")
	}
	reader := bufio.NewScanner(file)
	for reader.Scan() {
		line := reader.Text()
		bits := strings.Split(line, "|")
		if bits[0] != j.Name {
			if _, err := tmpf.WriteString(line + "\n"); err != nil {
				log.Fatal("Unable to write to temporary lock file")
			}
		}
	}

	if err := os.Rename(tmpf.Name(), lockFile); err != nil {
		log.Fatal("Failed to rename temporary lock file:", err)
	}
}
