// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3
package job

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"io/ioutil"
	"notifications"
	"os"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetLevel(log.PanicLevel)

	getLockFile = func() string {
		return "/tmp/metl-tmp-lock-file"
	}
	getStoragePath = func() string {
		return "/tmp"
	}
}

type TestData struct {
	n string
	v interface{}
	e interface{}
}

func TestNewJob(t *testing.T) {
	c := New("../../test_data/test_job")

	testData := []TestData{
		{"Name", c.Name, "test job"},
		{"Description", c.Description, "test job description"},
		{"Author", c.Author, "test"},
		{"Schedule", c.Schedule, "00 22 * * *"},

		{"Notifications.All.Hipchat", c.Notifications.All.Hipchat, "token@room"},
		{"Notifications.Warnings.Email", c.Notifications.Warnings.Email[0], "email@email"},
		{"Notifications.Fatals.Email", c.Notifications.Fatals.Email[0], "email@email"},

		{"Job.Fetching.File", c.Job.Fetching.File, "file://test_data/test.csv"},
		{"Job.Parsing.Engine", c.Job.Parsing.Engine, "csv"},
		{"Job.Parsing.Options[header]", c.Job.Parsing.Options["header"], true},
		{"Job.Processing.Workers", c.Job.Processing.Workers, 3},
		{"Job.Processing.AllowEmpty", c.Job.Processing.AllowEmpty, false},
		{"Job.Processing.Columns[0].Name", c.Job.Processing.Columns[0].Name, "A"},
		{"Job.Processing.Columns[0].Mapping", c.Job.Processing.Columns[0].Mapping, "COLUMN A"},
		{"Job.Processing.Columns[0].Type", c.Job.Processing.Columns[0].Type, "int"},
		{"Job.Processing.Columns[0].Transform", c.Job.Processing.Columns[0].Transform, "{{ printf \"%03s\" . }}"},
		{"Job.Processing.Columns[0].Failure", c.Job.Processing.Columns[0].Failure, "reject"},
		{"Job.Processing.Columns[0].Discard", c.Job.Processing.Columns[0].Discard, false},
		{"Job.Processing.Columns[3].Name", c.Job.Processing.Columns[3].Name, "D"},
		{"Job.Processing.Columns[3].Discard", c.Job.Processing.Columns[3].Discard, true},

		{"Job.Outputting.Engine", c.Job.Outputting.Engine, "stdout"},
	}

	for _, d := range testData {
		if d.v != d.e {
			t.Errorf("%v: expecting %v, got %v", d.n, d.e, d.v)
		}
	}
}

func TestLock(t *testing.T) {
	c := New("../../test_data/test_job")
	c.Lock()
	defer os.Remove(getLockFile())

	f, err := ioutil.ReadFile(getLockFile())
	if err != nil {
		t.Error(err)
		return
	}

	lockinfo := string(f[:len(f)-1])
	expected := fmt.Sprintf("%s|%d", c.Name, time.Now().Unix())
	if lockinfo != expected {
		t.Errorf("Got %s, expecting %s", lockinfo, expected)
	}
}

func TestUnlock(t *testing.T) {
	c := New("../../test_data/test_job")

	if err := ioutil.WriteFile(getLockFile(), []byte(c.Name), 0600); err != nil {
		t.Error(err)
		return
	}
	defer os.Remove(getLockFile())

	c.Unlock()

	f, err := ioutil.ReadFile(getLockFile())
	if err != nil {
		t.Error(err)
		return
	}

	lockinfo := string(f)
	if lockinfo != "" {
		t.Errorf("Got '%s', expecting '%s'", lockinfo, "")
	}
}

func TestCounterCount(t *testing.T) {
	c := &Counter{}
	to := uint(374585)

	var wg sync.WaitGroup

	for i := uint(0); i < to; i++ {
		wg.Add(1)
		go func() {
			c.Count()
			wg.Done()
		}()
	}
	wg.Wait()

	if c.count != to {
		t.Errorf("Expecting %d, got %v", to, c.count)
	}
}

func TestCounterGetCount(t *testing.T) {
	to := uint(457848)
	c := &Counter{
		count: to,
	}

	if c.GetCount() != to {
		t.Errorf("Expecting %d, got %v", to, c.count)
	}
}

func TestJobDoneNotify(t *testing.T) {
	j := &Job{
		Name:      "test",
		StartTime: time.Now(),
	}

	n := &NotifyTest{c: 0}
	n1 := &NotifyTest{c: 0}

	jf := &JobFile{
		Notify: []notifications.Notifier{n, n1},
		Stats: struct {
			Processed *Counter
			Accepted  *Counter
		}{
			Processed: &Counter{},
			Accepted:  &Counter{},
		},
	}

	j.Done(jf)

	count := n.c + n1.c
	if count != 2 {
		t.Errorf("Expecting 2, got %d", count)
	}
}

type NotifyTest struct {
	c int
}

func (n *NotifyTest) Notify(m notifications.Message) {
	n.c++
}
