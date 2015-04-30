// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package command provides runnable commands for the cli interface.
// Command run does the bulk of the ETL work.
package command

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"
	"github.com/jwaldrip/odin/cli"
	"io"
	"io/ioutil"
	"job"
	"metl"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

var (
	crontabFile string = "crontab"
	cmd         string = "/usr/bin/env metl run"
	logDir      string = "/var/log/metl/"
)

type Add struct{}

func (v *Add) DefineFlags(c *cli.SubCommand) {
	// do nothing
}

func (v *Add) Run(c cli.Command) {
	jobName := c.Param("jobname").String()
	var job job.Job
	if _, err := toml.DecodeFile(metl.GetJobFilePath(jobName)+".toml", &job); err != nil {
		log.Fatal(err)
	}

	cronfile := filepath.Join(metl.Etl.GetLocalStoragePath(), crontabFile)

	file, err := os.Open(cronfile)
	if err != nil {
		file, err = os.Create(cronfile)
		if err != nil {
			log.WithFields(
				log.Fields{
					"filename": cronfile,
				},
			).Fatal("Unable to create cronfile: ", err)
		}
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

	buffer := make([]string, 0)
	if err = jobIsInFile(file, jobName, &buffer); err != nil {
		log.Fatal(err)
	}
	buffer = append(buffer, fmt.Sprintf("%s %s >> %s%s.log", cmd, jobName, logDir, jobName))

	tmpf, err := ioutil.TempFile(metl.Etl.GetLocalStoragePath(), "tmp")
	if err != nil {
		log.Fatal("Unable to create temporary file.")
	}

	for _, line := range buffer {
		if _, err := tmpf.WriteString(line + "\n"); err != nil {
			log.Fatal("Unable to write to temporary file")
		}
	}

	if err := os.Rename(tmpf.Name(), cronfile); err != nil {
		log.Fatal("Failed to rename temporary lock file:", err)
	}

	// execute /usr/bin/env crontab cronfile
}

func jobIsInFile(file io.Reader, jobName string, buffer *[]string) error {
	buf := *buffer
	reader := bufio.NewScanner(file)
	for reader.Scan() {
		line := reader.Text()
		if strings.Contains(line, jobName) {
			return errors.New(jobName + " is already added")
		}
		buf = append(buf, line)
	}
	buffer = &buf
	return nil
}
