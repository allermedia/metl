// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package command provides runnable commands for the cli interface.
// Command run does the bulk of the ETL work.
package command

import (
	log "github.com/Sirupsen/logrus"
	"github.com/jwaldrip/odin/cli"
	"job"
	"metl"
)

// create relevant interfaces for all operations
// fetching, read, output etc.  (anythingn in go that we can already reuse?)

type Run struct{}

func (v *Run) DefineFlags(c *cli.SubCommand) {
	// empty
}

func (v *Run) Run(c cli.Command) {

	jobName := c.Param("jobname").String()
	log.WithFields(
		log.Fields{
			"pkg":  "command",
			"func": "Run",
			"job":  jobName,
		},
	).Info("Starting job")

	j := job.New(metl.GetJobFilePath(jobName))

	j.Lock()

	jf, err := j.Fetch()
	if err != nil {
		log.Fatal("Error fetching job:", err)
	}

	jf.Run()

	j.Unlock()
	j.Done(jf)

	// log stats, notify of errors etc
	// notify of any errors

	// log stats to dashboard, number of processed, rejected rows etc.

	// if parsing option is empty then, we just download the file
}
