// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package command provides runnable commands for the cli interface.
// Command unlock provides unlocking options for hanging jobs.
package command

import (
	"github.com/jwaldrip/odin/cli"
	"job"
	"metl"
)

type Unlock struct{}

func (v *Unlock) DefineFlags(c *cli.SubCommand) {
	// empty
}

func (v *Unlock) Run(c cli.Command) {
	jobName := c.Param("jobname").String()
	job.New(metl.GetJobFilePath(jobName)).Unlock()
}
