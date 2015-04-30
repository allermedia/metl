// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package command provides runnable commands for the cli interface.
// Command run does the bulk of the ETL work.
package command

import (
	"fmt"
	"github.com/BurntSushi/toml"
	log "github.com/Sirupsen/logrus"
	"github.com/jwaldrip/odin/cli"
	"job"
	"metl"
	"os"
	"path/filepath"
)

type List struct{}

func (v *List) DefineFlags(c *cli.SubCommand) {
	c.DefineBoolFlag("all", false, "Show all available jobs")
	c.AliasFlag('a', "all")
}

func (v *List) Run(c cli.Command) {
	if c.Flag("all").Get() == true {
		// read from job directory - all available jobs
		v.AllJobs()
	} else {
		// read from crontab file - all scheduled jobs
		fmt.Println("Not yet implemented - try with -a")
	}

}

func (v *List) AllJobs() {
	dir := metl.GetJobFilesDir()
	files, err := filepath.Glob(filepath.Join(dir, "*.toml"))
	if err != nil {
		log.Fatal(err)
	}

	var job job.Job

	for _, file := range files {
		if _, err := toml.DecodeFile(file, &job); err != nil {
			log.Warn(err)
			continue
		}

		fmt.Fprintf(os.Stdout, "%s (by %s) %s\n", job.Name, job.Author, job.Description)
	}
}
