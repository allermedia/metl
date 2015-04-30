// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package command provides runnable commands for the cli interface.
// Command version provides version information about the application
package command

import (
	"fmt"
	"github.com/jwaldrip/odin/cli"
	"os"
)

const (
	NAME = "miniETL"
)

var (
	// Populated when building
	GitTag    string
	GitCommit string
	GitBranch string
)

type Version struct{}

func (v *Version) DefineFlags(c *cli.SubCommand) {
	// empty
}

func (v *Version) Run(c cli.Command) {
	fmt.Fprintf(os.Stdout, "%s version %s (%s:%s)\n", NAME, GitTag, GitBranch, GitCommit)
}
