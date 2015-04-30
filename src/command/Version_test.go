// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3
package command

import (
	"github.com/jwaldrip/odin/cli"
)

func ExampleVersionRun() {
	GitTag = "unknown"
	GitCommit = "unknown"
	GitBranch = "unknown"

	v := &Version{}
	var c cli.Command
	v.Run(c)

	// Output:
	// miniETL version unknown (unknown:unknown)
}
