// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

package main

import (
	"command"
	"metl"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	etl := metl.New()

	etl.AddRunnable("run", &command.Run{}, "Run a job", "jobname")
	etl.AddRunnable("unlock", &command.Unlock{}, "Unlock a job", "jobname")
	etl.AddRunnable("add", &command.Add{}, "Schedule a new job", "jobname")
	etl.AddRunnable("status", &command.Status{}, "Display running job list")
	etl.AddRunnable("list", &command.List{}, "List available jobs")
	etl.AddRunnable("version", &command.Version{}, "Display version information")

	etl.Init()
	etl.Start()
}
