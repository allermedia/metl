// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3
package metl

import (
	"fmt"
	"github.com/jwaldrip/odin/cli"
	"testing"
)

var (
	jobFiles     string = "job-files-test"
	localStorage string = "local-storage"
	subCommand   cli.Command
)

func init() {
	Etl = New()
	Etl.preRunnerTasks = func() {
		Etl.localStorage = localStorage
		Etl.setLockFile()
	}
	subCommand = Etl.app.DefineSubCommand("test", "test test", func(c cli.Command) {})
}

func ExampleTestRunner() {
	Etl.AddRunnable("test", &TestCommand{}, "test test")
	Etl.runner(subCommand)

	// Output:
	// TestCommand
}

type TestCommand struct{}

func (t *TestCommand) Run(c cli.Command) {
	fmt.Println("TestCommand")
}
func (v *TestCommand) DefineFlags(c *cli.SubCommand) {}

func TestRunnerPanic(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			if e != "Runnable test not defined!" {
				t.Errorf("Expecting %s, got %s", "'Runnable test not defined!'", e)
			}
		} else {
			t.Errorf("Expecting recovery from panic")
		}
	}()
	Etl.runner(subCommand)
}

func TestGetLocalStoragePath(t *testing.T) {
	path := Etl.GetLocalStoragePath()
	if path != localStorage {
		t.Errorf("Got %v, expecting %s", path, localStorage)
	}
}

func TestGetLockFilePath(t *testing.T) {
	path := Etl.GetLockFilePath()
	expecting := localStorage + "/state.lock"
	if path != expecting {
		t.Errorf("Got %v, expecting %s", path, expecting)
	}
}

func TestGetJobFilePath(t *testing.T) {
	Etl.app.DefineStringFlag("job-files", jobFiles, "")
	Etl.Start()
	path := GetJobFilePath("test")
	expecting := jobFiles + "/test"
	if path != expecting {
		t.Errorf("Got %v, expecting %s", path, expecting)
	}
}
