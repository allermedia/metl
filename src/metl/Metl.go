// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package metl provides helper functions for the etl packages & commands
package metl

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/jwaldrip/odin/cli"
	"os"
	"path/filepath"
)

type Runnable interface {
	DefineFlags(*cli.SubCommand)
	Run(cli.Command)
}

type metl struct {
	app       *cli.CLI
	runnables map[string]Runnable

	preRunnerTasks func()

	localStorage string
	lockFile     string
}

var Etl *metl

func New() *metl {
	// signal handling -- gracefully handle ctrl+c when running a job

	Etl = &metl{
		app: cli.New("", "mini ETL system", func(c cli.Command) {
			c.Usage()
		}),
		runnables: make(map[string]Runnable),
	}

	return Etl
}

func (etl *metl) AddRunnable(name string, runner Runnable, desc string, args ...string) {
	sc := etl.app.DefineSubCommand(name, desc, etl.runner, args...)
	runner.DefineFlags(sc)
	etl.runnables[name] = runner
}

func (etl *metl) Init() {
	// All global config vars to be defined by env vars, and by flags

	// Storage of files used by the applications (downloaded files, saved config etc.)
	local_storage := os.Getenv("METL_LOCAL_STORAGE")
	if local_storage == "" {
		local_storage = filepath.Join(os.Getenv("HOME"), ".metl")
	}
	etl.app.DefineStringFlag("local-storage", local_storage, "Location for application's file storage (downloaded files, local db etc.)")
	etl.app.AliasFlag('s', "local-storage")

	etl.app.DefineStringFlag("job-files", os.Getenv("METL_JOB_FILES"), "Location of job configration files")
	etl.app.AliasFlag('f', "job-files")

	ll := os.Getenv("METL_LOG_LEVEL")
	if ll == "" {
		ll = "info"
	}
	etl.app.DefineStringFlag("log", ll, "Log level output (error, warning, info)")
	etl.app.AliasFlag('l', "log")

	etl.app.DefineSubCommand("help", "Display usage information", etl.help)

	etl.preRunnerTasks = func() {

		etl.localStorage = etl.app.Flag("local-storage").String()
		etl.setLockFile()

		level, err := log.ParseLevel(etl.app.Flag("log").String())
		if err != nil {
			level = log.InfoLevel
		}
		log.SetLevel(level)
		log.SetOutput(os.Stderr)

		createLocalStorageDirectory(etl.localStorage)
	}
}

func (etl *metl) runner(c cli.Command) {
	etl.preRunnerTasks()

	runnable := c.Name()
	if runner, ok := etl.runnables[runnable]; ok {
		runner.Run(c)
	} else {
		panic(fmt.Sprintf("Runnable %s not defined!", runnable))
	}
}

func (etl *metl) Start(args ...string) {
	etl.app.Start(args...)
}

func (etl *metl) GetLocalStoragePath() string {
	return etl.localStorage
}

func (etl *metl) GetLockFilePath() string {
	return etl.lockFile
}

func (etl *metl) setLockFile() {
	etl.lockFile = filepath.Join(etl.localStorage, "state.lock")
}

func GetJobFilesDir() string {
	return Etl.app.Flag("job-files").String()
}

func GetJobFilePath(jobName string) string {
	return filepath.Join(GetJobFilesDir(), jobName)
}

func createLocalStorageDirectory(directory string) error {
	log.WithFields(
		log.Fields{
			"pkg":  "metl",
			"func": "createLocalStorageDirectory",
			"dir":  directory,
		},
	).Debug("Checking local storage directory")
	if err := os.MkdirAll(directory, os.FileMode(0750)); err != nil {
		log.Fatalf("Error creating %s: %s", directory, err)
		return err
	}
	return nil
}

func (etl *metl) help(c cli.Command) {
	etl.app.Usage()
}
