// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package command provides runnable commands for the cli interface.
// Command status provides information which jobs are currently active.
package command

import (
	"bufio"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/jwaldrip/odin/cli"
	"metl"
	"os"
	"strconv"
	"strings"
	"time"
)

type Status struct{}

func (v *Status) DefineFlags(c *cli.SubCommand) {
	// empty
}

func (v *Status) Run(c cli.Command) {
	file, err := os.Open(metl.Etl.GetLockFilePath())
	if err != nil {
		log.Fatal("Unable to open lock file for reading:", err)
	}
	defer file.Close()

	fmt.Println("The following jobs are active:")
	reader := bufio.NewScanner(file)
	for reader.Scan() {
		bits := strings.Split(reader.Text(), "|")
		timestamp, err := strconv.ParseInt(bits[1], 10, 64)
		if err != nil {
			log.Fatal("Unable to convert timestamp:", err)
		}
		since := time.Unix(timestamp, 0).Format("15:04:05 (02 Jan)")
		fmt.Printf(">> %s: since %s\n", bits[0], since)
	}
}
