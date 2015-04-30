// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

import (
	"fmt"
	"os"
)

type Outputter interface {
	Write(RowProcessed)
	Open()
	Close()
}

type Stdout struct{}

func (s *Stdout) Write(row RowProcessed) {
	fmt.Fprintln(os.Stdout, row)
}

func (s *Stdout) Open()  {}
func (s *Stdout) Close() {}
