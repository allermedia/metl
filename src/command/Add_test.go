// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3
package command

import (
	"bytes"
	"testing"
)

func TestJobExistsInFile(t *testing.T) {
	file := bytes.NewBufferString("metl test_job >> log\nmetl job_other >> logother")

	buffer := make([]string, 0)
	if err := jobIsInFile(file, "test_job", &buffer); err == nil {
		t.Errorf("Expecting error as not nil, got %v", err)
	}
}
