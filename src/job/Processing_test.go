// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

import (
	"testing"
)

func TestMapAddGetColumn(t *testing.T) {
	processor := NewColumnMap()
	processor.AddColumn(ProcessColumn{
		Name: "test",
	})

	col := processor.GetColumn("test")
	if col.Name != "test" {
		t.Errorf("Expecting ProcessColumn, got %v", col)
	}
}
