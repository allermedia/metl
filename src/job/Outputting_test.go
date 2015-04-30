// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

func ExampleStdoutWrite() {

	row := make(RowProcessed)
	row["test"] = "test"

	out := &Stdout{}
	out.Write(row)

	// Output:
	// map[test:test]
}
