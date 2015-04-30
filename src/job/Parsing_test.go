// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3
package job

import (
	"fmt"
	"testing"
)

var (
	fileLocation = "../../test_data/test.csv"
	records      = 5
	header       = []string{"A", "B", "C", "D", "E"}
	values       = []string{"1", "a", "true", "AA", "AA"}
	processor    ColumnMapper
)

func init() {
	processor = NewColumnMap()
	processor.AddColumn(ProcessColumn{
		Name:    "int",
		Mapping: "INT",
		Type:    "int",
		Failure: "reject",
	})
	processor.AddColumn(ProcessColumn{
		Name:    "intkeep",
		Mapping: "INTKEEP",
		Type:    "int",
		Failure: "keep",
	})
	processor.AddColumn(ProcessColumn{
		Name:    "discard",
		Discard: true,
	})
	processor.AddColumn(ProcessColumn{
		Name:      "stringupper",
		Mapping:   "STRINGUPPER",
		Type:      "string",
		Transform: "{{ toUpper .}}",
		Failure:   "keep",
	})
	processor.AddColumn(ProcessColumn{
		Name:      "stringlower",
		Mapping:   "STRINGLOWER",
		Type:      "string",
		Transform: "{{ toLower .}}",
		Failure:   "keep",
	})
	processor.AddColumn(ProcessColumn{
		Name:    "bool",
		Mapping: "BOOL",
		Type:    "bool",
		Failure: "keep",
	})
	processor.AddColumn(ProcessColumn{
		Name:    "float",
		Mapping: "FLOAT",
		Type:    "float",
		Failure: "reject",
	})
	processor.AddColumn(ProcessColumn{
		Name:       "empty",
		Mapping:    "EMPTY",
		Type:       "float",
		Failure:    "reject",
		AllowEmpty: true,
	})
	processor.AddColumn(ProcessColumn{
		Name:    "length",
		Mapping: "LENGTH",
		Type:    "string",
		Failure: "reject",
		Length:  3,
	})
	processor.AddColumn(ProcessColumn{
		Name:           "cr",
		Mapping:        "CR",
		Type:           "string",
		Failure:        "reject",
		CharacterRange: []string{"A", "Z"},
	})
}

func ExampleCSVString() {
	csv := &CSVParser{}

	fmt.Println(csv)

	// Output:
	// CSV
}

func TestCSVParseHeaderRow(t *testing.T) {
	options := make(map[string]interface{})
	options["header"] = true
	csv := &CSVParser{
		Options: options,
	}

	err := csv.Open(fileLocation)
	if err != nil {
		t.Fatal(err)
	}
	defer csv.Close()

	if len(csv.headerRow) != records {
		t.Errorf("Expecting %d, got %v", records, len(csv.headerRow))
	}

	for i := 0; i < records; i++ {
		if csv.headerRow[i] != header[i] {
			t.Errorf("Expecting %s, got %v", csv.headerRow[i], header[i])
		}
	}
}

func TestCSVMapRowWithHeader(t *testing.T) {
	options := make(map[string]interface{})
	options["header"] = true
	csv := &CSVParser{
		Options: options,
	}

	err := csv.Open(fileLocation)
	if err != nil {
		t.Fatal(err)
	}
	defer csv.Close()

	v := csv.Next()
	if v == false {
		t.Error("Expecting true, got false")
	}

	for i := 0; i < records; i++ {
		if csv.next.(Row)[header[i]] != values[i] {
			t.Errorf("Expecting %s, got %v", values[i], csv.next.(Row)[header[i]])
		}
	}
}

func TestCSVMapRowNoHeader(t *testing.T) {
	options := make(map[string]interface{})
	options["header"] = false
	csv := &CSVParser{
		Options: options,
	}

	err := csv.Open(fileLocation)
	if err != nil {
		t.Fatal(err)
	}
	defer csv.Close()

	v := csv.Next()
	if v == false {
		t.Error("Expecting true, got false")
	}

	for i := 0; i < records; i++ {
		key := fmt.Sprint(i + 1)
		if csv.next.(Row)[key] != header[i] {
			t.Errorf("Expecting %s, got %v", header[i], csv.next.(Row)[key])
		}
	}
}

func TestCSVMapRowWithHeaderRowCall(t *testing.T) {
	options := make(map[string]interface{})
	options["header"] = true
	csv := &CSVParser{
		Options: options,
	}

	err := csv.Open(fileLocation)
	if err != nil {
		t.Fatal(err)
	}
	defer csv.Close()

	v := csv.Next()
	if v == false {
		t.Error("Expecting true, got false")
	}

	row := csv.Row()
	for i := 0; i < records; i++ {
		if row.(Row)[header[i]] != values[i] {
			t.Errorf("Expecting %s, got %v", values[i], row.(Row)[header[i]])
		}
	}
}

func TestCSVParseSkipRow(t *testing.T) {
	options := make(map[string]interface{})
	options["skip"] = int64(2)
	csv := &CSVParser{
		Options: options,
	}

	err := csv.Open(fileLocation)
	if err != nil {
		t.Fatal(err)
	}
	defer csv.Close()

	row, _ := csv.reader.Read()

	if row[0] != "2" {
		t.Error("Expecting 2, got %v", row[0])
	}
}

func TestProcessMappingKeyChange(t *testing.T) {
	row := make(Row)
	row["int"] = "34"

	prow := row.Process(&processor)

	if k, ok := prow["INT"]; !ok {
		t.Errorf("Expecting int, got %v", k)
	}
}

func TestProcessIntType(t *testing.T) {
	row := make(Row)
	row["int"] = "34"

	prow := row.Process(&processor)

	if prow["INT"] != "34" {
		t.Errorf("Expecting 34, got %v", prow["INT"])
	}
}

func TestProcessIntReject(t *testing.T) {
	row := make(Row)
	row["int"] = "asdf"

	prow := row.Process(&processor)

	if prow != nil {
		t.Errorf("Expecting nil, got %v", prow)
	}
}

func TestProcessIntKeep(t *testing.T) {
	row := make(Row)
	row["intkeep"] = "asdf"

	prow := row.Process(&processor)

	if prow == nil {
		t.Errorf("Expecting empty map, got %v", prow)
	}
}

func TestProcessDiscard(t *testing.T) {
	row := make(Row)
	row["discard"] = "asdf"

	prow := row.Process(&processor)

	if prow == nil {
		t.Errorf("Expecting empty map, got %v", prow)
	}
}

func TestProcessStringTransformUpper(t *testing.T) {
	row := make(Row)
	row["stringupper"] = "asdf"

	prow := row.Process(&processor)

	if prow["STRINGUPPER"] != "ASDF" {
		t.Errorf("Expecting ASDF, got %v", prow["STRINGUPPER"])
	}
}

func TestProcessStringTransformLower(t *testing.T) {
	row := make(Row)
	row["stringlower"] = "ASDF"

	prow := row.Process(&processor)

	if prow["STRINGLOWER"] != "asdf" {
		t.Errorf("Expecting asdf, got %v", prow["STRINGLOWER"])
	}
}

func TestProcessBoolType(t *testing.T) {
	row := make(Row)
	row["bool"] = "T"

	prow := row.Process(&processor)

	if prow["BOOL"] != "true" {
		t.Errorf("Expecting true, got %v", prow["BOOL"])
	}
}

func TestProcessBoolTypeInvalid(t *testing.T) {
	row := make(Row)
	row["bool"] = "A"

	prow := row.Process(&processor)

	if len(prow) > 1 {
		t.Errorf("Expecting empty Row, got %v", prow)
	}
}

func TestProcessFloatType(t *testing.T) {
	row := make(Row)
	row["float"] = "1.123"

	prow := row.Process(&processor)

	if prow["FLOAT"] != "1.123" {
		t.Errorf("Expecting 1.123, got %v", prow["FLOAT"])
	}
}

func TestProcessFloatTypeInvalid(t *testing.T) {
	row := make(Row)
	row["float"] = "1s"

	prow := row.Process(&processor)

	if prow["FLOAT"] != "" {
		t.Errorf("Expecting \"\", got %v", prow["FLOAT"])
	}
}

func TestProcessAllowEmpty(t *testing.T) {
	row := make(Row)
	row["empty"] = ""

	prow := row.Process(&processor)

	if prow["EMPTY"] != "" {
		t.Errorf("Expecting \"\", got %v", prow["EMPTY"])
	}
}

func TestProcessNotAllowEmpty(t *testing.T) {
	row := make(Row)
	row["int"] = ""

	prow := row.Process(&processor)

	if prow != nil {
		t.Errorf("Expecting nil, got %v", prow)
	}
}

func TestProcessLengthAccept(t *testing.T) {
	row := make(Row)
	row["length"] = "ABC"

	prow := row.Process(&processor)

	if prow["LENGTH"] != "ABC" {
		t.Errorf("Expecting ABC, got %v", prow["LENGTH"])
	}
}

func TestProcessLengthFail(t *testing.T) {
	row := make(Row)
	row["length"] = "ABCD"

	prow := row.Process(&processor)

	if prow != nil {
		t.Errorf("Expecting nil, got %v", prow)
	}
}

func TestProcessCharacterRangeAccept(t *testing.T) {
	row := make(Row)
	row["cr"] = "ABCZ"

	prow := row.Process(&processor)

	if prow["CR"] != "ABCZ" {
		t.Errorf("Expecting ABCZ, got %v", prow["CR"])
	}
}

func TestProcessCharacterRangeFail(t *testing.T) {
	row := make(Row)
	row["cr"] = "ABC:"

	prow := row.Process(&processor)

	if prow != nil {
		t.Errorf("Expecting nil, got %v", prow)
	}
}
