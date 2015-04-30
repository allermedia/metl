// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

import (
	"bytes"
	"encoding/csv"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"text/template"
	"time"
)

var (
	templateFunctions = template.FuncMap{
		"toUpper": strings.ToUpper,
		"toLower": strings.ToLower,
		"Now": func(format string) string {
			return time.Now().Format(format)
		},
	}
)

type Parser interface {
	Open(file string) error
	Close()
	Next() bool
	Row() RowRaw
}

type RowRaw interface {
	Process(*ColumnMapper) RowProcessed
	AddColumn(key string, value string)
}

type Row map[string]string

func (r Row) AddColumn(key string, value string) {
	r[key] = value
}

func (r Row) Process(cm *ColumnMapper) RowProcessed {
	row := make(RowProcessed)
	for k, v := range r {
		m := (*cm).GetColumn(k)
		if m.Discard == true {
			continue
		}

		// Check the type of the variable received.  Everything in theory is a string and will
		// be treated as a string when output, so we use the orignal values but check the type.
		var err error
		switch m.Type {
		case "string", "variable":
			// Do nothing.
		case "int":
			_, err = strconv.ParseInt(v, 0, 0)
		case "bool":
			// Convert valid true/false values to string value true or false
			var x bool
			x, err = strconv.ParseBool(v)
			v = strconv.FormatBool(x)
		case "float":
			//var x float64
			_, err = strconv.ParseFloat(v, 64)
			//v = strconv.FormatFloat(x, 'f', m.Precision, 64)
		default:
			log.WithFields(log.Fields{
				"type": m.Type,
			}).Fatal("Received unexpected type")
			return nil
		}

		if err != nil && !m.AllowEmpty {
			log.WithFields(log.Fields{
				"status":    m.Failure,
				"column":    m.Name,
				"expecting": m.Type,
				"value":     v,
			}).Warn("Unexpected type when processing field")
			if m.Failure == "reject" {
				return nil
			}
			// Even though we have selected "keep", we still remove the invalid field.
			// @todo - add option to actually use the field as well.
			continue
		}

		// @todo - perhaps this code should execute for string types only ..?

		strRune := []rune(v)

		if m.Length != 0 && len(strRune) != m.Length {
			log.WithFields(log.Fields{
				"expected length": m.Length,
				"actual length":   len(strRune),
				"value":           v,
			}).Warn("Row length check failed")
			if m.Failure == "reject" {
				return nil
			}
		}

		if len(m.CharacterRange) > 1 {
			lo := []rune(m.CharacterRange[0])
			hi := []rune(m.CharacterRange[1])

			for _, r := range strRune {
				if !(r >= lo[0] && r <= hi[0]) {
					log.WithFields(log.Fields{
						"lo":        m.CharacterRange[0],
						"hi":        m.CharacterRange[1],
						"character": string(r),
					}).Warn("Character out of range")
					if m.Failure == "reject" {
						return nil
					}
				}
			}
		}

		// Transformation stuff
		if m.Transform != "" {
			var t *template.Template
			t, err = template.New("fields").Funcs(templateFunctions).Parse(m.Transform)
			if err != nil {
				log.Fatal(err)
			}
			var nv bytes.Buffer
			err := t.Execute(&nv, v)
			if err != nil {
				log.Fatal(err)
			}
			v = nv.String()
		}

		row[m.Mapping] = v
	}
	return row
}

type RowProcessed map[string]string

type CSVParser struct {
	Options map[string]interface{}
	file    *os.File
	reader  *csv.Reader
	next    RowRaw

	headerRow []string
}

func (p *CSVParser) Open(file string) error {
	logFields := log.Fields{
		"parser": "csv",
	}

	var err error
	p.file, err = os.Open(file)
	if err != nil {
		log.WithFields(logFields).Warn(err)
		return err
	}

	p.reader = csv.NewReader(p.file)
	p.reader.LazyQuotes = true

	if skip, ok := p.Options["skip"]; ok {
		log.WithFields(logFields).Debugf("Skipping %d rows", skip)
		for i := 0; i < int(skip.(int64)); i++ {
			p.reader.Read()
		}
	}

	if h, ok := p.Options["header"]; ok && h.(bool) == true {
		log.WithFields(logFields).Debug("Using row 1 as header input")

		p.headerRow, err = p.reader.Read()
		if err != nil {
			log.WithFields(logFields).Warn(err)
		}
	}
	return nil
}

func (p *CSVParser) Close() {
	p.file.Close()
}

func (p *CSVParser) String() string {
	return "CSV"
}

func (p *CSVParser) Next() bool {
	var rowExists bool

	next, err := p.reader.Read()

	if err != nil {
		if err.Error() != "EOF" {
			log.WithFields(log.Fields{
				"parser": "csv",
			}).Warn(err)
		}

		rowExists = false
	}

	if len(next) > 0 {
		rowExists = true

		row := make(Row)
		for i := 0; i < len(next); i++ {
			if len(p.headerRow) > 0 {
				row[p.headerRow[i]] = next[i]
			} else {
				row[fmt.Sprint(i+1)] = next[i]
			}
		}
		p.next = row
	}

	return rowExists
}

func (p *CSVParser) Row() RowRaw {
	return p.next
}
