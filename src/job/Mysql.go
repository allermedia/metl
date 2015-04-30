// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

import (
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"strings"
)

type Mysql struct {
	Options map[string]interface{}

	db   *sql.DB
	stmt *sql.Stmt

	prepared bool

	columns []string
}

func (m *Mysql) Write(row RowProcessed) {

	if !m.prepared {
		m.prepareQuery(row)
	}

	// Match data up in the order of our columns
	data := make([]interface{}, len(m.columns))
	for i, v := range m.columns {
		data[i] = row[v]
	}

	_, err := m.stmt.Exec(data...)
	if err != nil {
		log.Warn(err)
	}
}

func (m *Mysql) prepareQuery(row RowProcessed) {
	m.columns = make([]string, 0)
	vals := m.columns

	for k := range row {
		m.columns = append(m.columns, k)
		vals = append(vals, "?")
	}

	var err error
	m.stmt, err = m.db.Prepare(fmt.Sprintf("insert into %s (%s) values (%s)", m.Options["table"], strings.Join(m.columns, ","), strings.Join(vals, ",")))
	if err != nil {
		log.Fatal(err)
	}

	m.prepared = true
}

func (m *Mysql) Open() {
	parts := strings.FieldsFunc(m.Options["dsn"].(string), func(c rune) bool {
		return c == '/' || c == '@' || c == ':'
	})

	log.WithFields(log.Fields{
		"user":     parts[0],
		"host":     parts[2],
		"database": parts[3],
	}).Debug("Connecting to MySQL")

	var err error
	m.db, err = sql.Open("mysql", m.Options["dsn"].(string))
	if err != nil {
		log.Fatal(err)
	}
	log.Info("Connected to MySQL")
}

func (m *Mysql) Close() {
	if m.prepared {
		m.stmt.Close()
	}
	m.db.Close()
}
