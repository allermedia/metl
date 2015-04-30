// Copyright 2014 Aller Media AS.  All rights reserved.
// License: GPL3

// Package job provides local job information and access.
package job

import (
	log "github.com/Sirupsen/logrus"
)

type ColumnMapper interface {
	AddColumn(ProcessColumn)
	GetColumn(string) ProcessColumn
}

// @todo locking
type ColumnMap struct {
	columns map[string]ProcessColumn
}

func NewColumnMap() ColumnMapper {
	return &ColumnMap{
		columns: make(map[string]ProcessColumn),
	}
}

// @todo check if column exists already or not
func (cm *ColumnMap) AddColumn(column ProcessColumn) {
	cm.columns[column.Name] = column

	log.WithFields(log.Fields{
		"name":         column.Name,
		"mapping name": column.Mapping,
		"type":         column.Type,
		"discard":      column.Discard,
		"transform":    column.Transform,
	}).Debug("Loading column processing rules")
}

// check if column exists before returning ..
func (cm *ColumnMap) GetColumn(name string) ProcessColumn {
	if c, ok := cm.columns[name]; ok {
		return c
	}
	return ProcessColumn{}
}
