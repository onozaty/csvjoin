package main

import (
	"encoding/csv"
	"fmt"
	"io"
)

var (
	version = "dev"
	commit  = "none"
)

type CsvTable interface {
	find(key string) map[string]string
	primaryColumn() string
	columnNames() []string
}

type MemoryTable struct {
	PrimaryColumn string
	ColumnNames   []string
	Rows          map[string][]string
}

func main() {
}

func (t *MemoryTable) find(key string) map[string]string {

	row := t.Rows[key]

	if row == nil {
		return nil
	}

	rowMap := make(map[string]string)
	for i := 0; i < len(t.ColumnNames); i++ {
		rowMap[t.ColumnNames[i]] = row[i]
	}

	return rowMap
}

func (t *MemoryTable) primaryColumn() string {

	return t.PrimaryColumn
}

func (t *MemoryTable) columnNames() []string {

	return t.ColumnNames
}

func loadCsvTable(reader *csv.Reader, primaryColumn string) (CsvTable, error) {

	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	primaryColumnIndex := indexOf(headers, primaryColumn)
	if primaryColumnIndex == -1 {
		return nil, fmt.Errorf("%s not found", primaryColumn)
	}

	rows := make(map[string][]string)
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		rows[row[primaryColumnIndex]] = row
	}

	return &MemoryTable{
		PrimaryColumn: primaryColumn,
		ColumnNames:   headers,
		Rows:          rows,
	}, nil
}

func indexOf(l []string, s string) int {

	for i, v := range l {
		if v == s {
			return i
		}
	}
	return -1
}
