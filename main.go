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
	joinColumnName() string
	columnNames() []string
}

type MemoryTable struct {
	JoinColumnName string
	ColumnNames    []string
	Rows           map[string][]string
}

func main() {
}

func join(left *csv.Reader, right *csv.Reader, joinColumnName string, result *csv.Writer) error {

	rightTable, err := loadCsvTable(right, joinColumnName)
	if err != nil {
		return err
	}

	leftColumnNames, err := left.Read()
	if err != nil {
		return err
	}
	leftJoinColumnIndex := indexOf(leftColumnNames, joinColumnName)
	if leftJoinColumnIndex == -1 {
		return fmt.Errorf("%s is not found", joinColumnName)
	}

	// 追加するものは、結合用のカラムを除く
	appendRightColumnNames := remove(rightTable.columnNames(), joinColumnName)
	resultColumnNames := append(leftColumnNames, appendRightColumnNames...)
	result.Write(resultColumnNames)

	// 基準となるCSVを読み込みながら、結合用のカラムの値をキーとして片方のCSVから値を取得
	for {
		leftRow, err := left.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		rightRowMap := rightTable.find(leftRow[leftJoinColumnIndex])
		rightRow := make([]string, len(appendRightColumnNames))

		for i, appendColumnName := range appendRightColumnNames {
			if rightRowMap != nil {
				rightRow[i] = rightRowMap[appendColumnName]
			}
		}

		result.Write(append(leftRow, rightRow...))
	}

	return nil
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

func (t *MemoryTable) joinColumnName() string {

	return t.JoinColumnName
}

func (t *MemoryTable) columnNames() []string {

	return t.ColumnNames
}

func loadCsvTable(reader *csv.Reader, joinColumnName string) (CsvTable, error) {

	headers, err := reader.Read()
	if err != nil {
		return nil, err
	}

	primaryColumnIndex := indexOf(headers, joinColumnName)
	if primaryColumnIndex == -1 {
		return nil, fmt.Errorf("%s is not found", joinColumnName)
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

		_, has := rows[row[primaryColumnIndex]]
		if has {
			return nil, fmt.Errorf("%s is duplicated", row[primaryColumnIndex])
		}

		rows[row[primaryColumnIndex]] = row
	}

	return &MemoryTable{
		JoinColumnName: joinColumnName,
		ColumnNames:    headers,
		Rows:           rows,
	}, nil
}

func indexOf(strings []string, search string) int {

	for i, v := range strings {
		if v == search {
			return i
		}
	}
	return -1
}

func remove(strings []string, search string) []string {

	result := []string{}
	for _, v := range strings {
		if v != search {
			result = append(result, v)
		}
	}
	return result
}
