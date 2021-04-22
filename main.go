package main

import (
	"encoding/csv"
	"fmt"
	"io"

	"github.com/pkg/errors"
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

func join(first *csv.Reader, second *csv.Reader, joinColumnName string, out *csv.Writer) error {

	secondTable, err := loadCsvTable(second, joinColumnName)
	if err != nil {
		return errors.Wrap(err, "Failed to read the second CSV file")
	}

	firstColumnNames, err := first.Read()
	if err != nil {
		return errors.Wrap(err, "Failed to read the first CSV file")
	}
	firstJoinColumnIndex := indexOf(firstColumnNames, joinColumnName)
	if firstJoinColumnIndex == -1 {
		return fmt.Errorf("%s is not found", joinColumnName)
	}

	// 追加するものは、結合用のカラムを除く
	appendsecondColumnNames := remove(secondTable.columnNames(), joinColumnName)
	outColumnNames := append(firstColumnNames, appendsecondColumnNames...)
	out.Write(outColumnNames)

	// 基準となるCSVを読み込みながら、結合用のカラムの値をキーとしてもう片方のCSVから値を取得
	for {
		firstRow, err := first.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "Failed to read the first CSV file")
		}

		secondRowMap := secondTable.find(firstRow[firstJoinColumnIndex])
		secondRow := make([]string, len(appendsecondColumnNames))

		for i, appendColumnName := range appendsecondColumnNames {
			if secondRowMap != nil {
				secondRow[i] = secondRowMap[appendColumnName]
			}
		}

		out.Write(append(firstRow, secondRow...))
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

		// 格納前に既にあるか確認
		// -> 重複して存在した場合はエラーに
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
