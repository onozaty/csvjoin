package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/pkg/errors"

	flag "github.com/spf13/pflag"
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

	if len(commit) > 7 {
		commit = commit[:7]
	}

	var firstPath string
	var secondPath string
	var outputPath string
	var joinColumnName string
	var help bool

	flag.StringVarP(&firstPath, "first", "1", "", "First CSV file path")
	flag.StringVarP(&secondPath, "second", "2", "", "Second CSV file path")
	flag.StringVarP(&joinColumnName, "column", "c", "", "Name of the column to use for the join")
	flag.StringVarP(&outputPath, "output", "o", "", "Output CSV file path")
	flag.BoolVarP(&help, "help", "h", false, "Help")
	flag.Parse()
	flag.CommandLine.SortFlags = false

	if help {
		fmt.Printf("csvjoin v%s (%s)\n", version, commit)
		flag.Usage()
		os.Exit(0)
	}

	if firstPath == "" || secondPath == "" || joinColumnName == "" || outputPath == "" {
		fmt.Printf("csvjoin v%s (%s)\n", version, commit)
		flag.Usage()
		os.Exit(1)
	}

	firstFile, err := os.Open(firstPath)
	if err != nil {
		fmt.Printf("\nFailed to open file. path:%s error:%v\n", firstPath, err)
		os.Exit(1)
	}
	defer firstFile.Close()

	firstReader, err := newCsvReader(firstFile)
	if err != nil {
		fmt.Printf("\nFailed to read file. path:%s error:%v\n", firstPath, err)
		os.Exit(1)
	}

	secondFile, err := os.Open(secondPath)
	if err != nil {
		fmt.Printf("\nFailed to open file. path:%s error:%v\n", secondPath, err)
		os.Exit(1)
	}
	defer secondFile.Close()

	secondReader, err := newCsvReader(secondFile)
	if err != nil {
		fmt.Printf("\nFailed to read file. path:%s error:%v\n", secondPath, err)
		os.Exit(1)
	}

	outputFile, err := os.Create(outputPath)
	if err != nil {
		fmt.Printf("\nFailed to create file. path:%s error:%v\n", outputPath, err)
		os.Exit(1)
	}
	defer outputFile.Close()
	out := csv.NewWriter(outputFile)

	err = join(firstReader, secondReader, joinColumnName, out)

	if err != nil {
		fmt.Printf("\nError: %v\n", err)
		os.Exit(1)
	}

	out.Flush()
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

var utf8bom = []byte{0xEF, 0xBB, 0xBF}

func newCsvReader(file *os.File) (*csv.Reader, error) {

	br := bufio.NewReader(file)
	mark, err := br.Peek(len(utf8bom))
	if err != nil {
		return nil, err
	}

	if reflect.DeepEqual(mark, utf8bom) {
		// BOMがあれば読み飛ばす
		br.Discard(len(utf8bom))
	}

	return csv.NewReader(br), nil
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
