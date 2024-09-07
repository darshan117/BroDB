package query

import (
	"blackdb/btree"
	Init "blackdb/init"
	"blackdb/pager"
	"encoding/binary"
	"fmt"
	"log"
	"strconv"
)

// TODO: struct for the schema columns
type Column struct {
	Name    string
	Coltype any
}

type ColumnValues map[string]Value
type Schema struct {
	RowId     int
	TableName string
	AllCols   []ColumnDefinition
	Columns   ColumnValues
}

// supported type for the column are
// INT 4 byte
// TEXT 1 byte for length variable length contetn of the text
// BOOLEAN 1 byte
type ColType int

var schema Schema

const (
	ColInt ColType = iota
	ColText
	ColBool
)

// TODO: function for the records initialization
func MakeSchema(createstmt *CreateStatement) *Schema {
	// TODO: read the schema page
	schema.TableName = createstmt.TableName
	schema.AllCols = createstmt.Columns
	schema.AllCols = append([]ColumnDefinition{{ColName: "ROWID", ColType: Integer}}, schema.AllCols...)
	schema.Columns = make(map[string]Value)
	schema.Columns["ROWID"] = Value{val: "0", valtype: Integer}
	for _, v := range createstmt.Columns {
		schema.Columns[v.ColName] = Value{val: "", valtype: v.ColType}
	}

	return &schema
}

func RunQuery(q Query) {
	switch s := q.statements.(type) {
	case *CreateStatement:
		s.AddSchema()
		fmt.Println("schema added at ", Init.SCHEMA_TABLE)
	case *InsertStatement:
		err := s.InsertRecord(schema)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("schema added at ", Init.SCHEMA_TABLE)
	case *SelectStatement:
		s.EvalSelect()

	}
}

func (c *CreateStatement) StringSchema() string {
	start := "LET'S BUILD THIS PLAYBOOK "
	start += c.TableName
	start += "("
	for _, v := range c.Columns {
		start += " "
		start += v.ColName
		start += " "

		switch v.ColType {
		case Integer:
			start += INT
		case Text:
			start += TOKEN_TEXT

		}
		if v.ColName != c.Columns[len(c.Columns)-1].ColName {

			start += ","
		}

	}
	start += ")"
	start += ";"
	return start
}

func (createstmt *CreateStatement) AddSchema() {
	if schema.TableName == createstmt.TableName {
		log.Fatalf("schema already exists for this table %s ", createstmt.TableName)
		return
	}
	// FIXME: not correct logic
	if Init.SCHEMA_TABLE == 0 || schema.TableName != createstmt.TableName {
		page, err := pager.MakePage(pager.SCHEMA_PAGE, uint16(Init.TOTAL_PAGES))
		if err != nil {
			log.Fatal(err)
		}
		Init.UpdateSchemaTable(uint(page.PageId))
		page.AddCell([]byte(createstmt.StringSchema()))
	}

	MakeSchema(createstmt)
	// TODO: write to the file

}

func SchemaInitialization() {
	// check for the schema page == zero
	// if zero make new schema page
	// add the schema to it

}
func (is *InsertStatement) InsertRecord(sch Schema) error {
	// check  for the cols names and their corresponding
	record := sch.Columns
	schema.RowId++
	if _, ok := record["ROWID"]; ok {
		// check val type with schema
		record["ROWID"] = Value{Integer, strconv.Itoa(schema.RowId)}
	}
	for i := 0; i < len(is.Columns); i++ {
		if _, ok := sch.Columns[is.Columns[i]]; !ok {
			// check val type with schema
			return fmt.Errorf("InsertRecord: given column name does not exist. %s ", is.Columns[i])
		}
		val := sch.Columns[is.Columns[i]]
		if is.Values[i].valtype != val.valtype {
			return fmt.Errorf("InsertRecord: got different value type for the col want = %q got %q", is.Values[i].valtype, val.valtype)
		}
		record[is.Columns[i]] = Value{val: is.Values[i].val, valtype: is.Values[i].valtype}
	}
	// get the page
	// fmt.Println(record)
	serRecord := serializeRecord(record)
	// fmt.Println(serRecord)

	return AddRecord(serRecord)
}

func AddRecord(rec []byte) error {
	// check if the record page exists
	if Init.RECORD_PAGE == 0 {
		Init.RECORD_PAGE = Init.TOTAL_PAGES
		_, err := pager.MakePage(pager.RECORD_PAGE, uint16(Init.TOTAL_PAGES))
		if err != nil {
			return fmt.Errorf("AddRecord: %w ", err)
		}

	}
	page, err := pager.GetPage(uint(Init.RECORD_PAGE))
	if err != nil {
		fmt.Println(Init.RECORD_PAGE, Init.TOTAL_PAGES)
		return fmt.Errorf("AddRecord: get page %w ", err)
	}
	// fmt.Println("cell", rec)

	slot := int((page.FreeStart)-uint16(pager.PAGEHEAD_SIZE)) / 2
	if err := page.AddCell(rec); err != nil {
		if err == pager.ErrNoRoom {
			Init.RECORD_PAGE = Init.TOTAL_PAGES
			newRecPage, err := pager.MakePage(pager.RECORD_PAGE, uint16(Init.TOTAL_PAGES))
			if err != nil {
				return fmt.Errorf("AddRecord: %w", err)
			}
			slot = int((newRecPage.FreeStart)-uint16(pager.PAGEHEAD_SIZE)) / 2
			if err := newRecPage.AddCell(rec); err != nil {
				return fmt.Errorf("AddRecord: %w ", err)
			}

		} else {

			return fmt.Errorf("AddRecord: %w ", err)
		}

	}
	// insert the rowid to the btree
	buf := make([]byte, 6)
	binary.BigEndian.PutUint32(buf[0:], uint32(Init.RECORD_PAGE))
	// slot := int((page.FreeStart-2)-uint16(pager.PAGEHEAD_SIZE)) / 2

	binary.BigEndian.PutUint16(buf[4:], uint16(slot))
	fmt.Println(buf)

	// defer func() {
	// 	buf = nil
	// }()
	// _, err = btree.Insert(uint32(schema.RowId), uint32(Init.RECORD_PAGE))
	_, err = btree.Insert(uint32(schema.RowId), buf)
	if err != nil {
		return err
	}
	// for i := 0; i < int(page.NumSlots); i++ {
	// 	cell, _ := page.GetCell(uint(i))
	// 	fmt.Println(deserializeRecord(cell.CellContent))
	// 	fmt.Println(cell.CellContent)
	// 	// rpage, _ := pager.GetPage(uint(Init.ROOTPAGE))
	// 	fmt.Println(schema.RowId)

	// }
	// fmt.Println(rpage.GetKeys())
	return nil
}

// TODO: function to serialize the records
func serializeRecord(record ColumnValues) []byte {
	var result []byte
	for _, col := range schema.AllCols {
		v := record[col.ColName]
		switch v.valtype {
		case Integer:
			buf := make([]byte, 4)
			intval, _ := strconv.Atoi(v.val)
			binary.BigEndian.PutUint32(buf, uint32(intval))
			result = append(result, buf...)
		case Text:
			lengthBuffer := make([]byte, 1)
			lengthBuffer[0] = uint8(len(v.val))
			result = append(result, lengthBuffer...)
			result = append(result, []byte(v.val)...)
		case Boolean:
			buf := make([]byte, 1)
			if v.val == "1" {
				buf[0] = 1
			} else {
				buf[0] = 0

			}
			result = append(result, buf...)
		}

	}
	// var buf bytes.Buffer
	return result

}

func deserializeRecord(buf []byte) ColumnValues {

	pointer := 0
	record := make(ColumnValues, 0)
	for _, col := range schema.AllCols {
		if col.ColName == "ROWID" {
			pointer += 4
			continue
		}
		switch col.ColType {
		case Integer:
			record[col.ColName] = Value{val: strconv.Itoa(int(binary.BigEndian.Uint32(buf[pointer:]))), valtype: Integer}
			pointer += 4
		case Text:
			length := buf[pointer]
			pointer += 1
			record[col.ColName] = Value{val: string(buf[pointer : pointer+int(length)]), valtype: Integer}
			pointer += int(length)
		case Boolean:
			record[col.ColName] = Value{val: string(buf[pointer]), valtype: Integer}

		}

	}
	return record

}

func (sel *SelectStatement) SelectAll() ([]ColumnValues, error) {
	if sel.TableName != schema.TableName {
		log.Fatal("got different table name")
	}
	AllRecords, err := btree.BtreeDFSTraverseKeys()
	if err != nil {
		return nil, err
	}
	colsvals := make([]ColumnValues, 0)
	for _, v := range AllRecords {
		fmt.Println(v)
		if len(v) == 0 {
			break
		}
		colsvals = append(colsvals, deserializeRecord(v))
	}
	// fmt.Println(deserializeRecord(v))
	return colsvals, nil

}

func (sel *SelectStatement) EvalSelect() error {
	for i := 0; i < len(sel.Columns); i++ {
		if _, ok := schema.Columns[sel.Columns[i]]; !ok {
			// check val type with schema
			return fmt.Errorf("SelectRecord: given column name does not exist. %s ", sel.Columns[i])
		}
	}
	allRecs, err := sel.SelectAll()
	if err != nil {
		return err
	}
	// FIXME: can make it string easy for rendering tables
	// fmt.Println(allRecs)
	// colWithAllVals := make(map[string][]string, 0)
	// for _, v := range allRecs {
	// 	for i := 0; i < len(sel.Columns); i++ {
	// 		colWithAllVals[sel.Columns[i]] = append(colWithAllVals[sel.Columns[i]], v[sel.Columns[i]].val)

	// 	}
	// }
	// fmt.Println(colWithAllVals)
	if sel.Where != nil {
		switch s := sel.Where.Expr.(type) {
		case *ExprOperation:
			fmt.Println(s.EvalExpOp(allRecs))
		}
	}

	return nil
}

func (ex *ExprOperation) EvalExpOp(allRecs []ColumnValues) []int {
	switch ex.Operator {
	case OpEquals:
		var ident string
		var val string
		switch e := ex.Left.(type) {
		case *ExprIdentifier:
			ident = e.Name

		}

		switch e := ex.Right.(type) {
		case *ExprStringVal:
			val = e.Value
		case *ExprIntegerVal:
			val = e.Value

		}
		colWithAllVals := make([]int, 0)
		for i, v := range allRecs {

			if v[ident].val == val {
				colWithAllVals = append(colWithAllVals, i)

			}
		}
		return colWithAllVals
	case OpAnd:
		leftval := ex.Left.(*ExprOperation).EvalExpOp(allRecs)
		rightval := ex.Right.(*ExprOperation).EvalExpOp(allRecs)
		return findIntersectionUnsorted(leftval, rightval)
	}
	return nil

}

func findIntersectionUnsorted(arr1, arr2 []int) []int {
	set := make(map[int]bool)
	var result []int

	for _, num := range arr1 {
		set[num] = true
	}

	for _, num := range arr2 {
		if set[num] {
			result = append(result, num)
			delete(set, num)
		}
	}

	return result
}

// TODO: function to insert new record to the correct spot

// TODO: function to add the new record

// TODO: function to update the current record page

// TODO: function to deserialize the record
