package query

import (
	"blackdb/btree"
	Init "blackdb/init"
	"blackdb/pager"
	"fmt"
	"reflect"
	"testing"
)

func Initialize() {
	Init.Init()
	pager.MakePageZero(22, 1)
	err := pager.LoadPage(0)
	if err != nil {
		fmt.Println("error while loading the page")
	}
	// if Init.SCHEMA_TABLE != 0 {
	// 	schemaPage, err := pager.GetPage(uint(Init.SCHEMA_TABLE))
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	schemaCell, err := schemaPage.GetCell(0)
	// 	s
	// 	// log.Fatal(string(schemaCell.CellContent))
	// }
}

func TestCreateStatement(t *testing.T) {
	Initialize()
	line := `let's build this playbook pickupline( id  int, new text,success_rate int);`
	l := NewLexer(line)
	p := NewParser(l)
	stmt := p.Run()
	if len(p.err) != 0 {
		t.Error(p.err)
	}
	q := Query{statements: stmt}
	RunQuery(q)
	fmt.Println("primary key is ", schema.PrimaryKey)

}

func TestInsertStatement(t *testing.T) {
	// TestCreateStatement(t)
	nkeys := 3
	for i := 0; i <= nkeys; i++ {
		line := fmt.Sprintf(`slam this into pickupline (id,new) this crazy shit (%d,"hello there who  %d");`, i, i)
		l := NewLexer(line)
		p := NewParser(l)
		stmt := p.Run()
		if len(p.err) != 0 {
			t.Error(p.err)
		}
		q := Query{statements: stmt}
		RunQuery(q)

	}
	testkeys := make([]uint32, 0, nkeys)
	for i := 0; i <= nkeys; i++ {
		testkeys = append(testkeys, uint32(i))
	}
	allkeys, _ := btree.BtreeDFSTraversal()
	if !reflect.DeepEqual(allkeys, testkeys) {
		t.Errorf(
			`
		expected:%v,
		got:%v
		`, testkeys, allkeys)
	}

}

func TestSelectStatement(t *testing.T) {

	line := fmt.Sprintf(`show me all from pickupline;`)
	l := NewLexer(line)
	p := NewParser(l)
	stmt := p.Run()
	if len(p.err) != 0 {
		t.Error(p.err)
	}
	q := Query{statements: stmt}
	RunQuery(q)
}

func TestDeleteStatement(t *testing.T) {
	// t.Skip()
	// TestCreateStatement(t)
	// TestInsertStatement(t)
	line := fmt.Sprintf(`ditch this crap from pickupline where success_rate=0;`)
	l := NewLexer(line)
	p := NewParser(l)
	stmt := p.Run()
	if len(p.err) != 0 {
		t.Error(p.err)
	}
	q := Query{statements: stmt}
	RunQuery(q)
	line = fmt.Sprintf(`show me all from pickupline;`)
	l = NewLexer(line)
	p = NewParser(l)
	stmt = p.Run()
	if len(p.err) != 0 {
		t.Error(p.err)
	}
	q = Query{statements: stmt}
	RunQuery(q)
	TestSelectStatement(t)

}
