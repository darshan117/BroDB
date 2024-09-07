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
}

func TestCreateStatement(t *testing.T) {
	Initialize()
	line := `let's build this playbook hello ( id  int, new text);`
	l := NewLexer(line)
	// fmt.Println(l.NextToken())
	// l.lexer()

	p := NewParser(l)
	stmt := p.Run()
	if len(p.err) != 0 {
		t.Error(p.err)
	}
	q := Query{statements: stmt}
	RunQuery(q)

}

func TestInsertStatement(t *testing.T) {
	// TestCreateStatement(t)
	nkeys := 300
	for i := 0; i <= nkeys; i++ {
		line := fmt.Sprintf(`slam this into hello (id,new) this crazy shit (%d,"hello %d");`, i, i)
		// fmt.Println(line)
		l := NewLexer(line)
		// fmt.Println(l.NextToken())
		// l.lexer()

		p := NewParser(l)
		stmt := p.Run()
		if len(p.err) != 0 {
			t.Error(p.err)
		}
		q := Query{statements: stmt}
		RunQuery(q)

	}
	// line := fmt.Sprintf(`slam this into hello (id,new) this crazy shit (%d,"hello %d");`, 1060, 1060)
	// fmt.Println(line)
	// l := NewLexer(line)
	// fmt.Println(l.NextToken())
	// l.lexer()

	// p := NewParser(l)
	// stmt := p.Run()
	// if len(p.err) != 0 {
	// 	t.Error(p.err)
	// }
	// q := Query{statements: stmt}
	// RunQuery(q)
	testkeys := make([]uint32, 0, nkeys)
	for i := 1; i <= nkeys+1; i++ {
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
	// TestCreateStatement(t)
	// TestInsertStatement(t)

	line := fmt.Sprintf(`show me (id,new) from hello where id = 7 and new= "hello 9";`)
	// fmt.Println(line)
	l := NewLexer(line)
	// fmt.Println(l.NextToken())
	// l.lexer()

	p := NewParser(l)
	stmt := p.Run()
	if len(p.err) != 0 {
		t.Error(p.err)
	}
	q := Query{statements: stmt}
	RunQuery(q)
}
