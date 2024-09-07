package query

import (
	"bufio"
	"fmt"
	"io"
)

const PROMPT = ">> "

func Start(in io.Reader, out io.Writer) {
	scanner := bufio.NewScanner(in)
	for {
		fmt.Printf(PROMPT)
		scanned := scanner.Scan()
		if !scanned {
			return
		}
		line := scanner.Text()
		l := NewLexer(line)
		// fmt.Println(l.NextToken())
		// l.lexer()

		p := NewParser(l)
		stmt := p.Run()
		if len(p.err) != 0 {
			fmt.Println(p.err)
		}
		q := Query{statements: stmt}
		RunQuery(q)
		// fmt.Println(l)
	}
}
