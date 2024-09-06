package query

import (
	"encoding/json"
	"fmt"
	"log"
)

type ColumnType int

const (
	Integer ColumnType = iota
	Boolean
	Text
)

type Operator int

const (
	OpEquals Operator = iota
	OpOr
	OpAnd
)

type CreateStatement struct {
	TableName string
	Columns   []ColumnDefinition
}
type ColumnDefinition struct {
	ColName string
	ColType ColumnType
}

type InsertStatement struct {
	// TODO: parse insert statement
	TableName string
	Columns   []string
	Values    []string
}

type DeleteStatement struct {
	TableName string
	Where     *WhereExpression
}

type UpdateStatement struct {
	TableName string
	Columns   []string
	Values    []string
	Where     *WhereExpression
}

type SelectStatement struct {
	TableName string
	Columns   []string
	Where     *WhereExpression

	// Limit can be added here
}

type WhereExpression struct {
	Expr Expr
}

type Expr interface {
	something()
}

type ExprIdentifier struct {
	Name string
}
type ExprIntegerVal struct {
	Value string
}
type ExprStringVal struct {
	Value string
}
type ExprOperation struct {
	Left     Expr
	Operator Operator
	Right    Expr
}

func (*ExprIdentifier) something() {}
func (*ExprIntegerVal) something() {}
func (*ExprStringVal) something()  {}
func (*ExprOperation) something()  {}

type Statement interface {
	GetType()
}

func (c *CreateStatement) GetType() {}
func (*SelectStatement) GetType()   {}
func (*InsertStatement) GetType()   {}
func (*UpdateStatement) GetType()   {}
func (*DeleteStatement) GetType()   {}

type Expression struct{}

type parser struct {
	l         *Lexer
	curToken  Token
	peekToken Token
	err       []string
}

func (p *parser) Error() []string {
	return p.err
}

func (p *parser) Parser() Statement {
	switch p.peekToken.Type {
	case TOKEN_LETS:
		return p.parseCreateStatement()
	case TOKEN_SELECT:
		return p.parseSelectStatement()
	case TOKEN_SLAM:
		return p.parseInsertStatement()
	case TOKEN_DELETE:
		return p.parseDeleteStatement()
	case TOKEN_LISTEN:
		return p.parseUpdateStatement()
	}
	return nil
}
func NewParser(lexer Lexer) parser {
	return parser{l: &lexer}
}

func (p *parser) Run() {

	// for !p.curTokenIs(EOF) {
	p.getNextToken()
	fmt.Printf("%+v\n", p.Parser())

	// }

}
func (p *parser) curTokenIs(sometoken TokenType) bool {
	return p.curToken.Type == sometoken
}

func (p *parser) peekTokenIs(sometoken TokenType) bool {
	return p.peekToken.Type == sometoken
}
func (p *parser) getNextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

// Create statement
// TODO: should return some intermediate representation

func (p *parser) parseCreateStatement() Statement {
	if !p.expectPeek(TOKEN_LETS) {
		return nil
	}
	if !p.expectPeek(TOKEN_BUILD) {
		return nil
	}

	if !p.expectPeek(TOKEN_THIS) {
		return nil
	}
	if !p.expectPeek(TOKEN_PLAYBOOK) {
		return nil
	}
	if !p.expectPeek(IDENT) {
		return nil
	}
	tablename := p.curToken.Literal
	table := &CreateStatement{TableName: tablename}
	if !p.expectPeek(LPAREN) {
		return nil
	}
	// cols := []ColumnDefinition{}
	for {

		if !p.expectPeek(IDENT) {
			return nil
		}
		colname := p.curToken.Literal
		p.getNextToken()
		var coltype ColumnType
		switch p.curToken.Type {
		case INT:
			coltype = Integer
		case TOKEN_BOOL:
			coltype = Boolean
		case TOKEN_TEXT:
			coltype = Text
		default:
			return nil

		}
		table.Columns = append(table.Columns, ColumnDefinition{ColName: colname, ColType: coltype})
		if p.peekToken.Type == RPAREN {
			p.getNextToken()
			break
		}

		if !p.expectPeek(COMMA) {
			return nil
		}
	}
	if !p.expectPeek(SEMICOLON) {
		return nil
	}
	return table

}

func (p *parser) peekError(t TokenType) {
	msg := fmt.Sprintf("next token should be %s but got %+v instead ", t, p.peekToken)
	p.err = append(p.err, msg)
}

func (p *parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.getNextToken()
		return true
	}

	p.peekError(t)
	return false

}

func (p *parser) parseUpdateStatement() Statement {
	if !p.expectPeek(TOKEN_LISTEN) {
		return nil
	}
	if !p.expectPeek(TOKEN_UP) {
		return nil
	}

	if !p.expectPeek(TOKEN_UPGRADE) {
		return nil
	}
	if !p.expectPeek(IDENT) {
		return nil
	}
	updateStmt := UpdateStatement{TableName: p.curToken.Literal}
	if !p.expectPeek(TOKEN_SET) {
		return nil
	}
	p.getNextToken()
	switch p.curToken.Type {
	case IDENT:
		// handle col
		colname := p.curToken.Literal
		updateStmt.Columns = append(updateStmt.Columns, colname)
	case LPAREN:
		for {
			if !p.expectPeek(IDENT) {
				return nil
			}
			colname := p.curToken.Literal
			updateStmt.Columns = append(updateStmt.Columns, colname)
			if p.peekToken.Type == RPAREN {
				break
			}
			if !p.expectPeek(COMMA) {
				return nil
			}

		}
		p.expectPeek(RPAREN)
		// handle multiple cols
	default:
		msg := fmt.Sprintf(" token should be identifier or a Left parenthesis but got %s instead ", p.curToken)
		p.err = append(p.err, msg)

	}
	if !p.expectPeek(TOKEN_WHERE) {
		return nil
	}
	p.getNextToken()
	updateStmt.Where = &WhereExpression{p.parseExprOperation()}
	if !p.expectPeek(SEMICOLON) {
		return nil
	}
	return &updateStmt

}

func (p *parser) parseDeleteStatement() Statement {
	if !p.expectPeek(TOKEN_DELETE) {
		return nil
	}
	if !p.expectPeek(TOKEN_THIS) {
		return nil
	}
	if !p.expectPeek(TOKEN_CRAP) {
		return nil
	}
	if !p.expectPeek(TOKEN_FROM) {
		return nil
	}

	if !p.expectPeek(IDENT) {
		return nil
	}
	delStmt := DeleteStatement{TableName: p.curToken.Literal}
	if !p.expectPeek(TOKEN_WHERE) {
		return nil
	}
	p.getNextToken()
	delStmt.Where = &WhereExpression{p.parseExprOperation()}
	if !p.expectPeek(SEMICOLON) {
		return nil
	}
	fmt.Printf("---\njson encoding\n")
	jsonData, err := json.Marshal(&delStmt)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(jsonData))

	return &delStmt

}

func (p *parser) parseInsertStatement() Statement {
	if !p.expectPeek(TOKEN_SLAM) {
		return nil
	}
	if !p.expectPeek(TOKEN_THIS) {
		return nil
	}
	if !p.expectPeek(TOKEN_INTO) {
		return nil
	}
	if !p.expectPeek(IDENT) {
		return nil
	}
	insertStmt := InsertStatement{TableName: p.curToken.Literal}
	p.getNextToken()
	switch p.curToken.Type {
	case IDENT:
		// handle col
		colname := p.curToken.Literal
		insertStmt.Columns = append(insertStmt.Columns, colname)
	case LPAREN:
		for {
			if !p.expectPeek(IDENT) {
				return nil
			}
			colname := p.curToken.Literal
			insertStmt.Columns = append(insertStmt.Columns, colname)
			if p.peekToken.Type == RPAREN {
				break
			}
			if !p.expectPeek(COMMA) {
				return nil
			}

		}
		p.expectPeek(RPAREN)
		// handle multiple cols
	default:
		msg := fmt.Sprintf(" token should be identifier or a Left parenthesis but got %s instead ", p.curToken)
		p.err = append(p.err, msg)

	}
	if !p.expectPeek(TOKEN_THIS) {
		return nil
	}
	if !p.expectPeek(TOKEN_CRAZY) {
		return nil
	}
	if !p.expectPeek(TOKEN_SHIT) {
		return nil
	}
	p.getNextToken()
	switch p.curToken.Type {
	case LPAREN:
		for {
			p.getNextToken()
			switch p.curToken.Type {
			case INT:
				insertStmt.Values = append(insertStmt.Values, p.curToken.Literal)
			case TOKEN_TEXT:
				insertStmt.Values = append(insertStmt.Values, p.curToken.Literal)

			}

			if p.peekToken.Type == RPAREN {
				break
			}
			if !p.expectPeek(COMMA) {
				return nil
			}

		}
		p.expectPeek(RPAREN)
		// handle multiple cols
	default:
		msg := fmt.Sprintf(" token should be identifier or a Left parenthesis but got %s instead ", p.curToken)
		p.err = append(p.err, msg)

	}
	if !p.expectPeek(SEMICOLON) {
		return nil
	}
	if len(insertStmt.Columns) != len(insertStmt.Values) {
		// FIXME: handle error here
		return nil
	}
	return &insertStmt

}
func (p *parser) parseSelectStatement() Statement {
	if !p.expectPeek(TOKEN_SELECT) {
		return nil
	}

	if !p.expectPeek(TOKEN_ME) {
		return nil
	}
	selquery := SelectStatement{}
	p.getNextToken()
	switch p.curToken.Type {
	case IDENT:
		// handle col
		colname := p.curToken.Literal
		selquery.Columns = append(selquery.Columns, colname)
	case LPAREN:
		for {
			if !p.expectPeek(IDENT) {
				return nil
			}
			colname := p.curToken.Literal
			selquery.Columns = append(selquery.Columns, colname)
			if p.peekToken.Type == RPAREN {
				break
			}
			if !p.expectPeek(COMMA) {
				return nil
			}

		}
		p.expectPeek(RPAREN)
		// handle multiple cols
	default:
		msg := fmt.Sprintf(" token should be identifier or a Left parenthesis but got %s instead ", p.curToken)
		p.err = append(p.err, msg)

	}
	if !p.expectPeek(TOKEN_FROM) {
		return nil
	}
	if !p.expectPeek(IDENT) {
		return nil
	}
	selquery.TableName = p.curToken.Literal
	p.getNextToken()
	switch p.curToken.Type {
	case SEMICOLON:
		return &selquery

	case TOKEN_WHERE:
		p.getNextToken()
		selquery.Where = &WhereExpression{p.parseExprOperation()}

	}
	return &selquery

}

func (p *parser) parseExprOperation() Expr {
	var left Expr
	switch p.curToken.Type {
	case IDENT:
		left = &ExprIdentifier{Name: p.curToken.Literal}

	case INT:
		left = &ExprIntegerVal{Value: p.curToken.Literal}
	case TOKEN_TEXT:
		left = &ExprIntegerVal{Value: p.curToken.Literal}

	}

	p.getNextToken()
	var op Operator
	switch p.curToken.Type {
	case ASSIGN:
		op = OpEquals
	default:
		p.err = append(p.err, fmt.Sprintf("got invalid operator %s", p.curToken))
	}
	p.getNextToken()
	var right Expr
	switch p.curToken.Type {

	case INT:
		right = &ExprIntegerVal{Value: p.curToken.Literal}
		//TODO: case string not yet implemented
	case TOKEN_TEXT:
		right = &ExprStringVal{Value: p.curToken.Literal}
		// TODO: case boolean
	}
	expression := ExprOperation{Left: left, Operator: op, Right: right}
	// p.getNextToken()
	if p.peekToken.Type == AND {
		p.getNextToken()
		p.getNextToken()
		right = p.parseExprOperation()
		return &ExprOperation{&expression, OpAnd, right}
	}
	return &expression

}
