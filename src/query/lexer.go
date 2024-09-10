package query

import (
	"fmt"
	"strings"
)

type Lexer struct {
	input   string
	ch      byte
	readPos int
	pos     int
}

func isLetter(char byte) bool {
	return char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char == '_' || char == 39
}
func isNumber(char byte) bool {
	return char >= '0' && char <= '9'
}

func NewLexer(input string) Lexer {
	l := Lexer{
		input: input,
	}
	l.readChar()
	return l

}

func (l *Lexer) lexer() {
	for {
		tok := l.NextToken()
		if tok.Type == EOF {
			return
		}
		fmt.Println(tok)

	}
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos += 1
}

func (l *Lexer) peekChar() {
	panic("TODO:  implement")
}

func (l *Lexer) sanitizeWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' {
		l.readChar()
	}
}

func newToken(tokentype TokenType, char byte) Token {
	return Token{Type: tokentype, Literal: string(char)}
}
func (l *Lexer) NextToken() Token {
	var tok Token
	l.sanitizeWhitespace()
	switch l.ch {
	case '=':
		tok = newToken(ASSIGN, l.ch)
	case ';':
		tok = newToken(SEMICOLON, l.ch)

	case '(':
		tok = newToken(LPAREN, l.ch)
	case ')':
		tok = newToken(RPAREN, l.ch)
	case '{':
		tok = newToken(LBRACE, l.ch)
	case '}':
		tok = newToken(RBRACE, l.ch)
	case '+':
		tok = newToken(PLUS, l.ch)
	case '-':
		tok = newToken(MINUS, l.ch)
	case '/':
		tok = newToken(SLASH, l.ch)
	case '*':
		tok = newToken(ASTERISK, l.ch)
	case '<':
		tok = newToken(LT, l.ch)
	case '>':
		tok = newToken(GT, l.ch)
	case ',':
		tok = newToken(COMMA, l.ch)
	case 34:
		tok.Type = TOKEN_TEXT
		tok.Literal = l.readString()
	case 0:
		tok.Literal = ""
		tok.Type = EOF
	default:
		if isLetter(l.ch) {

			tok.Literal = l.readIdent()
			tok.Type = LookupIdent(strings.ToLower(tok.Literal))
			return tok
		} else if isNumber(l.ch) {
			tok.Type = INT
			// FIXME: convert to integer
			tok.Literal = l.readNumber()
			return tok
		} else {
			newToken(ILLEGAL, l.ch)
		}

	}
	l.readChar()
	return tok

}
func (l *Lexer) readIdent() string {
	pos := l.pos
	for isLetter(l.ch) {
		l.readChar()
	}
	return l.input[pos:l.pos]
}
func (l *Lexer) readstring() string {
	pos := l.pos
	for isLetter(l.ch) {
		l.readChar()
	}
	return l.input[pos:l.pos]
}
func (l *Lexer) readString() string {
	pos := l.pos + 1
	for {
		l.readChar()
		if l.ch == '"' || l.ch == 0 {
			break
		}
	}
	return l.input[pos:l.pos]
}

func (l *Lexer) readNumber() string {
	pos := l.pos
	for isNumber(l.ch) {
		l.readChar()
	}
	return l.input[pos:l.pos]
}
