package query

type TokenType string
type Token struct {
	Type    TokenType
	Literal string
}

const (
	// column type
	TOKEN_INTEGER = "INT"
	TOKEN_TEXT    = "TEXT"
	TOKEN_BOOL    = "BOOL"

	// create
	TOKEN_BRO      = "BRO"
	TOKEN_LETS     = "LET'S"
	TOKEN_BUILD    = "BUILD"
	TOKEN_THIS     = "THIS"
	TOKEN_PLAYBOOK = "PLAYBOOK"

	// read
	TOKEN_SELECT = "SHOW"
	TOKEN_ME     = "ME"
	TOKEN_FROM   = "FROM"

	// insert
	TOKEN_INSERT = "BRO-SERT"

	// update
	TOKEN_LISTEN  = "LISTEN"
	TOKEN_UP      = "UP"
	TOKEN_UPGRADE = "UPGRADE"
	TOKEN_SET     = "SET"

	// where
	TOKEN_WHERE = "WHERE"

	//delete
	TOKEN_DELETE = "DITCH"

	// operation
	ILLEGAL = "ILLEGAL"
	EOF     = "EOF"
	IDENT   = "IDENT"
	INT     = "INT"
	AND     = "AND"
	OR      = "OR"

	// Operators
	ASSIGN   = "="
	PLUS     = "+"
	MINUS    = "-"
	BANG     = "!"
	ASTERISK = "*"
	SLASH    = "/"
	LT       = "<"
	GT       = ">"
	EQ       = "=="
	NOT_EQ   = "!="

	COMMA     = ","
	SEMICOLON = ";"

	LPAREN = "("
	RPAREN = ")"
	LBRACE = "{"
	RBRACE = "}"
)

var IdentTable = map[string]TokenType{
	"bro":      TOKEN_BRO,
	"up":       TOKEN_UP,
	"bro-sert": TOKEN_INSERT,
	"build":    TOKEN_BUILD,
	"ditch":    TOKEN_DELETE,
	"from":     TOKEN_FROM,
	"let's":    TOKEN_LETS,
	"listen":   TOKEN_LISTEN,
	"me":       TOKEN_ME,
	"show":     TOKEN_SELECT,
	"set":      TOKEN_SET,
	"this":     TOKEN_THIS,
	"playbook": TOKEN_PLAYBOOK,
	"where":    TOKEN_WHERE,
	"upgrade":  TOKEN_UPGRADE,
	"int":      TOKEN_INTEGER,
	"bool":     TOKEN_BOOL,
	"text":     TOKEN_TEXT,
	"and":      AND,
	"or":       OR,
}

func LookupIdent(ident string) TokenType {
	if tok, ok := IdentTable[ident]; ok {
		return tok
	}
	return IDENT
}
