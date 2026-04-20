package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIdent_single(t *testing.T) {
	assert.Equal(t, "users", Ident("users"))
}

func TestIdent_schemaAndTable(t *testing.T) {
	assert.Equal(t, "public.users", Ident("public", "users"))
}

func TestIdent_reservedKeyword(t *testing.T) {
	assert.Equal(t, `"select"`, Ident("select"))
}

func TestIdent_uppercase(t *testing.T) {
	assert.Equal(t, `"Users"`, Ident("Users"))
}

func TestIdent_empty(t *testing.T) {
	assert.Equal(t, "", Ident(""))
}

func TestIdent_withSpecialChars(t *testing.T) {
	assert.Equal(t, `"my-table"`, Ident("my-table"))
}

func TestIdent_withDoubleQuote(t *testing.T) {
	assert.Equal(t, `"my""table"`, Ident(`my"table`))
}

func TestIdent_unreservedKeyword(t *testing.T) {
	// "name" is unreserved in PostgreSQL, should not be quoted
	assert.Equal(t, "name", Ident("name"))
}

func TestIdent_multipleTokens(t *testing.T) {
	// A string that scans to multiple tokens should be quoted
	assert.Equal(t, `"a b"`, Ident("a b"))
}

func TestQuoteLiteral(t *testing.T) {
	assert.Equal(t, "'hello'", QuoteLiteral("hello"))
}

func TestQuoteLiteral_withSingleQuote(t *testing.T) {
	assert.Equal(t, "'it''s'", QuoteLiteral("it's"))
}
