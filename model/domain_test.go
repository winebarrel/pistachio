package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func TestDomain_FQDN(t *testing.T) {
	d := &model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"}
	assert.Equal(t, "public.pos_int", d.FQDN())
}

func TestDomain_SQL_Simple(t *testing.T) {
	d := &model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"}
	assert.Equal(t, "CREATE DOMAIN public.pos_int AS integer;", d.SQL())
}

func TestDomain_SQL_WithDefault(t *testing.T) {
	def := "0"
	d := &model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", Default: &def}
	assert.Contains(t, d.SQL(), "DEFAULT 0")
}

func TestDomain_SQL_WithNotNull(t *testing.T) {
	d := &model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", NotNull: true}
	assert.Contains(t, d.SQL(), "NOT NULL")
}

func TestDomain_SQL_WithCollation(t *testing.T) {
	col := "en_US"
	d := &model.Domain{Schema: "public", Name: "name", BaseType: "text", Collation: &col}
	assert.Contains(t, d.SQL(), `COLLATE "en_US"`)
}

func TestDomain_SQL_WithConstraint(t *testing.T) {
	d := &model.Domain{
		Schema:   "public",
		Name:     "pos_int",
		BaseType: "integer",
		Constraints: []*model.DomainConstraint{
			{Name: "pos_check", Definition: "CHECK (VALUE > 0)"},
		},
	}
	sql := d.SQL()
	assert.Contains(t, sql, "CONSTRAINT pos_check CHECK (VALUE > 0)")
}

func TestDomain_CommentSQL(t *testing.T) {
	comment := "Positive integer"
	d := &model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer", Comment: &comment}
	assert.Equal(t, "COMMENT ON DOMAIN public.pos_int IS 'Positive integer';", d.CommentSQL())
}

func TestDomain_CommentSQL_NoComment(t *testing.T) {
	d := &model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"}
	assert.Equal(t, "", d.CommentSQL())
}

func TestDomainToSQL(t *testing.T) {
	comment := "Positive integer"
	d := &model.Domain{
		Schema:   "public",
		Name:     "pos_int",
		BaseType: "integer",
		Comment:  &comment,
	}
	sql := model.DomainToSQL(d)
	assert.Contains(t, sql, "-- public.pos_int")
	assert.Contains(t, sql, "CREATE DOMAIN public.pos_int AS integer;")
	assert.Contains(t, sql, "COMMENT ON DOMAIN")
}

func TestDomainsToSQL(t *testing.T) {
	domains := orderedmap.New[string, *model.Domain]()
	domains.Set("public.pos_int", &model.Domain{Schema: "public", Name: "pos_int", BaseType: "integer"})
	domains.Set("public.email", &model.Domain{Schema: "public", Name: "email", BaseType: "text"})
	sql := model.DomainsToSQL(domains)
	assert.Contains(t, sql, "public.pos_int")
	assert.Contains(t, sql, "public.email")
}
