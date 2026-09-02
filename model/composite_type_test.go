package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

func TestCompositeType_FQCN(t *testing.T) {
	ct := &model.CompositeType{Schema: "public", Name: "address"}
	assert.Equal(t, "public.address", ct.FQCN())
}

func TestCompositeType_FQCN_QuotedIdentifier(t *testing.T) {
	ct := &model.CompositeType{Schema: "public", Name: "My Type"}
	assert.Equal(t, `public."My Type"`, ct.FQCN())
}

func TestCompositeType_SQL(t *testing.T) {
	ct := &model.CompositeType{
		Schema: "public",
		Name:   "address",
		Attributes: []*model.CompositeAttribute{
			{Name: "street", TypeName: "text"},
			{Name: "city", TypeName: "text"},
		},
	}
	expected := "CREATE TYPE public.address AS (\n" +
		"    street text,\n" +
		"    city text\n" +
		");"
	assert.Equal(t, expected, ct.SQL())
}

func TestCompositeType_SQL_Collation(t *testing.T) {
	coll := `pg_catalog."C"`
	ct := &model.CompositeType{
		Schema: "public",
		Name:   "t",
		Attributes: []*model.CompositeAttribute{
			{Name: "name", TypeName: "text", Collation: &coll},
		},
	}
	expected := "CREATE TYPE public.t AS (\n" +
		`    name text COLLATE pg_catalog."C"` + "\n" +
		");"
	assert.Equal(t, expected, ct.SQL())
}

func TestCompositeAttribute_TypeSQL(t *testing.T) {
	assert.Equal(t, "text", (model.CompositeAttribute{TypeName: "text"}).TypeSQL())
	coll := `pg_catalog."C"`
	assert.Equal(t, `text COLLATE pg_catalog."C"`, (model.CompositeAttribute{TypeName: "text", Collation: &coll}).TypeSQL())
}

func TestCompositeType_CommentSQL(t *testing.T) {
	comment := "an address"
	ct := &model.CompositeType{Schema: "public", Name: "address", Comment: &comment}
	assert.Equal(t, "COMMENT ON TYPE public.address IS 'an address';", ct.CommentSQL())

	ct.Comment = nil
	assert.Empty(t, ct.CommentSQL())
}

func TestCompositeType_AttributeCommentSQLs(t *testing.T) {
	c := "the city"
	ct := &model.CompositeType{
		Schema: "public",
		Name:   "address",
		Attributes: []*model.CompositeAttribute{
			{Name: "street", TypeName: "text"},
			{Name: "city", TypeName: "text", Comment: &c},
		},
	}
	assert.Equal(t, []string{"COMMENT ON COLUMN public.address.city IS 'the city';"}, ct.AttributeCommentSQLs())
}

func TestCompositeTypeToSQL(t *testing.T) {
	comment := "an address"
	attrComment := "the city"
	ct := &model.CompositeType{
		Schema:  "public",
		Name:    "address",
		Comment: &comment,
		Attributes: []*model.CompositeAttribute{
			{Name: "street", TypeName: "text"},
			{Name: "city", TypeName: "text", Comment: &attrComment},
		},
	}
	expected := "-- public.address\n" +
		"CREATE TYPE public.address AS (\n" +
		"    street text,\n" +
		"    city text\n" +
		");\n" +
		"COMMENT ON TYPE public.address IS 'an address';\n" +
		"COMMENT ON COLUMN public.address.city IS 'the city';"
	assert.Equal(t, expected, model.CompositeTypeToSQL(ct))
}

func TestCompositeTypesToSQL(t *testing.T) {
	m := orderedmap.New[string, *model.CompositeType]()
	m.Set("public.a", &model.CompositeType{Schema: "public", Name: "a", Attributes: []*model.CompositeAttribute{{Name: "x", TypeName: "int"}}})
	m.Set("public.b", &model.CompositeType{Schema: "public", Name: "b", Attributes: []*model.CompositeAttribute{{Name: "y", TypeName: "text"}}})
	out := model.CompositeTypesToSQL(m)
	assert.Contains(t, out, "CREATE TYPE public.a AS (")
	assert.Contains(t, out, "CREATE TYPE public.b AS (")
	assert.Contains(t, out, "\n\n")
}
