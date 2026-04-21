package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func TestEnum_FQEN(t *testing.T) {
	e := &model.Enum{Schema: "public", Name: "status"}
	assert.Equal(t, "public.status", e.FQEN())
}

func TestEnum_FQEN_QuotedIdentifier(t *testing.T) {
	e := &model.Enum{Schema: "public", Name: "My Type"}
	assert.Equal(t, `public."My Type"`, e.FQEN())
}

func TestEnum_SQL(t *testing.T) {
	e := &model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive", "pending"},
	}
	expected := "CREATE TYPE public.status AS ENUM (\n" +
		"    'active',\n" +
		"    'inactive',\n" +
		"    'pending'\n" +
		");"
	assert.Equal(t, expected, e.SQL())
}

func TestEnum_CommentSQL(t *testing.T) {
	comment := "User status"
	e := &model.Enum{
		Schema:  "public",
		Name:    "status",
		Values:  []string{"active"},
		Comment: &comment,
	}
	assert.Equal(t, "COMMENT ON TYPE public.status IS 'User status';", e.CommentSQL())
}

func TestEnum_CommentSQL_NoComment(t *testing.T) {
	e := &model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active"},
	}
	assert.Equal(t, "", e.CommentSQL())
}

func TestEnumToSQL(t *testing.T) {
	comment := "User status"
	e := &model.Enum{
		Schema:  "public",
		Name:    "status",
		Values:  []string{"active", "inactive"},
		Comment: &comment,
	}
	expected := "-- public.status\n" +
		"CREATE TYPE public.status AS ENUM (\n" +
		"    'active',\n" +
		"    'inactive'\n" +
		");\n" +
		"COMMENT ON TYPE public.status IS 'User status';"
	assert.Equal(t, expected, model.EnumToSQL(e))
}

func TestEnumsToSQL(t *testing.T) {
	enums := orderedmap.New[string, *model.Enum]()
	enums.Set("public.status", &model.Enum{
		Schema: "public",
		Name:   "status",
		Values: []string{"active", "inactive"},
	})
	enums.Set("public.role", &model.Enum{
		Schema: "public",
		Name:   "role",
		Values: []string{"admin", "user"},
	})
	got := model.EnumsToSQL(enums)
	assert.Contains(t, got, "CREATE TYPE public.status AS ENUM")
	assert.Contains(t, got, "CREATE TYPE public.role AS ENUM")
}
