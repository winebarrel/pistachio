package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/orderedmap"
)

func TestView_FQVN(t *testing.T) {
	v := View{Schema: "public", Name: "active_users"}
	assert.Equal(t, "public.active_users", v.FQVN())
}

func TestView_SQL(t *testing.T) {
	v := View{Schema: "public", Name: "active_users", Definition: "SELECT id, name FROM users WHERE active = true"}
	expected := "CREATE OR REPLACE VIEW public.active_users AS\nSELECT id, name FROM users WHERE active = true;"
	assert.Equal(t, expected, v.SQL())
}

func TestView_SQL_trims(t *testing.T) {
	v := View{Schema: "public", Name: "v1", Definition: "  SELECT 1;  "}
	assert.Equal(t, "CREATE OR REPLACE VIEW public.v1 AS\nSELECT 1;", v.SQL())
}

func TestView_CommentSQL(t *testing.T) {
	comment := "Active users view"
	v := View{Schema: "public", Name: "active_users", Comment: &comment}
	assert.Equal(t, "COMMENT ON VIEW public.active_users IS 'Active users view';", v.CommentSQL())
}

func TestView_CommentSQL_nil(t *testing.T) {
	v := View{Schema: "public", Name: "active_users"}
	assert.Equal(t, "", v.CommentSQL())
}

func TestViewsToSQL(t *testing.T) {
	comment := "my view"
	views := orderedmap.New[string, *View]()
	views.Set("public.v1", &View{Schema: "public", Name: "v1", Definition: "SELECT 1", Comment: &comment})
	views.Set("public.v2", &View{Schema: "public", Name: "v2", Definition: "SELECT 2"})

	got := ViewsToSQL(views)
	expected := `-- public.v1
CREATE OR REPLACE VIEW public.v1 AS
SELECT 1;
COMMENT ON VIEW public.v1 IS 'my view';

-- public.v2
CREATE OR REPLACE VIEW public.v2 AS
SELECT 2;`
	assert.Equal(t, expected, got)
}
