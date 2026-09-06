package model

type Index struct {
	OID          uint32
	Schema       string
	Name         string
	RenameFrom   *string
	Table        string
	Definition   string
	TableSpace   *string
	Concurrently bool
	Comment      *string
}

func (idx Index) FQTN() string {
	return Ident(idx.Schema, idx.Table)
}

func (idx Index) SQL() string {
	return idx.Definition + ";"
}

// CommentSQL renders the index's own COMMENT ON, or an empty string when it
// carries none. COMMENT ON INDEX names the index alone, never the relation it
// sits on.
func (idx Index) CommentSQL() string {
	if idx.Comment == nil {
		return ""
	}
	return "COMMENT ON INDEX " + Ident(idx.Schema, idx.Name) + " IS " + QuoteLiteral(*idx.Comment) + ";"
}
