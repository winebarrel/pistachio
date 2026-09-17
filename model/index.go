package model

type Index struct {
	OID          uint32  `json:"oid"`
	Schema       string  `json:"schema"`
	Name         string  `json:"name"`
	RenameFrom   *string `json:"rename_from"`
	Table        string  `json:"table"`
	Definition   string  `json:"definition"`
	TableSpace   *string `json:"table_space"`
	Concurrently bool    `json:"concurrently"`
	Comment      *string `json:"comment"`
	// Attached marks an index attached to an index on the partitioned parent.
	// PostgreSQL rejects a DROP of one and drops it with the parent's. Only the
	// catalog sets it.
	Attached bool `json:"attached"`
	// Size is the size estimate dump --explain writes in a comment above the
	// index. Only dump sets it.
	Size string `json:"-"`
}

func (idx Index) FQTN() string {
	return Ident(idx.Schema, idx.Table)
}

func (idx Index) SQL() string {
	return idx.Definition + ";"
}

// DumpSQL is SQL with the size comment dump --explain puts above it.
func (idx Index) DumpSQL() string {
	if idx.Size == "" {
		return idx.SQL()
	}
	return "-- " + idx.Size + "\n" + idx.SQL()
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
