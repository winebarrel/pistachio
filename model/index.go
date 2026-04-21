package model

type Index struct {
	OID        uint32
	Schema     string
	Name       string
	RenameFrom *string
	Table      string
	Definition string
	TableSpace *string
}

func (idx Index) FQTN() string {
	return Ident(idx.Schema, idx.Table)
}

func (idx Index) SQL() string {
	return idx.Definition + ";"
}
