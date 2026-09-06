package model

import "fmt"

// https://www.postgresql.org/docs/current/catalog-pg-constraint.html

type ConstraintType byte

func (b ConstraintType) IsCheckConstraint() bool {
	return b == 'c'
}

func (b ConstraintType) IsForeignKeyConstraint() bool {
	return b == 'f'
}

func (b ConstraintType) IsNotNullConstraint() bool {
	return b == 'n'
}

func (b ConstraintType) IsPrimaryKeyConstraint() bool {
	return b == 'p'
}

func (b ConstraintType) IsUniqueConstraint() bool {
	return b == 'u'
}

func (b ConstraintType) IsExclusionConstraint() bool {
	return b == 'x'
}

type Constraint struct {
	OID        uint32
	Name       string
	RenameFrom *string
	Type       ConstraintType
	Definition string
	Columns    []string
	Deferrable bool
	Deferred   bool
	Validated  bool
	// Inherited marks a constraint a partition child holds only because its
	// parent has one. PostgreSQL refuses to alter or drop such a copy, and a
	// statement on the parent reaches it, so the diff leaves it alone. Only
	// the catalog sets it: a desired schema declares what it writes.
	Inherited bool
}

func (con *Constraint) String() string {
	return fmt.Sprintf("%#v", *con)
}

type ForeignKey struct {
	Constraint
	Schema    string
	Table     string
	RefSchema *string
	RefTable  *string
}

func (fk *ForeignKey) String() string {
	return fmt.Sprintf("%#v", *fk)
}

// SQL renders the ALTER TABLE that adds the key. onPartitioned says the owning
// table is partitioned, where PostgreSQL rejects ONLY outright; a foreign key
// is not inherited either way, so the word only tracks what pg_dump writes.
func (fk ForeignKey) SQL(onPartitioned bool) string {
	only := "ONLY "
	if onPartitioned {
		only = ""
	}
	sql := "ALTER TABLE " + only + Ident(fk.Schema, fk.Table) +
		" ADD CONSTRAINT " + Ident(fk.Name) + " " + fk.Definition
	if !fk.Validated {
		sql += " NOT VALID"
	}
	return sql + ";"
}
