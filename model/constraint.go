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

func (b ConstraintType) IsConstraintTrigger() bool {
	return b == 't'
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

func (fk ForeignKey) SQL() string {
	sql := "ALTER TABLE ONLY " + Ident(fk.Schema, fk.Table) +
		" ADD CONSTRAINT " + Ident(fk.Name) + " " + fk.Definition
	if !fk.Validated {
		sql += " NOT VALID"
	}
	return sql + ";"
}
