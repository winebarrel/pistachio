package model

import "fmt"

// https://www.postgresql.org/docs/current/catalog-pg-constraint.html

type ConstraintType byte

// MarshalJSON writes the constraint kind as a word instead of the
// pg_constraint character, since the JSON is read outside pistachio.
func (b ConstraintType) MarshalJSON() ([]byte, error) {
	switch b {
	case 'c':
		return []byte(`"check"`), nil
	case 'f':
		return []byte(`"foreign_key"`), nil
	case 'n':
		return []byte(`"not_null"`), nil
	case 'p':
		return []byte(`"primary_key"`), nil
	case 'u':
		return []byte(`"unique"`), nil
	case 'x':
		return []byte(`"exclusion"`), nil
	default:
		return []byte(`""`), nil
	}
}

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
	OID        uint32         `json:"oid"`
	Name       string         `json:"name"`
	RenameFrom *string        `json:"rename_from"`
	Type       ConstraintType `json:"type"`
	Definition string         `json:"definition"`
	Columns    []string       `json:"columns"`
	Deferrable bool           `json:"deferrable"`
	Deferred   bool           `json:"deferred"`
	Validated  bool           `json:"validated"`
	// Inherited marks a constraint a partition child holds only because its
	// parent has one. PostgreSQL refuses to alter or drop such a copy, and a
	// statement on the parent reaches it, so the diff leaves it alone. Only
	// the catalog sets it: a desired schema declares what it writes.
	Inherited bool `json:"inherited"`
	// IndexName is the index a desired constraint written
	// ADD CONSTRAINT ... USING INDEX takes over. Only the parser sets it:
	// the promotion renames the index to the constraint's name, so the
	// catalog has no such name to report. The diff keeps that index out of
	// the index drops and adds, and takes an existing constraint of the same
	// name, type and deferral as satisfying the declaration.
	IndexName string `json:"index_name"`
}

func (con *Constraint) String() string {
	return fmt.Sprintf("%#v", *con)
}

type ForeignKey struct {
	Constraint
	Schema    string  `json:"schema"`
	Table     string  `json:"table"`
	RefSchema *string `json:"ref_schema"`
	RefTable  *string `json:"ref_table"`
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
