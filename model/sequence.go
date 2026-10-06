package model

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

// Sequence holds a PostgreSQL sequence. The sequences of identity columns, and
// of serial columns with the default sequence name, belong to the column and
// are not held here. Other owned sequences are, with their owner.
type Sequence struct {
	OID       uint32 `json:"oid"`
	Schema    string `json:"schema"`
	Name      string `json:"name"`
	DataType  string `json:"data_type"`
	Start     int64  `json:"start"`
	Min       int64  `json:"min"`
	Max       int64  `json:"max"`
	Increment int64  `json:"increment"`
	Cache     int64  `json:"cache"`
	Cycle     bool   `json:"cycle"`
	Unlogged  bool   `json:"unlogged"`
	// OwnerTable and OwnerColumn name the column the sequence is OWNED BY,
	// nil when no column owns it. PostgreSQL requires the table to be in the
	// sequence's schema, so the table name is not qualified.
	OwnerTable  *string `json:"owner_table"`
	OwnerColumn *string `json:"owner_column"`
	RenameFrom  *string `json:"rename_from"`
	Comment     *string `json:"comment"`
	// Ignore marks the sequence as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the catalog
	// side.
	Ignore bool `json:"ignore"`
}

func (seq Sequence) FQN() string {
	return Ident(seq.Schema, seq.Name)
}

// Owned reports whether a table column owns the sequence.
func (seq Sequence) Owned() bool {
	return seq.OwnerTable != nil && seq.OwnerColumn != nil
}

// OwnerFQTN returns the qualified name of the table that owns the sequence, or
// "" when no column owns it.
func (seq Sequence) OwnerFQTN() string {
	if !seq.Owned() {
		return ""
	}
	return Ident(seq.Schema, *seq.OwnerTable)
}

// OwnedBySQL returns the ALTER SEQUENCE ... OWNED BY for the sequence, or ""
// when no column owns it. It is separate from SQL because the sequence is
// created before the table and OWNED BY needs the table.
func (seq Sequence) OwnedBySQL() string {
	if !seq.Owned() {
		return ""
	}
	return "ALTER SEQUENCE " + seq.FQN() + " OWNED BY " + Ident(seq.Schema, *seq.OwnerTable, *seq.OwnerColumn) + ";"
}

func (seq Sequence) SQL() string {
	create := "CREATE SEQUENCE "
	if seq.Unlogged {
		create = "CREATE UNLOGGED SEQUENCE "
	}
	lines := []string{
		create + seq.FQN(),
		"    AS " + seq.DataType,
		"    START WITH " + strconv.FormatInt(seq.Start, 10),
		"    INCREMENT BY " + strconv.FormatInt(seq.Increment, 10),
		"    MINVALUE " + strconv.FormatInt(seq.Min, 10),
		"    MAXVALUE " + strconv.FormatInt(seq.Max, 10),
		"    CACHE " + strconv.FormatInt(seq.Cache, 10),
	}
	if seq.Cycle {
		lines = append(lines, "    CYCLE")
	}
	return strings.Join(lines, "\n") + ";"
}

func (seq Sequence) CommentSQL() string {
	if seq.Comment != nil {
		return "COMMENT ON SEQUENCE " + seq.FQN() + " IS " + QuoteLiteral(*seq.Comment) + ";"
	}
	return ""
}

// String returns a debug-friendly representation.
func (seq Sequence) String() string {
	return fmt.Sprintf("%#v", seq)
}

func SequenceToSQL(seq *Sequence) string {
	parts := []string{"-- " + seq.FQN(), seq.SQL()}
	if s := seq.CommentSQL(); s != "" {
		parts = append(parts, s)
	}
	return strings.Join(parts, "\n")
}

func SequencesToSQL(sequences *orderedmap.Map[string, *Sequence]) string {
	return joinSQL(sequences, SequenceToSQL)
}

// SequencesOwnedBySQL returns the OWNED BY statement of every owned sequence,
// in the order of the map, or "" when no column owns any of them.
func SequencesOwnedBySQL(sequences *orderedmap.Map[string, *Sequence]) string {
	var stmts []string
	for seq := range sequences.Values() {
		if s := seq.OwnedBySQL(); s != "" {
			stmts = append(stmts, s)
		}
	}
	return strings.Join(stmts, "\n")
}
