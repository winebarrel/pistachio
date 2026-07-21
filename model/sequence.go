package model

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/winebarrel/orderedmap"
)

// Sequence holds metadata for a standalone PostgreSQL sequence (one created by
// CREATE SEQUENCE, not the sequence auto-generated behind a serial or identity
// column). Sequences owned by a table column are handled as column attributes
// and are excluded from the sequence diff pipeline.
type Sequence struct {
	OID       uint32
	Schema    string
	Name      string
	DataType  string
	Start     int64
	Min       int64
	Max       int64
	Increment int64
	Cache     int64
	Cycle     bool
	// OwnerTable and OwnerColumn are set from the OWNED BY relationship
	// (pg_depend deptype 'a' for serial, 'i' for identity). They are nil for
	// standalone sequences, which are the only ones the pipeline manages.
	OwnerTable  *string
	OwnerColumn *string
	RenameFrom  *string
	Comment     *string
	// Ignore marks the sequence as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the catalog
	// side.
	Ignore bool
}

func (seq Sequence) FQN() string {
	return Ident(seq.Schema, seq.Name)
}

// Owned reports whether the sequence is owned by a table column (serial or
// identity). Owned sequences are not managed as standalone objects.
func (seq Sequence) Owned() bool {
	return seq.OwnerTable != nil
}

func (seq Sequence) SQL() string {
	lines := []string{
		"CREATE SEQUENCE " + seq.FQN(),
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
	return strings.Join(
		orderedmap.TransformSlice(sequences, func(_ string, seq *Sequence) string {
			return SequenceToSQL(seq)
		}),
		"\n\n",
	)
}
