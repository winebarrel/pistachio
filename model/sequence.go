package model

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

// Sequence holds metadata for a standalone PostgreSQL sequence (one created by
// CREATE SEQUENCE, not the sequence auto-generated behind a serial or identity
// column). Sequences owned by a table column are handled as column attributes
// and are excluded from the sequence diff pipeline.
type Sequence struct {
	OID       uint32 `json:"-"`
	Schema    string `json:"schema"`
	Name      string `json:"name"`
	DataType  string `json:"data_type"`
	Start     int64  `json:"start"`
	Min       int64  `json:"min"`
	Max       int64  `json:"max"`
	Increment int64  `json:"increment"`
	Cache     int64  `json:"cache"`
	Cycle     bool   `json:"cycle,omitzero"`
	Unlogged  bool   `json:"unlogged,omitzero"`
	// OwnerTable and OwnerColumn are set from the OWNED BY relationship
	// (pg_depend deptype 'a' for serial, 'i' for identity). They are nil for
	// standalone sequences, which are the only ones the pipeline manages.
	OwnerTable  *string `json:"owner_table,omitempty"`
	OwnerColumn *string `json:"owner_column,omitempty"`
	RenameFrom  *string `json:"rename_from,omitempty"`
	Comment     *string `json:"comment,omitempty"`
	// Ignore marks the sequence as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the catalog
	// side.
	Ignore bool `json:"ignore,omitzero"`
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
	return strings.Join(
		sequences.TransformSlice(func(_ string, seq *Sequence) string {
			return SequenceToSQL(seq)
		}),
		"\n\n",
	)
}
