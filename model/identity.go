package model

import (
	"strconv"
	"strings"
)

// IdentitySequence holds the parameters of the sequence behind an identity
// column, the sequence_options of GENERATED ... AS IDENTITY. PostgreSQL takes
// the sequence data type from the column, so there is no data type here.
//
// Both the catalog and the parser fill in every field, applying the defaults
// PostgreSQL derives from the column type and the increment, so the diff is a
// plain comparison. It is nil for a column that is not an identity column.
type IdentitySequence struct {
	Start     int64
	Min       int64
	Max       int64
	Increment int64
	Cache     int64
	Cycle     bool
}

// seqTypeBounds returns the min and max value of a sequence data type. An
// unknown type falls back to the bigint bounds.
func seqTypeBounds(dataType string) (int64, int64) {
	switch dataType {
	case "smallint":
		return -32768, 32767
	case "integer":
		return -2147483648, 2147483647
	default: // bigint
		return -9223372036854775808, 9223372036854775807
	}
}

// DefaultIdentitySequence returns the parameters PostgreSQL gives an identity
// column of typeName whose definition writes no option other than the
// increment. The increment decides the direction, and the direction decides
// the bounds and the start, so it has to be resolved first.
func DefaultIdentitySequence(typeName string, increment int64) IdentitySequence {
	typeMin, typeMax := seqTypeBounds(typeName)

	seq := IdentitySequence{Increment: increment, Cache: 1}
	if increment > 0 {
		seq.Min = 1
		seq.Max = typeMax
		seq.Start = seq.Min
	} else {
		seq.Min = typeMin
		seq.Max = -1
		seq.Start = seq.Max
	}

	return seq
}

// RetypedTo returns the parameters the sequence has once the column's type
// changes from oldType to newType. PostgreSQL carries a bound left at the old
// type's default over to the new type's default and leaves an explicit bound
// alone. A diff that follows a type change reads the current side through
// this, or it plans a bound the retype already set.
func (s *IdentitySequence) RetypedTo(oldType, newType string) *IdentitySequence {
	if s == nil || oldType == newType {
		return s
	}

	old := DefaultIdentitySequence(oldType, s.Increment)
	def := DefaultIdentitySequence(newType, s.Increment)

	retyped := *s
	if retyped.Min == old.Min {
		retyped.Min = def.Min
	}
	if retyped.Max == old.Max {
		retyped.Max = def.Max
	}

	return &retyped
}

// Options returns the sequence options that differ from the defaults for a
// column of typeName, each as a clause such as "START WITH 100". A column left
// entirely at the defaults returns nothing, so the common identity column
// keeps its short form.
func (s *IdentitySequence) Options(typeName string) []string {
	if s == nil {
		return nil
	}

	// The increment's own default is 1. The rest resolve against the increment
	// this column has, so a descending identity at the bounds that direction
	// implies renders as INCREMENT BY alone.
	def := DefaultIdentitySequence(typeName, s.Increment)

	var opts []string
	if s.Start != def.Start {
		opts = append(opts, "START WITH "+strconv.FormatInt(s.Start, 10))
	}
	if s.Increment != 1 {
		opts = append(opts, "INCREMENT BY "+strconv.FormatInt(s.Increment, 10))
	}
	if s.Min != def.Min {
		opts = append(opts, "MINVALUE "+strconv.FormatInt(s.Min, 10))
	}
	if s.Max != def.Max {
		opts = append(opts, "MAXVALUE "+strconv.FormatInt(s.Max, 10))
	}
	if s.Cache != def.Cache {
		opts = append(opts, "CACHE "+strconv.FormatInt(s.Cache, 10))
	}
	if s.Cycle != def.Cycle {
		opts = append(opts, "CYCLE")
	}

	return opts
}

// OptionsSQL renders Options in the parenthesized form that follows
// GENERATED ... AS IDENTITY, with a leading space. It returns an empty string
// when every option is at its default.
func (s *IdentitySequence) OptionsSQL(typeName string) string {
	opts := s.Options(typeName)
	if len(opts) == 0 {
		return ""
	}

	return " (" + strings.Join(opts, " ") + ")"
}
