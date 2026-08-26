package model

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/winebarrel/orderedmap/v2"
)

// Routine holds metadata for a PostgreSQL function or procedure (pg_proc rows
// with prokind 'f' or 'p'). Aggregates and window functions are not managed,
// and neither is a function whose body is written in the SQL-standard
// BEGIN ATOMIC form: such a body records real pg_depend entries on the tables
// it reads, which contradicts the create order pistachio uses for routines.
type Routine struct {
	OID       uint32
	Schema    string
	Name      string
	Procedure bool
	Args      []*RoutineArg
	// ReturnType is empty for a procedure. RETURNS TABLE is stored as "record"
	// with ReturnsSet true; the columns live in Args with mode TABLE.
	ReturnType string
	ReturnsSet bool
	Language   string
	// Body is the string in the AS clause. For LANGUAGE c the AS clause names
	// two strings, and Body holds the link symbol while ObjFile holds the
	// object file.
	Body            string
	ObjFile         string
	Volatility      string // IMMUTABLE, STABLE or VOLATILE
	Strict          bool
	SecurityDefiner bool
	Leakproof       bool
	Parallel        string // SAFE, RESTRICTED or UNSAFE
	// Cost and Rows are nil when the routine carries the default for its
	// language, matching pg_get_functiondef, which prints neither.
	Cost   *float64
	Rows   *float64
	Config []*RoutineConfig
	// Comment is read from pg_description on the catalog side and from
	// COMMENT ON FUNCTION / PROCEDURE on the desired side.
	Comment *string
	// Ignore marks the routine as unmanaged (set by -- pista:ignore). Ignored
	// objects are not created, altered, or dropped; always false on the
	// catalog side.
	Ignore bool
}

// RoutineArg is one entry of a routine's parameter list.
type RoutineArg struct {
	// Mode is "", "OUT", "INOUT", "VARIADIC" or "TABLE". An empty Mode is IN.
	Mode string
	Name string
	// Type carries no type modifier: PostgreSQL discards one on a parameter,
	// so keeping it would differ from what the catalog reports back.
	Type string
	// Default is the deparsed default expression, empty when the parameter
	// has none.
	Default string
}

// RoutineConfig is one SET clause on a routine (pg_proc.proconfig).
type RoutineConfig struct {
	Name string
	Args []string
}

// Volatility and parallel-safety values, and the defaults pg_get_functiondef
// leaves out of its output.
const (
	VolatilityVolatile = "VOLATILE"
	ParallelUnsafe     = "UNSAFE"
)

// InArg reports whether the parameter is part of the routine's identity.
// PostgreSQL keys pg_proc by the IN, INOUT and VARIADIC parameter types, so
// those are the ones FQRN spells out.
func (a RoutineArg) InArg() bool {
	switch a.Mode {
	case "", "INOUT", "VARIADIC":
		return true
	default:
		return false
	}
}

// FQRN returns the key the diff matches routines by: the schema-qualified name
// with the identity argument list, in the shape
// pg_get_function_identity_arguments produces. Two routines in one schema can
// share a name, so the argument list is part of it.
//
// A type in the routine's own schema loses that qualifier here. The catalog
// reports such a type bare when search_path reaches it while a desired schema
// may write it qualified, and without this the two spellings would key as two
// routines, so a plan would create one and drop the other on every run.
// Signature renders the types as they were read, for the SQL that has to
// resolve them.
func (r Routine) FQRN() string {
	return Ident(r.Schema, r.Name) + "(" + strings.Join(r.argTypes(true), ", ") + ")"
}

// Signature returns the schema-qualified name and identity argument list with
// the type names as read, for a DROP or a COMMENT that PostgreSQL has to
// resolve under the same search_path the types were reported through.
func (r Routine) Signature() string {
	return Ident(r.Schema, r.Name) + "(" + strings.Join(r.argTypes(false), ", ") + ")"
}

func (r Routine) argTypes(stripSchema bool) []string {
	types := make([]string, 0, len(r.Args))
	for _, a := range r.Args {
		if !a.InArg() {
			continue
		}
		if stripSchema {
			types = append(types, StripTypeSchema(a.Type, r.Schema))
			continue
		}
		types = append(types, a.Type)
	}
	return types
}

// Kind returns the keyword the DDL for this routine uses.
func (r Routine) Kind() string {
	if r.Procedure {
		return "PROCEDURE"
	}
	return "FUNCTION"
}

// TableArgs returns the RETURNS TABLE columns, empty for any other routine.
func (r Routine) TableArgs() []*RoutineArg {
	var args []*RoutineArg
	for _, a := range r.Args {
		if a.Mode == "TABLE" {
			args = append(args, a)
		}
	}
	return args
}

func (a RoutineArg) String() string {
	var b strings.Builder
	if a.Mode != "" && a.Mode != "TABLE" {
		b.WriteString(a.Mode)
		b.WriteByte(' ')
	}
	if a.Name != "" {
		b.WriteString(quoteIdent(a.Name))
		b.WriteByte(' ')
	}
	b.WriteString(a.Type)
	if a.Default != "" {
		b.WriteString(" DEFAULT ")
		b.WriteString(a.Default)
	}
	return b.String()
}

// SQL returns the canonical CREATE statement. Both sides of the diff build a
// Routine through the parser and render it here, so a dump fed back as the
// desired schema produces the same text again.
func (r Routine) SQL() string {
	params := make([]string, 0, len(r.Args))
	for _, a := range r.Args {
		if a.Mode == "TABLE" {
			continue
		}
		params = append(params, a.String())
	}

	lines := []string{
		"CREATE OR REPLACE " + r.Kind() + " " + Ident(r.Schema, r.Name) + "(" + strings.Join(params, ", ") + ")",
	}
	if ret := r.returnsClause(); ret != "" {
		lines = append(lines, "    "+ret)
	}
	lines = append(lines, "    LANGUAGE "+quoteIdent(r.Language))
	if attrs := r.attrClause(); attrs != "" {
		lines = append(lines, "    "+attrs)
	}
	for _, c := range r.Config {
		lines = append(lines, "    "+c.SQL())
	}
	lines = append(lines, "    AS "+r.asClause())

	return strings.Join(lines, "\n") + ";"
}

func (r Routine) returnsClause() string {
	if r.Procedure {
		return ""
	}
	if tableArgs := r.TableArgs(); len(tableArgs) > 0 {
		cols := make([]string, len(tableArgs))
		for i, a := range tableArgs {
			cols[i] = quoteIdent(a.Name) + " " + a.Type
		}
		return "RETURNS TABLE(" + strings.Join(cols, ", ") + ")"
	}
	if r.ReturnType == "" {
		return ""
	}
	if r.ReturnsSet {
		return "RETURNS SETOF " + r.ReturnType
	}
	return "RETURNS " + r.ReturnType
}

// attrClause renders the attributes that differ from the defaults.
// pg_get_functiondef leaves the defaults out, so rendering them would make a
// hand-written VOLATILE or PARALLEL UNSAFE read as a change on every run.
func (r Routine) attrClause() string {
	var attrs []string
	if r.Volatility != "" && r.Volatility != VolatilityVolatile {
		attrs = append(attrs, r.Volatility)
	}
	if r.Strict {
		attrs = append(attrs, "STRICT")
	}
	if r.SecurityDefiner {
		attrs = append(attrs, "SECURITY DEFINER")
	}
	if r.Leakproof {
		attrs = append(attrs, "LEAKPROOF")
	}
	if r.Parallel != "" && r.Parallel != ParallelUnsafe {
		attrs = append(attrs, "PARALLEL "+r.Parallel)
	}
	if r.Cost != nil {
		attrs = append(attrs, "COST "+formatFloat(*r.Cost))
	}
	if r.Rows != nil {
		attrs = append(attrs, "ROWS "+formatFloat(*r.Rows))
	}
	return strings.Join(attrs, " ")
}

func (r Routine) asClause() string {
	if r.ObjFile != "" {
		return QuoteLiteral(r.ObjFile) + ", " + QuoteLiteral(r.Body)
	}
	return DollarQuote(r.Body)
}

// SQL renders one SET clause. PostgreSQL parses SET x TO y and SET x TO 'y'
// to the same value, so either spelling in the desired schema matches what
// the catalog reports.
func (c RoutineConfig) SQL() string {
	args := make([]string, len(c.Args))
	for i, a := range c.Args {
		args[i] = configValue(a)
	}
	return "SET " + quoteIdent(c.Name) + " TO " + strings.Join(args, ", ")
}

// configValue renders one SET value the way pg_get_functiondef does: bare when
// the value is an identifier, a string literal otherwise. A double-quoted form
// would read as an identifier, which is not what a value like 64MB means.
func configValue(v string) string {
	if quoteIdent(v) == v {
		return v
	}
	return QuoteLiteral(v)
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// DollarQuote wraps s in the shortest dollar-quote delimiter it does not
// already contain.
func DollarQuote(s string) string {
	tag := "$$"
	for i := 1; strings.Contains(s, tag); i++ {
		tag = "$" + strings.Repeat("_", i) + "$"
	}
	return tag + s + tag
}

func (r Routine) CommentSQL() string {
	if r.Comment != nil {
		return "COMMENT ON " + r.Kind() + " " + r.Signature() + " IS " + QuoteLiteral(*r.Comment) + ";"
	}
	return ""
}

// DropSQL returns the DROP statement for the routine.
func (r Routine) DropSQL() string {
	return "DROP " + r.Kind() + " " + r.Signature() + ";"
}

// String returns a debug-friendly representation.
func (r Routine) String() string {
	return fmt.Sprintf("%#v", r)
}

func RoutineToSQL(r *Routine) string {
	parts := []string{"-- " + r.Signature(), r.SQL()}
	if s := r.CommentSQL(); s != "" {
		parts = append(parts, s)
	}
	return strings.Join(parts, "\n")
}

func RoutinesToSQL(routines *orderedmap.Map[string, *Routine]) string {
	return strings.Join(
		routines.TransformSlice(func(_ string, r *Routine) string {
			return RoutineToSQL(r)
		}),
		"\n\n",
	)
}
