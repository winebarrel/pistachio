package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/model"
)

// ErrUnsupportedRoutine marks a CREATE FUNCTION / PROCEDURE statement that
// pistachio reads but does not manage. The parser turns it into the same
// "ignored unsupported statement" warning any other unhandled statement gets,
// and the catalog skips the row rather than failing the whole read, so both
// sides of the diff leave the same routines out. Never fail a parse over one:
// a routine the desired schema declares is read whether or not
// --manage-routine is set, so an error here would reach someone who never
// asked for routines at all.
var ErrUnsupportedRoutine = errors.New("unsupported routine")

// errRoutineConfigFromCurrent marks SET x FROM CURRENT, which captures the
// session value at creation time. The statement does not carry that value, so
// there is nothing to compare against.
//
// This is not ErrUnsupportedRoutine, because the invariant that one rests on
// does not hold here: pg_get_functiondef writes the resolved value, so the
// catalog reads such a routine back like any other. Leaving it out of the
// desired side alone would plan a DROP of a routine the schema file declares.
// The routine is marked Ignore instead, which takes the key off both sides and
// surfaces it as -- ignored:, the same as -- pista:ignore.
var errRoutineConfigFromCurrent = errors.New("routine config from current")

// Default COST and ROWS per pg_proc. pg_get_functiondef prints neither when
// the routine carries the default, so the parser drops them as well.
const (
	defaultCostC     = 1
	defaultCostOther = 100
	defaultRowsSet   = 1000
)

// ParseRoutineDef builds a Routine from a single CREATE FUNCTION or
// CREATE PROCEDURE statement. The catalog reads pg_get_functiondef and calls
// this, so both sides of the diff go through one code path and render through
// one model.Routine.SQL.
func ParseRoutineDef(sql, defaultSchema string) (*model.Routine, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse routine definition: %w", err)
	}
	if len(result.Stmts) != 1 {
		return nil, fmt.Errorf("expected one statement in routine definition, got %d", len(result.Stmts))
	}
	cfs := result.Stmts[0].Stmt.GetCreateFunctionStmt()
	if cfs == nil {
		return nil, fmt.Errorf("routine definition is not a CREATE FUNCTION statement")
	}
	return parseCreateFunctionStmt(cfs, defaultSchema)
}

func parseCreateFunctionStmt(cfs *pg_query.CreateFunctionStmt, defaultSchema string) (*model.Routine, error) {
	// A SQL-standard body (BEGIN ATOMIC) records real pg_depend entries on
	// whatever it reads, so it cannot be created ahead of the tables the way
	// pistachio orders routines. The catalog skips these too.
	if cfs.SqlBody != nil {
		return nil, ErrUnsupportedRoutine
	}

	schema, name, err := splitFuncName(cfs.Funcname, defaultSchema)
	if err != nil {
		return nil, err
	}

	routine := &model.Routine{
		Schema:    schema,
		Name:      name,
		Procedure: cfs.IsProcedure,
	}

	if err := parseRoutineParams(cfs.Parameters, routine); err != nil {
		return nil, err
	}
	if err := parseRoutineReturn(cfs.ReturnType, routine); err != nil {
		return nil, err
	}
	if err := parseRoutineOptions(cfs.Options, routine); err != nil {
		return nil, err
	}

	normalizeRoutineDefaults(routine)

	return routine, nil
}

func splitFuncName(names []*pg_query.Node, defaultSchema string) (string, string, error) {
	parts := make([]string, 0, len(names))
	for _, n := range names {
		s := n.GetString_()
		if s == nil {
			return "", "", fmt.Errorf("unexpected routine name component")
		}
		parts = append(parts, s.GetSval())
	}
	switch len(parts) {
	case 1:
		return defaultSchema, parts[0], nil
	case 2:
		return parts[0], parts[1], nil
	default:
		return "", "", fmt.Errorf("unexpected routine name: %s", strings.Join(parts, "."))
	}
}

var paramModes = map[pg_query.FunctionParameterMode]string{
	pg_query.FunctionParameterMode_FUNC_PARAM_IN:       "",
	pg_query.FunctionParameterMode_FUNC_PARAM_DEFAULT:  "",
	pg_query.FunctionParameterMode_FUNC_PARAM_OUT:      "OUT",
	pg_query.FunctionParameterMode_FUNC_PARAM_INOUT:    "INOUT",
	pg_query.FunctionParameterMode_FUNC_PARAM_VARIADIC: "VARIADIC",
	pg_query.FunctionParameterMode_FUNC_PARAM_TABLE:    "TABLE",
}

func parseRoutineParams(params []*pg_query.Node, routine *model.Routine) error {
	for _, p := range params {
		fp := p.GetFunctionParameter()
		if fp == nil {
			return fmt.Errorf("unexpected routine parameter")
		}
		mode, ok := paramModes[fp.Mode]
		if !ok {
			return fmt.Errorf("unsupported routine parameter mode: %s", fp.Mode)
		}

		typeName, err := deparseTypeName(fp.ArgType)
		if err != nil {
			return err
		}

		arg := &model.RoutineArg{
			Mode: mode,
			Name: fp.Name,
			// PostgreSQL discards a type modifier on a parameter, so the
			// catalog never reports one back. Drop it here as well or a
			// desired varchar(10) would differ from the catalog's varchar.
			Type: stripTypeMod(typeName),
		}

		if fp.Defexpr != nil {
			def, err := deparseExpr(fp.Defexpr)
			if err != nil {
				return err
			}
			arg.Default = def
		}

		routine.Args = append(routine.Args, arg)
	}
	return nil
}

// stripTypeMod removes a type modifier while keeping any array bounds:
// "numeric(10,2)[]" becomes "numeric[]".
func stripTypeMod(typeName string) string {
	open := strings.Index(typeName, "(")
	if open < 0 {
		return typeName
	}
	closeIdx := strings.Index(typeName[open:], ")")
	if closeIdx < 0 {
		return typeName
	}
	return typeName[:open] + typeName[open+closeIdx+1:]
}

func parseRoutineReturn(returnType *pg_query.TypeName, routine *model.Routine) error {
	if routine.Procedure {
		return nil
	}

	if returnType != nil {
		// SETOF is tracked separately, so clear it for the deparse: it renders
		// as part of the type name and would come back as "SETOF integer".
		setof := returnType.Setof
		returnType.Setof = false
		name, err := deparseTypeName(returnType)
		returnType.Setof = setof
		if err != nil {
			return err
		}
		routine.ReturnType = stripTypeMod(name)
		routine.ReturnsSet = setof
		return nil
	}

	// No RETURNS clause. PostgreSQL derives the result from the OUT and INOUT
	// parameters, and pg_get_functiondef writes that derived type back, so the
	// desired side has to derive it the same way.
	var outArgs []*model.RoutineArg
	for _, a := range routine.Args {
		if a.Mode == "OUT" || a.Mode == "INOUT" {
			outArgs = append(outArgs, a)
		}
	}
	switch len(outArgs) {
	case 0:
		return fmt.Errorf("function %s has no RETURNS clause and no OUT parameter", model.Ident(routine.Schema, routine.Name))
	case 1:
		routine.ReturnType = outArgs[0].Type
	default:
		routine.ReturnType = "record"
	}
	return nil
}

func parseRoutineOptions(options []*pg_query.Node, routine *model.Routine) error {
	for _, o := range options {
		de := o.GetDefElem()
		if de == nil {
			return fmt.Errorf("unexpected routine option")
		}

		switch de.Defname {
		case "as":
			if err := parseRoutineBody(de.Arg, routine); err != nil {
				return err
			}
		case "language":
			routine.Language = defElemString(de.Arg)
		case "volatility":
			routine.Volatility = strings.ToUpper(defElemString(de.Arg))
		case "strict":
			routine.Strict = defElemBool(de.Arg)
		case "security":
			routine.SecurityDefiner = defElemBool(de.Arg)
		case "leakproof":
			routine.Leakproof = defElemBool(de.Arg)
		case "parallel":
			routine.Parallel = strings.ToUpper(defElemString(de.Arg))
		case "cost":
			f, err := defElemFloat(de.Arg)
			if err != nil {
				return err
			}
			routine.Cost = &f
		case "rows":
			f, err := defElemFloat(de.Arg)
			if err != nil {
				return err
			}
			routine.Rows = &f
		case "set":
			cfg, err := parseRoutineConfig(de.Arg)
			if errors.Is(err, errRoutineConfigFromCurrent) {
				routine.Ignore = true
				continue
			}
			if err != nil {
				return err
			}
			routine.Config = append(routine.Config, cfg)
		case "window":
			// prokind 'w'. The catalog does not read window functions, so the
			// desired side leaves them alone too.
			return ErrUnsupportedRoutine
		default:
			// SUPPORT and TRANSFORM FOR TYPE reach here, and pg_get_functiondef
			// writes both back, so the catalog skips the same routines.
			return fmt.Errorf("%w: option %s", ErrUnsupportedRoutine, de.Defname)
		}
	}
	return nil
}

func parseRoutineBody(arg *pg_query.Node, routine *model.Routine) error {
	items := arg.GetList().GetItems()
	switch len(items) {
	case 1:
		routine.Body = items[0].GetString_().GetSval()
	case 2:
		// LANGUAGE c: AS 'obj_file', 'link_symbol'.
		routine.ObjFile = items[0].GetString_().GetSval()
		routine.Body = items[1].GetString_().GetSval()
	default:
		return fmt.Errorf("unexpected routine body with %d parts", len(items))
	}
	return nil
}

func parseRoutineConfig(arg *pg_query.Node) (*model.RoutineConfig, error) {
	vss := arg.GetVariableSetStmt()
	if vss == nil {
		return nil, fmt.Errorf("unexpected SET clause on routine")
	}
	if len(vss.Args) == 0 {
		return nil, fmt.Errorf("%w: SET %s", errRoutineConfigFromCurrent, vss.Name)
	}

	cfg := &model.RoutineConfig{Name: vss.Name}
	for _, a := range vss.Args {
		c := a.GetAConst()
		if c == nil {
			return nil, fmt.Errorf("unexpected SET value on routine %s", vss.Name)
		}
		switch {
		case c.GetSval() != nil:
			cfg.Args = append(cfg.Args, c.GetSval().GetSval())
		case c.GetIval() != nil:
			cfg.Args = append(cfg.Args, strconv.FormatInt(int64(c.GetIval().GetIval()), 10))
		case c.GetFval() != nil:
			cfg.Args = append(cfg.Args, c.GetFval().GetFval())
		default:
			return nil, fmt.Errorf("unexpected SET value on routine %s", vss.Name)
		}
	}
	return cfg, nil
}

// The generated getters take a nil receiver, so a non-String (non-Boolean)
// argument falls through to the proto zero value without a guard here.
func defElemString(arg *pg_query.Node) string {
	return arg.GetString_().GetSval()
}

func defElemBool(arg *pg_query.Node) bool {
	return arg.GetBoolean().GetBoolval()
}

func defElemFloat(arg *pg_query.Node) (float64, error) {
	switch {
	case arg.GetInteger() != nil:
		return float64(arg.GetInteger().GetIval()), nil
	case arg.GetFloat() != nil:
		return strconv.ParseFloat(arg.GetFloat().GetFval(), 64)
	}
	return 0, fmt.Errorf("unexpected numeric routine option")
}

// normalizeRoutineDefaults drops the attribute values pg_get_functiondef
// leaves out, so a desired schema that spells them out matches the catalog.
func normalizeRoutineDefaults(routine *model.Routine) {
	if routine.Volatility == "" {
		routine.Volatility = model.VolatilityVolatile
	}
	if routine.Parallel == "" {
		routine.Parallel = model.ParallelUnsafe
	}
	if routine.Cost != nil && *routine.Cost == defaultCost(routine.Language) {
		routine.Cost = nil
	}
	if routine.Rows != nil && (!routine.ReturnsSet || *routine.Rows == defaultRowsSet) {
		routine.Rows = nil
	}
}

func defaultCost(language string) float64 {
	switch language {
	case "c", "internal":
		return defaultCostC
	default:
		return defaultCostOther
	}
}

// parseCommentOnRoutine attaches a COMMENT ON FUNCTION / PROCEDURE to the
// routine it names. The statement carries an ObjectWithArgs rather than the
// plain name list every other COMMENT ON uses, and the argument list is what
// tells two overloads apart. PostgreSQL lets the list be left out when the
// name is unambiguous, and so does this: a bare name matches when the desired
// schema declares exactly one routine with it.
func parseCommentOnRoutine(cs *pg_query.CommentStmt, defaultSchema string, routines *orderedmap.Map[string, *model.Routine]) {
	owa := cs.Object.GetObjectWithArgs()
	if owa == nil {
		return
	}

	schema, name, err := splitFuncName(owa.Objname, defaultSchema)
	if err != nil {
		return
	}

	routine := lookupRoutine(routines, schema, name, owa)
	if routine == nil {
		return
	}

	if cs.Comment != "" {
		c := cs.Comment
		routine.Comment = &c
	} else {
		routine.Comment = nil
	}
}

func lookupRoutine(routines *orderedmap.Map[string, *model.Routine], schema, name string, owa *pg_query.ObjectWithArgs) *model.Routine {
	if !owa.ArgsUnspecified {
		types := make([]string, 0, len(owa.Objargs))
		for _, a := range owa.Objargs {
			tn := a.GetTypeName()
			if tn == nil {
				return nil
			}
			typeName, err := deparseTypeName(tn)
			if err != nil {
				return nil
			}
			// The same two steps FQRN applies, or a comment naming a type in
			// the routine's own schema would key as a different routine and
			// silently fail to attach.
			types = append(types, model.StripTypeSchema(stripTypeMod(typeName), schema))
		}
		r, _ := routines.GetOk(model.Ident(schema, name) + "(" + strings.Join(types, ", ") + ")")
		return r
	}

	var found *model.Routine
	for _, r := range routines.All() {
		if r.Schema != schema || r.Name != name {
			continue
		}
		if found != nil {
			// Ambiguous, the same way PostgreSQL would reject it.
			return nil
		}
		found = r
	}
	return found
}
