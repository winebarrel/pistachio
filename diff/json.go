package diff

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/pistachio/internal/pgast"
)

// A SQL/JSON expression carries clauses PostgreSQL resolves while parsing and
// pg_get_expr then prints back in full, where the written definition left them
// off. Without the rules below, every schema using one of these expressions
// re-emits its constraint or view on each plan.
//
// Each rule was read off a running server rather than from the grammar, on 16,
// 17 and 18. The forms are pinned by the fixtures in
// testdata/plan/no_diff_json_*.yml and testdata/plan/alter_json_query_clauses.yml.
//
// Two shapes appear here, and the difference matters:
//
//   - Symmetric (normalizeJsonClauses): the clause resolves to one value that
//     does not depend on the arguments, so writing that value and leaving the
//     clause off mean the same thing. Both sides are reduced to "left off",
//     and any other value is left alone so it still surfaces as a difference.
//   - Asymmetric (alignJsonConstructorOutput): JSON_OBJECT, JSON_ARRAY and the
//     two aggregates resolve their return type from their argument types, and
//     pg_query does no type inference, so which of json and jsonb the server
//     would pick is not knowable here. The catalog's clause is dropped only
//     where the desired side wrote none, because then whatever the server
//     resolved for the stored definition is what the written one resolves to
//     as well.

// normalizeJsonClauses reduces the clauses of node that hold a resolved
// default to the form a definition written without them parses to. It mutates
// node in place and is applied to the catalog side and the written side alike.
func normalizeJsonClauses(node *pg_query.Node) {
	switch n := node.Node.(type) {
	case *pg_query.Node_JsonFuncExpr:
		normalizeJsonFuncExpr(n.JsonFuncExpr)
	case *pg_query.Node_JsonSerializeExpr:
		// JSON_SERIALIZE returns text whatever it is handed.
		n.JsonSerializeExpr.Output = dropDefaultJsonOutput(n.JsonSerializeExpr.Output, "text")
	case *pg_query.Node_JsonObjectConstructor:
		clearJsonOutputFormat(n.JsonObjectConstructor.Output)
	case *pg_query.Node_JsonArrayConstructor:
		clearJsonOutputFormat(n.JsonArrayConstructor.Output)
	case *pg_query.Node_JsonArrayQueryConstructor:
		clearJsonOutputFormat(n.JsonArrayQueryConstructor.Output)
	case *pg_query.Node_JsonObjectAgg:
		if c := n.JsonObjectAgg.Constructor; c != nil {
			clearJsonOutputFormat(c.Output)
		}
	case *pg_query.Node_JsonArrayAgg:
		if c := n.JsonArrayAgg.Constructor; c != nil {
			clearJsonOutputFormat(c.Output)
		}
	case *pg_query.Node_JsonParseExpr:
		clearJsonOutputFormat(n.JsonParseExpr.Output)
	case *pg_query.Node_JsonScalarExpr:
		clearJsonOutputFormat(n.JsonScalarExpr.Output)
	}
}

// normalizeJsonFuncExpr handles the PostgreSQL 17 query functions. Their
// return type does not depend on the argument types: JSON_VALUE returns text,
// JSON_QUERY returns jsonb and JSON_EXISTS returns boolean and takes no
// RETURNING clause at all.
func normalizeJsonFuncExpr(fn *pg_query.JsonFuncExpr) {
	canonicalizeJsonPathLiteral(fn.Pathspec)

	switch fn.Op {
	case pg_query.JsonExprOp_JSON_VALUE_OP:
		fn.Output = dropDefaultJsonOutput(fn.Output, "text")
	case pg_query.JsonExprOp_JSON_QUERY_OP:
		fn.Output = dropDefaultJsonOutput(fn.Output, "jsonb")
	}

	// JSON_QUERY prints its wrapper and its quote behaviour whether or not
	// they hold the default, so a definition written without them still comes
	// back with WITHOUT WRAPPER KEEP QUOTES. WITH CONDITIONAL WRAPPER, WITH
	// UNCONDITIONAL WRAPPER and OMIT QUOTES change the result at runtime and
	// are left alone.
	if fn.Wrapper == pg_query.JsonWrapper_JSW_NONE {
		fn.Wrapper = pg_query.JsonWrapper_JSW_UNSPEC
	}
	if fn.Quotes == pg_query.JsonQuotes_JS_QUOTES_KEEP {
		fn.Quotes = pg_query.JsonQuotes_JS_QUOTES_UNSPEC
	}

	// ON EMPTY and ON ERROR are printed only when they differ from the
	// default, so the catalog side is already empty for a default one. A
	// definition that spells the default out is the same thing written
	// longhand. JSON_EXISTS defaults to FALSE, the other two to NULL.
	fallback := pg_query.JsonBehaviorType_JSON_BEHAVIOR_NULL
	if fn.Op == pg_query.JsonExprOp_JSON_EXISTS_OP {
		fallback = pg_query.JsonBehaviorType_JSON_BEHAVIOR_FALSE
	}
	fn.OnEmpty = dropDefaultJsonBehavior(fn.OnEmpty, fallback)
	fn.OnError = dropDefaultJsonBehavior(fn.OnError, fallback)
}

// alignJsonConstructorOutput drops the catalog side's RETURNING clause on the
// expressions whose return type is resolved from their arguments, where the
// desired side wrote none. Only json and jsonb are dropped, since those are
// the two the server can resolve to; a constructor written to return text or
// bytea is a difference and stays.
//
// This is the one place that names node kinds, and it has to: the rule holds
// exactly for the constructors and the aggregates. Applying it to JSON_QUERY
// would hide a stored RETURNING json against a written form that returns
// jsonb, which is a real difference.
func alignJsonConstructorOutput(desired, current *pg_query.Node) {
	switch cn := current.Node.(type) {
	case *pg_query.Node_JsonObjectConstructor:
		if dn, ok := desired.Node.(*pg_query.Node_JsonObjectConstructor); ok {
			cn.JsonObjectConstructor.Output = dropResolvedJsonOutput(dn.JsonObjectConstructor.Output, cn.JsonObjectConstructor.Output)
		}
	case *pg_query.Node_JsonArrayConstructor:
		if dn, ok := desired.Node.(*pg_query.Node_JsonArrayConstructor); ok {
			cn.JsonArrayConstructor.Output = dropResolvedJsonOutput(dn.JsonArrayConstructor.Output, cn.JsonArrayConstructor.Output)
		}
	case *pg_query.Node_JsonArrayQueryConstructor:
		if dn, ok := desired.Node.(*pg_query.Node_JsonArrayQueryConstructor); ok {
			cn.JsonArrayQueryConstructor.Output = dropResolvedJsonOutput(dn.JsonArrayQueryConstructor.Output, cn.JsonArrayQueryConstructor.Output)
		}
	case *pg_query.Node_JsonObjectAgg:
		dn, ok := desired.Node.(*pg_query.Node_JsonObjectAgg)
		if ok && cn.JsonObjectAgg.Constructor != nil && dn.JsonObjectAgg.Constructor != nil {
			cn.JsonObjectAgg.Constructor.Output = dropResolvedJsonOutput(dn.JsonObjectAgg.Constructor.Output, cn.JsonObjectAgg.Constructor.Output)
		}
	case *pg_query.Node_JsonArrayAgg:
		dn, ok := desired.Node.(*pg_query.Node_JsonArrayAgg)
		if ok && cn.JsonArrayAgg.Constructor != nil && dn.JsonArrayAgg.Constructor != nil {
			cn.JsonArrayAgg.Constructor.Output = dropResolvedJsonOutput(dn.JsonArrayAgg.Constructor.Output, cn.JsonArrayAgg.Constructor.Output)
		}
	}
}

// canonicalizeJsonPathLiteral rewrites a string constant holding a jsonpath
// into the spelling the server stores. Anything that is not a string constant,
// and any path the canonicaliser does not fully recognise, is left alone.
func canonicalizeJsonPathLiteral(node *pg_query.Node) {
	ac := node.GetAConst()
	if ac == nil {
		return
	}
	sv := ac.GetSval()
	if sv == nil {
		return
	}
	if canonical, ok := pgast.CanonicalJSONPath(sv.Sval); ok {
		sv.Sval = canonical
	}
}

// alignJsonPathCast canonicalises the desired side's path literal where the
// catalog holds the same position as an explicit ::jsonpath cast.
//
// Outside the query functions a path is an ordinary argument (a @? '$.x',
// jsonb_path_query(a, '$.x')) and only the catalog spells out the cast that
// says so, so the written side cannot be recognised as a path on its own.
// Pairing the two sides is what identifies it. The cast itself is then
// stripped by the caller, as any other current-only cast would be.
func alignJsonPathCast(desired, current *pg_query.Node) {
	ct := current.GetTypeCast()
	if ct == nil || !isPlainJsonTypeName(ct.TypeName, "jsonpath") {
		return
	}
	canonicalizeJsonPathLiteral(desired)
}

func dropResolvedJsonOutput(desired, current *pg_query.JsonOutput) *pg_query.JsonOutput {
	if desired != nil || current == nil {
		return current
	}
	if isPlainJsonTypeName(current.TypeName, "json") || isPlainJsonTypeName(current.TypeName, "jsonb") {
		return nil
	}
	return current
}

// dropDefaultJsonOutput returns nil when out names exactly the type the
// expression returns anyway, so that writing the clause and leaving it off
// compare equal. A type with a modifier (character varying(10)) or an array
// form is never the resolved default and is kept.
func dropDefaultJsonOutput(out *pg_query.JsonOutput, defaultType string) *pg_query.JsonOutput {
	if out == nil {
		return nil
	}
	if !isPlainJsonTypeName(out.TypeName, defaultType) {
		clearJsonOutputFormat(out)
		return out
	}
	// A FORMAT written on the clause is printed back by JSON_QUERY, so the two
	// sides only agree when both carry it. Keep the clause in that case.
	if r := out.Returning; r != nil && r.Format != nil &&
		r.Format.FormatType != pg_query.JsonFormatType_JS_FORMAT_DEFAULT {
		return out
	}
	return nil
}

// clearJsonOutputFormat resets the FORMAT of a RETURNING clause to the default.
// The constructors and JSON_SERIALIZE never print it back, so `RETURNING json
// FORMAT JSON` and `RETURNING json` are one and the same in the catalog.
func clearJsonOutputFormat(out *pg_query.JsonOutput) {
	if out == nil || out.Returning == nil || out.Returning.Format == nil {
		return
	}
	out.Returning.Format.FormatType = pg_query.JsonFormatType_JS_FORMAT_DEFAULT
	out.Returning.Format.Encoding = pg_query.JsonEncoding_JS_ENC_DEFAULT
}

// dropDefaultJsonBehavior returns nil for an ON EMPTY / ON ERROR clause that
// names the behaviour the expression takes anyway. A behaviour carrying an
// expression (DEFAULT 'z' ON EMPTY) is never the default.
func dropDefaultJsonBehavior(b *pg_query.JsonBehavior, defaultType pg_query.JsonBehaviorType) *pg_query.JsonBehavior {
	if b == nil || b.Expr != nil {
		return b
	}
	if b.Btype == defaultType {
		return nil
	}
	return b
}

// isPlainJsonTypeName reports whether tn names exactly the given built-in type
// with no modifier and no array bounds. pg_query canonicalises some type
// keywords into a pg_catalog-qualified form (RETURNING json parses to
// pg_catalog.json) while leaving others bare (jsonb), so both spellings count;
// a user-defined type that happens to be named myapp.json does not.
func isPlainJsonTypeName(tn *pg_query.TypeName, want string) bool {
	if tn == nil || len(tn.Typmods) != 0 || len(tn.ArrayBounds) != 0 {
		return false
	}
	switch len(tn.Names) {
	case 1:
		s := tn.Names[0].GetString_()
		return s != nil && s.Sval == want
	case 2:
		schema := tn.Names[0].GetString_()
		s := tn.Names[1].GetString_()
		return schema != nil && s != nil && schema.Sval == "pg_catalog" && s.Sval == want
	}
	return false
}
