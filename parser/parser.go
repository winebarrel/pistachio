package parser

import (
	"fmt"
	"os"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

type ParseResult struct {
	Tables *orderedmap.Map[string, *model.Table]
	Views  *orderedmap.Map[string, *model.View]
}

func setUnique[V any](m *orderedmap.Map[string, V], key, kind string, v V) error {
	if _, ok := m.GetOk(key); ok {
		return fmt.Errorf("duplicate %s: %s", kind, key)
	}
	m.Set(key, v)
	return nil
}

func ReadSQLFile(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read SQL file: %w", err)
	}

	return string(data), nil
}

func ParseSQLFile(path string) (*ParseResult, error) {
	return ParseSQLFileWithSchema(path, "public")
}

func ParseSQLFileWithSchema(path string, defaultSchema string) (*ParseResult, error) {
	sql, err := ReadSQLFile(path)
	if err != nil {
		return nil, err
	}

	return ParseSQLWithSchema(sql, defaultSchema)
}

func ParseSQLFiles(paths []string) (*ParseResult, error) {
	return ParseSQLFilesWithSchema(paths, "public")
}

func ParseSQLFilesWithSchema(paths []string, defaultSchema string) (*ParseResult, error) {
	var sqls []string
	for _, path := range paths {
		sql, err := ReadSQLFile(path)
		if err != nil {
			return nil, err
		}
		sqls = append(sqls, sql)
	}

	return ParseSQLWithSchema(strings.Join(sqls, "\n"), defaultSchema)
}

func ParseSQL(sql string) (*ParseResult, error) {
	return ParseSQLWithSchema(sql, "public")
}

func ParseSQLWithSchema(sql string, defaultSchema string) (*ParseResult, error) {
	result, err := pg_query.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	tables := orderedmap.New[string, *model.Table]()
	views := orderedmap.New[string, *model.View]()

	for _, rawStmt := range result.Stmts {
		node := rawStmt.Stmt

		switch {
		case node.GetCreateStmt() != nil:
			table, err := parseCreateStmt(node.GetCreateStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if err := setUnique(tables, table.FQTN(), "table", table); err != nil {
				return nil, err
			}

		case node.GetViewStmt() != nil:
			view, err := parseViewStmt(node.GetViewStmt(), defaultSchema)
			if err != nil {
				return nil, err
			}
			if err := setUnique(views, view.FQVN(), "view", view); err != nil {
				return nil, err
			}

		case node.GetIndexStmt() != nil:
			idx, err := parseIndexStmt(node.GetIndexStmt(), rawStmt, defaultSchema)
			if err != nil {
				return nil, err
			}
			fqtn := model.Ident(idx.Schema, idx.Table)
			if t, ok := tables.GetOk(fqtn); ok {
				if err := setUnique(t.Indexes, idx.Name, "index", idx); err != nil {
					return nil, err
				}
			}

		case node.GetAlterTableStmt() != nil:
			as := node.GetAlterTableStmt()
			schema := as.Relation.Schemaname
			if schema == "" {
				schema = defaultSchema
			}
			fqtn := model.Ident(schema, as.Relation.Relname)
			t, ok := tables.GetOk(fqtn)
			if !ok {
				continue
			}

			con, fk, err := parseAlterTableConstraint(as, defaultSchema)
			if err != nil {
				return nil, err
			}
			if fk != nil {
				if err := setUnique(t.ForeignKeys, fk.Name, "foreign key", fk); err != nil {
					return nil, err
				}
			} else if con != nil {
				if err := setUnique(t.Constraints, con.Name, "constraint", con); err != nil {
					return nil, err
				}
			}

		case node.GetCommentStmt() != nil:
			cs := node.GetCommentStmt()
			parseCommentStmt(cs, defaultSchema, tables, views)
		}
	}

	return &ParseResult{Tables: tables, Views: views}, nil
}

func parseCreateStmt(cs *pg_query.CreateStmt, defaultSchema string) (*model.Table, error) {
	schema := cs.Relation.Schemaname
	if schema == "" {
		schema = defaultSchema
	}

	table := &model.Table{
		Schema:      schema,
		Name:        cs.Relation.Relname,
		Unlogged:    cs.Relation.Relpersistence == "u",
		Partitioned: cs.Partspec != nil,
		Columns:     orderedmap.New[string, *model.Column](),
		Constraints: orderedmap.New[string, *model.Constraint](),
		ForeignKeys: orderedmap.New[string, *model.ForeignKey](),
		Indexes:     orderedmap.New[string, *model.Index](),
	}

	if cs.Tablespacename != "" {
		ts := cs.Tablespacename
		table.TableSpace = &ts
	}

	if cs.Partspec != nil {
		def, err := deparsePartitionSpec(cs)
		if err != nil {
			return nil, err
		}
		table.PartitionDef = &def
	}

	if len(cs.InhRelations) > 0 {
		rv := cs.InhRelations[0].GetRangeVar()
		if rv != nil {
			parentSchema := rv.Schemaname
			if parentSchema == "" {
				parentSchema = defaultSchema
			}
			parent := model.Ident(parentSchema, rv.Relname)
			table.PartitionOf = &parent

			if cs.Partbound != nil {
				bound, err := deparsePartitionBound(cs)
				if err != nil {
					return nil, err
				}
				table.PartitionBound = &bound
			}
		}
	}

	for _, elt := range cs.TableElts {
		switch {
		case elt.GetColumnDef() != nil:
			cd := elt.GetColumnDef()
			col, err := parseColumnDef(cd)
			if err != nil {
				return nil, err
			}
			if err := setUnique(table.Columns, col.Name, "column", col); err != nil {
				return nil, err
			}

			// Extract column-level constraints (PRIMARY KEY, UNIQUE, CHECK, FK).
			if err := extractColumnConstraints(cd, table, schema, defaultSchema); err != nil {
				return nil, err
			}

		case elt.GetConstraint() != nil:
			con := elt.GetConstraint()
			if con.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
				fk, err := parseInlineForeignKey(con, schema, cs.Relation.Relname, defaultSchema)
				if err != nil {
					return nil, err
				}
				if fk != nil {
					if err := setUnique(table.ForeignKeys, fk.Name, "foreign key", fk); err != nil {
						return nil, err
					}
				}
			} else {
				constraint, err := parseTableConstraint(con, table.Name)
				if err != nil {
					return nil, err
				}
				if constraint != nil {
					if err := setUnique(table.Constraints, constraint.Name, "constraint", constraint); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	return table, nil
}

func parseColumnDef(cd *pg_query.ColumnDef) (*model.Column, error) {
	col := &model.Column{
		Name: cd.Colname,
	}

	if cd.TypeName != nil {
		typeName, err := deparseTypeName(cd.TypeName)
		if err != nil {
			return nil, fmt.Errorf("failed to deparse type for column %s: %w", cd.Colname, err)
		}
		col.TypeName = typeName
	}

	if cd.CollClause != nil && len(cd.CollClause.Collname) > 0 {
		var parts []string
		for _, n := range cd.CollClause.Collname {
			if s := n.GetString_(); s != nil {
				parts = append(parts, s.Sval)
			}
		}
		if len(parts) > 0 {
			collation := strings.Join(parts, ".")
			col.Collation = &collation
		}
	}

	for _, conNode := range cd.Constraints {
		con := conNode.GetConstraint()
		if con == nil {
			continue
		}
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_NOTNULL:
			col.NotNull = true
		case pg_query.ConstrType_CONSTR_DEFAULT:
			if con.RawExpr != nil {
				def, err := deparseExpr(con.RawExpr)
				if err != nil {
					return nil, fmt.Errorf("failed to deparse default for column %s: %w", cd.Colname, err)
				}
				col.Default = &def
			}
		case pg_query.ConstrType_CONSTR_IDENTITY:
			switch con.GeneratedWhen {
			case "a":
				col.Identity = model.ColumnIdentity('a')
			case "d":
				col.Identity = model.ColumnIdentity('d')
			}
		case pg_query.ConstrType_CONSTR_GENERATED:
			if con.GeneratedWhen == "s" {
				col.Generated = model.ColumnGenerated('s')
			}
			if con.RawExpr != nil {
				def, err := deparseExpr(con.RawExpr)
				if err != nil {
					return nil, fmt.Errorf("failed to deparse generated expr for column %s: %w", cd.Colname, err)
				}
				col.Default = &def
			}
		}
	}

	return col, nil
}

// extractColumnConstraints extracts named constraints from a column definition
// (e.g. PRIMARY KEY, UNIQUE, CHECK, EXCLUSION, FOREIGN KEY) and adds them to
// the table. Column-attribute constraints (NOT NULL, DEFAULT, IDENTITY,
// GENERATED) are skipped as they are handled by parseColumnDef.
// Unnamed non-attribute constraints return an error.
func extractColumnConstraints(cd *pg_query.ColumnDef, table *model.Table, schema, defaultSchema string) error {
	for _, conNode := range cd.Constraints {
		con := conNode.GetConstraint()
		if con == nil {
			continue
		}
		// Skip column-attribute constraints (NOT NULL, DEFAULT, IDENTITY, GENERATED)
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_NOTNULL, pg_query.ConstrType_CONSTR_DEFAULT,
			pg_query.ConstrType_CONSTR_IDENTITY, pg_query.ConstrType_CONSTR_GENERATED:
			continue
		}
		if con.Conname == "" {
			return fmt.Errorf("unnamed constraint on column %q in table %q is not supported (type: %s)", cd.Colname, table.Name, con.Contype)
		}
		switch con.Contype {
		case pg_query.ConstrType_CONSTR_FOREIGN:
			// Column-level FK has no FkAttrs; fill in the owning column name.
			if len(con.FkAttrs) == 0 {
				con.FkAttrs = []*pg_query.Node{pg_query.MakeStrNode(cd.Colname)}
			}
			fk, err := parseInlineForeignKey(con, schema, table.Name, defaultSchema)
			if err != nil {
				return err
			}
			if fk != nil {
				if err := setUnique(table.ForeignKeys, fk.Name, "foreign key", fk); err != nil {
					return err
				}
			}
		case pg_query.ConstrType_CONSTR_PRIMARY, pg_query.ConstrType_CONSTR_UNIQUE,
			pg_query.ConstrType_CONSTR_CHECK, pg_query.ConstrType_CONSTR_EXCLUSION:
			constraint, err := parseTableConstraint(con, table.Name)
			if err != nil {
				return err
			}
			if constraint != nil {
				if err := setUnique(table.Constraints, constraint.Name, "constraint", constraint); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func parseTableConstraint(con *pg_query.Constraint, tableName string) (*model.Constraint, error) {
	if con.Conname == "" {
		return nil, fmt.Errorf("unnamed constraint on table %q is not supported (type: %s)", tableName, con.Contype)
	}

	var conType model.ConstraintType
	switch con.Contype {
	case pg_query.ConstrType_CONSTR_PRIMARY:
		conType = model.ConstraintType('p')
	case pg_query.ConstrType_CONSTR_UNIQUE:
		conType = model.ConstraintType('u')
	case pg_query.ConstrType_CONSTR_CHECK:
		conType = model.ConstraintType('c')
	case pg_query.ConstrType_CONSTR_EXCLUSION:
		conType = model.ConstraintType('x')
	default:
		return nil, nil
	}

	def, err := deparseConstraintDef(con)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse constraint %s: %w", con.Conname, err)
	}

	var columns []string
	for _, k := range con.Keys {
		if s := k.GetString_(); s != nil {
			columns = append(columns, s.Sval)
		}
	}

	return &model.Constraint{
		Name:       con.Conname,
		Type:       conType,
		Definition: def,
		Columns:    columns,
		Deferrable: con.Deferrable,
		Deferred:   con.Initdeferred,
		Validated:  true,
	}, nil
}

func parseIndexStmt(is *pg_query.IndexStmt, rawStmt *pg_query.RawStmt, defaultSchema string) (*model.Index, error) {
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{Stmt: rawStmt.Stmt}},
	}
	def, err := pg_query.Deparse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse index: %w", err)
	}

	schema := is.Relation.Schemaname
	if schema == "" {
		schema = defaultSchema
	}

	var tablespace *string
	if is.TableSpace != "" {
		ts := is.TableSpace
		tablespace = &ts
	}

	return &model.Index{
		Schema:     schema,
		Name:       is.Idxname,
		Table:      is.Relation.Relname,
		Definition: def,
		TableSpace: tablespace,
	}, nil
}

func parseViewStmt(vs *pg_query.ViewStmt, defaultSchema string) (*model.View, error) {
	schema := vs.View.Schemaname
	if schema == "" {
		schema = defaultSchema
	}

	// Deparse the SELECT query
	selectResult := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: vs.Query,
		}},
	}
	def, err := pg_query.Deparse(selectResult)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse view query: %w", err)
	}

	return &model.View{
		Schema:     schema,
		Name:       vs.View.Relname,
		Definition: def,
	}, nil
}

func parseCommentStmt(cs *pg_query.CommentStmt, defaultSchema string, tables *orderedmap.Map[string, *model.Table], views *orderedmap.Map[string, *model.View]) {
	items := cs.Object.GetList().GetItems()
	if len(items) == 0 {
		return
	}

	var names []string
	for _, item := range items {
		if s := item.GetString_(); s != nil {
			names = append(names, s.Sval)
		}
	}

	switch cs.Objtype {
	case pg_query.ObjectType_OBJECT_TABLE:
		schema := defaultSchema
		tableName := names[0]
		if len(names) >= 2 {
			schema = names[0]
			tableName = names[1]
		}
		fqtn := model.Ident(schema, tableName)
		if t, ok := tables.GetOk(fqtn); ok {
			if cs.Comment != "" {
				c := cs.Comment
				t.Comment = &c
			} else {
				t.Comment = nil
			}
		}
	case pg_query.ObjectType_OBJECT_VIEW:
		schema := defaultSchema
		viewName := names[0]
		if len(names) >= 2 {
			schema = names[0]
			viewName = names[1]
		}
		fqvn := model.Ident(schema, viewName)
		if v, ok := views.GetOk(fqvn); ok {
			if cs.Comment != "" {
				c := cs.Comment
				v.Comment = &c
			} else {
				v.Comment = nil
			}
		}
	case pg_query.ObjectType_OBJECT_COLUMN:
		if len(names) < 2 {
			return
		}
		schema := defaultSchema
		tableName := names[0]
		colName := names[1]
		if len(names) >= 3 {
			schema = names[0]
			tableName = names[1]
			colName = names[2]
		}
		fqtn := model.Ident(schema, tableName)
		if t, ok := tables.GetOk(fqtn); ok {
			if col, ok := t.Columns.GetOk(colName); ok {
				if cs.Comment != "" {
					c := cs.Comment
					col.Comment = &c
				} else {
					col.Comment = nil
				}
			}
		}
	}
}

func parseAlterTableConstraint(as *pg_query.AlterTableStmt, defaultSchema string) (*model.Constraint, *model.ForeignKey, error) {
	for _, cmdNode := range as.Cmds {
		cmd := cmdNode.GetAlterTableCmd()
		if cmd == nil || cmd.Subtype != pg_query.AlterTableType_AT_AddConstraint {
			continue
		}
		con := cmd.Def.GetConstraint()
		if con == nil {
			continue
		}

		schema := as.Relation.Schemaname
		if schema == "" {
			schema = defaultSchema
		}

		def, err := deparseConstraintDef(con)
		if err != nil {
			return nil, nil, err
		}

		if con.Contype == pg_query.ConstrType_CONSTR_FOREIGN {
			var refSchema, refTable *string
			if con.Pktable != nil {
				rs := con.Pktable.Schemaname
				if rs == "" {
					rs = defaultSchema
				}
				refSchema = &rs
				rt := con.Pktable.Relname
				refTable = &rt
			}

			var columns []string
			for _, attr := range con.FkAttrs {
				if s := attr.GetString_(); s != nil {
					columns = append(columns, s.Sval)
				}
			}

			fk := &model.ForeignKey{
				Constraint: model.Constraint{
					Name:       con.Conname,
					Type:       model.ConstraintType('f'),
					Definition: def,
					Columns:    columns,
					Deferrable: con.Deferrable,
					Deferred:   con.Initdeferred,
					Validated:  !con.SkipValidation,
				},
				Schema:    schema,
				Table:     as.Relation.Relname,
				RefSchema: refSchema,
				RefTable:  refTable,
			}

			return nil, fk, nil
		}

		// Non-FK constraint (PRIMARY KEY, UNIQUE, CHECK, etc.)
		constraint, err := parseTableConstraint(con, as.Relation.Relname)
		if err != nil {
			return nil, nil, err
		}

		return constraint, nil, nil
	}

	return nil, nil, nil
}

// parseInlineForeignKey builds a ForeignKey from an inline FOREIGN KEY
// constraint inside a CREATE TABLE statement.
func parseInlineForeignKey(con *pg_query.Constraint, schema, table, defaultSchema string) (*model.ForeignKey, error) {
	if con.Conname == "" {
		return nil, fmt.Errorf("unnamed FOREIGN KEY constraints are not supported (table: %s)", table)
	}

	def, err := deparseConstraintDef(con)
	if err != nil {
		return nil, fmt.Errorf("failed to deparse constraint %s: %w", con.Conname, err)
	}

	var refSchema, refTable *string
	if con.Pktable != nil {
		rs := con.Pktable.Schemaname
		if rs == "" {
			rs = defaultSchema
		}
		refSchema = &rs
		rt := con.Pktable.Relname
		refTable = &rt
	}

	var columns []string
	for _, attr := range con.FkAttrs {
		if s := attr.GetString_(); s != nil {
			columns = append(columns, s.Sval)
		}
	}

	return &model.ForeignKey{
		Constraint: model.Constraint{
			Name:       con.Conname,
			Type:       model.ConstraintType('f'),
			Definition: def,
			Columns:    columns,
			Deferrable: con.Deferrable,
			Deferred:   con.Initdeferred,
			Validated:  !con.SkipValidation,
		},
		Schema:    schema,
		Table:     table,
		RefSchema: refSchema,
		RefTable:  refTable,
	}, nil
}

// Deparse helpers

func deparseTypeName(tn *pg_query.TypeName) (string, error) {
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{
				Node: &pg_query.Node_CreateStmt{
					CreateStmt: &pg_query.CreateStmt{
						Relation: pg_query.MakeSimpleRangeVar("_t", 0),
						TableElts: []*pg_query.Node{
							pg_query.MakeSimpleColumnDefNode("_c", tn, nil, 0),
						},
					},
				},
			},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse type name: %w", err)
	}

	const marker = "_c "
	idx := strings.Index(sql, marker)
	if idx == -1 {
		return "", fmt.Errorf("unexpected deparse output for type: %s", sql)
	}
	rest := sql[idx+len(marker):]
	lastParen := strings.LastIndex(rest, ")")
	if lastParen < 0 {
		return "", fmt.Errorf("unexpected deparse output for type: %s", sql)
	}
	typeName := strings.TrimSpace(rest[:lastParen])
	// pg_query may qualify built-in types with "pg_catalog." (e.g. json → pg_catalog.json).
	// Strip the prefix so the result matches format_type() output.
	typeName = strings.TrimPrefix(typeName, "pg_catalog.")
	return normalizeTypeName(typeName), nil
}

var typeAliases = map[string]string{
	"int":         "integer",
	"int4":        "integer",
	"int2":        "smallint",
	"int8":        "bigint",
	"float4":      "real",
	"float8":      "double precision",
	"bool":        "boolean",
	"varchar":     "character varying",
	"char":        "character",
	"timestamp":   "timestamp without time zone",
	"timestamptz": "timestamp with time zone",
	"time":        "time without time zone",
	"timetz":      "time with time zone",
	"varbit":      "bit varying",
	"decimal":     "numeric",
	"float":       "double precision",
	"serial":      "integer",
	"bigserial":   "bigint",
	"smallserial": "smallint",
}

func normalizeTypeName(name string) string {
	// Handle types with modifiers like "varchar(255)" → "character varying(255)"
	base := name
	suffix := ""
	if idx := strings.Index(name, "("); idx != -1 {
		base = name[:idx]
		suffix = name[idx:]
	} else if idx := strings.Index(name, "["); idx != -1 {
		base = name[:idx]
		suffix = name[idx:]
	}

	// Normalize spacing in type modifiers: "numeric(10, 2)" → "numeric(10,2)"
	suffix = strings.ReplaceAll(suffix, ", ", ",")

	if canonical, ok := typeAliases[base]; ok {
		return canonical + suffix
	}
	return base + suffix
}

func deparseExpr(node *pg_query.Node) (string, error) {
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{
				Node: &pg_query.Node_SelectStmt{
					SelectStmt: &pg_query.SelectStmt{
						TargetList: []*pg_query.Node{
							pg_query.MakeResTargetNodeWithVal(node, 0),
						},
					},
				},
			},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse expression: %w", err)
	}
	const prefix = "SELECT "
	if !strings.HasPrefix(sql, prefix) {
		return "", fmt.Errorf("unexpected deparse output for expression: %s", sql)
	}
	return strings.TrimSpace(sql[len(prefix):]), nil
}

func deparseConstraintDef(con *pg_query.Constraint) (string, error) {
	alterCmd := &pg_query.AlterTableCmd{
		Subtype: pg_query.AlterTableType_AT_AddConstraint,
		Def:     &pg_query.Node{Node: &pg_query.Node_Constraint{Constraint: con}},
	}
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{
				Node: &pg_query.Node_AlterTableStmt{
					AlterTableStmt: &pg_query.AlterTableStmt{
						Relation: pg_query.MakeSimpleRangeVar("_t", 0),
						Cmds: []*pg_query.Node{{
							Node: &pg_query.Node_AlterTableCmd{AlterTableCmd: alterCmd},
						}},
						Objtype: pg_query.ObjectType_OBJECT_TABLE,
					},
				},
			},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse constraint: %w", err)
	}

	if con.Conname != "" {
		marker := "CONSTRAINT " + model.Ident(con.Conname) + " "
		idx := strings.Index(sql, marker)
		if idx != -1 {
			return strings.TrimSpace(sql[idx+len(marker):]), nil
		}
	}

	const fallbackMarker = " ADD "
	idx := strings.LastIndex(sql, fallbackMarker)
	if idx != -1 {
		return strings.TrimSpace(sql[idx+len(fallbackMarker):]), nil
	}

	return "", fmt.Errorf("could not extract constraint definition from: %s", sql)
}

func deparsePartitionSpec(cs *pg_query.CreateStmt) (string, error) {
	minCS := &pg_query.CreateStmt{
		Relation: pg_query.MakeSimpleRangeVar("_t", 0),
		TableElts: []*pg_query.Node{
			pg_query.MakeSimpleColumnDefNode("_c", &pg_query.TypeName{
				Names: []*pg_query.Node{pg_query.MakeStrNode("integer")},
			}, nil, 0),
		},
		Partspec: cs.Partspec,
	}
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{Node: &pg_query.Node_CreateStmt{CreateStmt: minCS}},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse partition spec: %w", err)
	}
	const prefix = "PARTITION BY "
	idx := strings.Index(sql, prefix)
	if idx == -1 {
		return "", fmt.Errorf("could not extract partition spec from: %s", sql)
	}
	return strings.TrimSpace(sql[idx+len(prefix):]), nil
}

func deparsePartitionBound(cs *pg_query.CreateStmt) (string, error) {
	minCS := &pg_query.CreateStmt{
		Relation: pg_query.MakeSimpleRangeVar("_t", 0),
		InhRelations: []*pg_query.Node{
			{Node: &pg_query.Node_RangeVar{RangeVar: pg_query.MakeSimpleRangeVar("_parent", 0)}},
		},
		Partbound: cs.Partbound,
	}
	result := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{{
			Stmt: &pg_query.Node{Node: &pg_query.Node_CreateStmt{CreateStmt: minCS}},
		}},
	}
	sql, err := pg_query.Deparse(result)
	if err != nil {
		return "", fmt.Errorf("failed to deparse partition bound: %w", err)
	}
	const prefix = "PARTITION OF _parent "
	idx := strings.Index(sql, prefix)
	if idx == -1 {
		return "", fmt.Errorf("could not extract partition bound from: %s", sql)
	}
	return strings.TrimSpace(sql[idx+len(prefix):]), nil
}
