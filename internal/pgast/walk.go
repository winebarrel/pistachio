package pgast

import (
	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Walk and WalkPair traverse a pg_query parse tree without naming a single
// node kind. pg_query nodes are protobuf messages, so the children of a node
// are just its message-typed fields; enumerating them through protoreflect
// reaches every one of the 268 kinds the grammar produces, including the ones
// PostgreSQL adds in a later release.
//
// The hand-written alternative was five recursive functions, each with its own
// switch covering about twenty kinds between them. Every kind left out was
// skipped in silence, which is how a column inside a SQL/JSON constructor went
// missing from a generated constraint name and how a view kept the table
// qualification the catalog strips.
//
// Two things the traversal deliberately does not do:
//
//   - It hands the visitor whole Nodes, never the String nodes underneath one.
//     A visitor that mutates (the column renamer does) would otherwise reach a
//     function name in FuncCall.Funcname, a field name in an A_Indirection or a
//     type name in a TypeName, all of which are String nodes written where a
//     column name could go. Acting on ColumnRef alone cannot reach them.
//   - It stops at a sub-link's contained SELECT when asked. Columns there
//     resolve against another table.

// Ctx tells a visitor where the node it is looking at sits: Parent is the
// message holding it and Field is the name of the holding field, with
// UpParent and UpField saying the same for the parent. Everything is zero at
// the root.
//
// A Node and the concrete message inside its oneof count as one step, so
// Parent is the message that meaningfully holds the node rather than the Node
// wrapper. Two levels is what the one rule using this needs, and keeping it a
// value rather than a chain keeps the walk from allocating per field.
type Ctx struct {
	Parent   proto.Message
	Field    string
	UpParent proto.Message
	UpField  string
}

func (c Ctx) push(parent proto.Message, field string) Ctx {
	return Ctx{Parent: parent, Field: field, UpParent: c.Parent, UpField: c.Field}
}

// IsSelectTarget reports whether the node is the expression of a SELECT target
// list entry. A cast written there decides the resulting view's output column
// type, so the normalizations that strip casts have to leave that one alone.
// The check reaches two levels up because a ResTarget also holds the elements
// of xmlattributes() and xmlforest(), where the rule does not apply.
func (c Ctx) IsSelectTarget() bool {
	if c.Field != "val" || c.UpField != "target_list" {
		return false
	}
	_, ok := c.Parent.(*pg_query.ResTarget)
	return ok
}

// VisitFunc is called for each node in the tree. Returning a different node
// replaces it in its parent; returning the node unchanged leaves it alone.
// The return value must not be nil: a rewrite drops a wrapper by returning
// what it wrapped, never by removing the position outright.
type VisitFunc func(ctx Ctx, node *pg_query.Node) *pg_query.Node

// PairVisitFunc is the two-tree form. It returns the node to keep on the
// current side, which must not be nil for the same reason.
type PairVisitFunc func(ctx Ctx, desired, current *pg_query.Node) *pg_query.Node

// WalkOptions carries the one exception the traversal knows about.
type WalkOptions struct {
	// SkipSubqueries stops the walk at a sub-link's contained SELECT. The
	// constraint namer, the desired-schema validator and the column renamer all
	// work within one table, so a column named in a sub-query is not theirs to
	// read or rewrite. The view comparison, which has to normalize the whole
	// body, leaves this false.
	SkipSubqueries bool
}

// Walk visits every node in the tree, children before the node itself, and
// returns the (possibly replaced) root. Children first because the
// normalizations are written that way: stripping a cast wrapper is decided
// after whatever it wraps has been normalized.
func Walk(node *pg_query.Node, opts WalkOptions, visit VisitFunc) *pg_query.Node {
	return walkNode(node, Ctx{}, opts, visit)
}

// WalkPair visits desired and current in step, the node itself before its
// children, and returns the (possibly replaced) current root. It descends only
// where both sides hold the same kind of node and their repeated fields have
// equal length, so a structural difference simply stops the walk there.
//
// The node comes before its children here because the only rule using it
// unwraps casts from the current side, and the unwrapped node is what the
// children have to be paired against.
func WalkPair(desired, current *pg_query.Node, visit PairVisitFunc) *pg_query.Node {
	return walkPairNode(desired, current, Ctx{}, visit)
}

func walkNode(node *pg_query.Node, ctx Ctx, opts WalkOptions, visit VisitFunc) *pg_query.Node {
	if node == nil {
		return nil
	}
	walkMessage(node, ctx, opts, visit)
	return visit(ctx, node)
}

func walkMessage(m proto.Message, ctx Ctx, opts WalkOptions, visit VisitFunc) {
	r := m.ProtoReflect()

	// A Node is a wrapper holding one of 268 oneof members. Asking which one is
	// set beats scanning the field list, and the member it holds counts as the
	// same step rather than a level of its own, so ctx passes through.
	if _, atNode := m.(*pg_query.Node); atNode {
		if fd := nodeOneof(r); fd != nil {
			walkMessage(r.Get(fd).Message().Interface(), ctx, opts, visit)
		}
		return
	}

	fds := r.Descriptor().Fields()
	for i := range fds.Len() {
		fd := fds.Get(i)
		if fd.Kind() != protoreflect.MessageKind || !r.Has(fd) {
			continue
		}
		if opts.SkipSubqueries && isSubLinkSubselect(m, fd) {
			continue
		}
		childCtx := ctx.push(m, fd.TextName())
		value := r.Get(fd)

		if fd.IsList() {
			// Every repeated message field the grammar produces holds Nodes.
			list := value.List()
			for j := range list.Len() {
				node, ok := list.Get(j).Message().Interface().(*pg_query.Node)
				if !ok {
					continue
				}
				if replaced := walkNode(node, childCtx, opts, visit); replaced != nil && replaced != node {
					list.Set(j, protoreflect.ValueOfMessage(replaced.ProtoReflect()))
				}
			}
			continue
		}

		child := value.Message().Interface()
		node, ok := child.(*pg_query.Node)
		if !ok {
			walkMessage(child, childCtx, opts, visit)
			continue
		}
		if replaced := walkNode(node, childCtx, opts, visit); replaced != nil && replaced != node {
			r.Set(fd, protoreflect.ValueOfMessage(replaced.ProtoReflect()))
		}
	}
}

func walkPairNode(desired, current *pg_query.Node, ctx Ctx, visit PairVisitFunc) *pg_query.Node {
	if desired == nil || current == nil {
		return current
	}
	current = visit(ctx, desired, current)
	walkPairMessage(desired, current, ctx, visit)
	return current
}

func walkPairMessage(desired, current proto.Message, ctx Ctx, visit PairVisitFunc) {
	dr, cr := desired.ProtoReflect(), current.ProtoReflect()

	if _, atNode := desired.(*pg_query.Node); atNode {
		// Both sides must hold the same oneof member, which is what keeps the
		// walk from pairing an A_Expr against a FuncCall.
		fd := nodeOneof(dr)
		if fd == nil || cr.WhichOneof(fd.ContainingOneof()) != fd {
			return
		}
		walkPairMessage(dr.Get(fd).Message().Interface(), cr.Get(fd).Message().Interface(), ctx, visit)
		return
	}

	fds := dr.Descriptor().Fields()
	for i := range fds.Len() {
		fd := fds.Get(i)
		if fd.Kind() != protoreflect.MessageKind || !dr.Has(fd) || !cr.Has(fd) {
			continue
		}
		childCtx := ctx.push(current, fd.TextName())
		dv, cv := dr.Get(fd), cr.Get(fd)

		if fd.IsList() {
			dl, cl := dv.List(), cv.List()
			if dl.Len() != cl.Len() {
				continue
			}
			for j := range dl.Len() {
				dn, dok := dl.Get(j).Message().Interface().(*pg_query.Node)
				cn, cok := cl.Get(j).Message().Interface().(*pg_query.Node)
				if !dok || !cok {
					continue
				}
				if replaced := walkPairNode(dn, cn, childCtx, visit); replaced != nil && replaced != cn {
					cl.Set(j, protoreflect.ValueOfMessage(replaced.ProtoReflect()))
				}
			}
			continue
		}

		dm, cm := dv.Message().Interface(), cv.Message().Interface()
		dn, dok := dm.(*pg_query.Node)
		cn, cok := cm.(*pg_query.Node)
		if !dok || !cok {
			walkPairMessage(dm, cm, childCtx, visit)
			continue
		}
		if replaced := walkPairNode(dn, cn, childCtx, visit); replaced != nil && replaced != cn {
			cr.Set(fd, protoreflect.ValueOfMessage(replaced.ProtoReflect()))
		}
	}
}

// nodeOneof returns the descriptor of the oneof member a Node holds, or nil
// for an empty one.
func nodeOneof(r protoreflect.Message) protoreflect.FieldDescriptor {
	oneofs := r.Descriptor().Oneofs()
	if oneofs.Len() == 0 {
		return nil
	}
	return r.WhichOneof(oneofs.Get(0))
}

func isSubLinkSubselect(m proto.Message, fd protoreflect.FieldDescriptor) bool {
	_, ok := m.(*pg_query.SubLink)
	return ok && fd.TextName() == "subselect"
}
