package pistachio

import (
	"context"
	"strings"

	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/model"
)

// explainDump attaches the size estimate dump --explain prints to every
// table, materialized view and index. The numbers are the ones plan --explain
// prints, read from pg_class in two queries, so the dump reads no table.
//
// A partitioned table sums its partitions, at any depth, and says how many
// there are. The sum is taken over every table the catalog read, so a
// partition a filter leaves out of the dump still counts toward its parent,
// and a partition in a schema the catalog did not read counts toward nothing.
// An INHERITS parent is its own rows alone, since its children are dumped next
// to it with sizes of their own.
func explainDump(
	ctx context.Context,
	cat *catalog.Catalog,
	tables *orderedmap.Map[string, *model.Table],
	views *orderedmap.Map[string, *model.View],
) error {
	stats, err := cat.TableStats(ctx)
	if err != nil {
		return err
	}
	indexSizes, err := cat.IndexSizes(ctx)
	if err != nil {
		return err
	}

	partitions := map[string][]*model.Table{}
	for _, t := range tables.CollectValues() {
		if t.IsPartitionChild() {
			partitions[*t.PartitionOf] = append(partitions[*t.PartitionOf], t)
		}
	}
	var descendants func(t *model.Table) []*model.Table
	descendants = func(t *model.Table) []*model.Table {
		var out []*model.Table
		for _, c := range partitions[t.FQTN()] {
			out = append(out, c)
			out = append(out, descendants(c)...)
		}
		return out
	}

	setIndexSizes := func(indexes *orderedmap.Map[string, *model.Index]) {
		for _, idx := range indexes.CollectValues() {
			if bytes, ok := indexSizes[model.Ident(idx.Schema, idx.Name)]; ok {
				idx.Size = sizePretty(bytes)
			}
		}
	}

	for _, t := range tables.CollectValues() {
		relations := []*model.Table{t}
		if t.Partitioned {
			relations = append(relations, descendants(t)...)
		}
		details := sizeDetails(stats, relations)
		if t.Partitioned {
			details = append(details, pluralize(len(relations)-1, "partition"))
		}
		t.Size = strings.Join(details, ", ")
		setIndexSizes(t.Indexes)
	}

	for _, v := range views.CollectValues() {
		if !v.Materialized {
			continue
		}
		v.Size = strings.Join(sizeDetails(stats, []*model.Table{{Schema: v.Schema, Name: v.Name}}), ", ")
		setIndexSizes(v.Indexes)
	}

	return nil
}
